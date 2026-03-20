package sender

import (
	"hpuft/protocol"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"
)


// TokenBucket controls the sending rate using a token-based pacer with
// dual-metric feedback from receiver heartbeats.
//
// Pacing uses a deficit accumulator instead of per-packet spin-waits.
// The sender runs at wire speed until accumulated debt reaches ≥1ms, then
// sleeps for exactly that debt. This avoids the sub-millisecond timing
// problem on Windows (and Go's asynchronous goroutine preemption) where
// spin loops and time.Sleep(<1ms) both overshoot by ~1ms, capping
// effective throughput at ~1.4 MB/s regardless of the configured target.
//
// # Rate adjustment (v3.1)
//
// Two-phase growth model:
//
//	Phase 1 — Multiplicative Probe (loss < 1%):
//	  S_new = S × Phase1Multiplier (1.25×), applied once per RTT.
//	  The sender has not yet found the link ceiling.
//
//	Phase 2 — Additive (permanent after first hold zone entry):
//	  S_new = S + MaxPayload / RTT, applied once per RTT.
//	  Gentle probing once the link ceiling is known.
//
//	Hold (1–5% loss): rate unchanged. First entry permanently transitions
//	  the controller into Phase 2.
//
//	Decrease (> 5% loss, two consecutive confirmations):
//	  S_new = smoothed(E) × DecreaseFrac (0.85).
//	  Undershoots the measured ceiling so router queues can drain.
type TokenBucket struct {
	mu sync.Mutex

	// rate is the current send rate in bytes per second
	rate float64

	// maxRate caps the upward probe (0 = unlimited)
	maxRate float64

	// congestion control config
	cc protocol.CongestionConfig

	// lastSend is updated after each Pace call (after any sleep)
	// so that elapsed reflects only loop overhead, not sleep time.
	lastSend time.Time

	// tokens is the current byte credit balance (negative = debt).
	tokens float64

	// EWMA smoothed effective rate (dampens jitter)
	smoothedRate float64
	ewmaAlpha    float64 // weight of new sample (0.3 = moderate smoothing)
	ewmaInit     bool

	// Consecutive decrease signals required before acting
	decreaseStreak int

	// Peak observed delivery rate for auto-ceiling
	peakRate float64

	// atCeiling suppresses log spam when rate is auto-capped
	atCeiling bool

	// heartbeatCount tracks how many heartbeats have been received.
	heartbeatCount int

	// Phase 2 tracking
	inPhase2      bool // permanent after first 1-5% hold zone entry
	lastIncreaseHB int  // heartbeatCount at which the last increase was applied

	// collapseHoldStreak counts consecutive delivery-collapse HOLDs.
	// When this exceeds collapseDecreaseThreshold the CC forces a decrease
	// rather than holding indefinitely at an unachievable rate.
	collapseHoldStreak int

	// rttEstimateNs holds the most recent RTT estimate in nanoseconds.
	// Derived from EchoTimestampNs in heartbeats. 0 = unknown.
	rttEstimateNs int64

	// lastLossRate is the most recent loss percentage (0–100) from heartbeats.
	lastLossRate float64

	// lastEchoNs is the highest EchoTimestampNs we have used to compute RTT.
	// We only update rttEstimateNs when a strictly newer echo arrives, so that
	// a frozen timestamp (echoed repeatedly while the sender is idle honoring
	// the NACK cooldown) does not cause RTT to inflate unboundedly.
	lastEchoNs int64

	// bytesSent tracks bytes sent in the current measurement window
	bytesSent atomic.Int64

	// stats
	increases atomic.Int64
	holds     atomic.Int64
	decreases atomic.Int64

	// logger receives debug-level CC decisions. nil = no-op (normal mode).
	logger *log.Logger
}

var discardLog = log.New(io.Discard, "", 0)

// NewTokenBucket creates a rate controller starting at initialRate bytes/sec.
func NewTokenBucket(initialRate float64, cc protocol.CongestionConfig) *TokenBucket {
	return &TokenBucket{
		rate:      initialRate,
		cc:        cc,
		lastSend:  time.Now(),
		ewmaAlpha: 0.3, // 0.3 = moderate smoothing (reacts in ~3 samples)
	}
}

// SetLogger enables debug-level CC logging to l. Pass nil to silence.
func (tb *TokenBucket) SetLogger(l *log.Logger) {
	tb.mu.Lock()
	tb.logger = l
	tb.mu.Unlock()
}

// logf writes to the debug logger if one is set.
func (tb *TokenBucket) logf(format string, args ...any) {
	if tb.logger != nil {
		tb.logger.Printf(format, args...)
	}
}

// SetMaxRate sets an upper bound on the sending rate (0 = unlimited).
func (tb *TokenBucket) SetMaxRate(maxRate float64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.maxRate = maxRate
}

// Rate returns the current sending rate in bytes/sec.
func (tb *TokenBucket) Rate() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.rate
}

// RTTEstimate returns the most recent smoothed RTT, or 0 if not yet measured.
func (tb *TokenBucket) RTTEstimate() time.Duration {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return time.Duration(tb.rttEstimateNs)
}

// Phase returns 1 (Multiplicative Probe) or 2 (Additive Avoidance).
func (tb *TokenBucket) Phase() int {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.inPhase2 {
		return 2
	}
	return 1
}

// LossRatePercent returns the most recently observed loss rate as a percentage
// (e.g. 0.10 means 0.10% loss). Returns 0 if no heartbeat has been received.
func (tb *TokenBucket) LossRatePercent() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.lastLossRate
}

// Pace rate-limits packet sends using a deficit accumulator.
//
// Each call accrues byte credits for the time elapsed since the last call,
// then debits the packet size. Credits are capped at a 2ms burst budget so
// idle periods don't bank excessive credit. When the deficit is large enough
// to justify a sleep (≥1ms — the minimum reliable OS sleep granularity on
// Windows), we sleep and reset the deficit to zero; small deficits are
// carried forward and will trigger a sleep once they accumulate enough.
func (tb *TokenBucket) Pace(packetBytes int) {
	tb.mu.Lock()
	rate := tb.rate

	if rate <= 0 {
		tb.mu.Unlock()
		tb.bytesSent.Add(int64(packetBytes))
		return
	}

	now := time.Now()
	tb.tokens += rate * now.Sub(tb.lastSend).Seconds()

	if maxBurst := rate * 0.002; tb.tokens > maxBurst {
		tb.tokens = maxBurst
	}

	tb.tokens -= float64(packetBytes)

	var sleepDur time.Duration
	if tb.tokens < 0 {
		sleepDur = time.Duration(-tb.tokens / rate * float64(time.Second))
		if sleepDur >= time.Millisecond {
			tb.tokens = 0
		} else {
			sleepDur = 0
		}
	}

	tb.mu.Unlock()
	tb.bytesSent.Add(int64(packetBytes))

	if sleepDur > 0 {
		time.Sleep(sleepDur)
	}

	tb.mu.Lock()
	tb.lastSend = time.Now()
	tb.mu.Unlock()
}

// OnHeartbeat processes a heartbeat from the receiver and adjusts the
// sending rate per the v3.1 phased growth model.
//
// Returns the new rate in bytes/sec.
func (tb *TokenBucket) OnHeartbeat(hb *protocol.HeartbeatPayload) float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.heartbeatCount++

	// --- Update RTT estimate from echoed sender timestamp ---
	// Only accept an echo that is strictly newer than the last one we used.
	// When the sender is idle (honoring NACK cooldown), the receiver keeps
	// echoing the same frozen SenderTimestampNs; if we compute
	//   rtt = now - frozenTs
	// every heartbeat, RTT inflates unboundedly and the cooldown eventually
	// exceeds the receiver's inactivity timeout, killing the transfer.
	if hb.EchoTimestampNs > 0 {
		echoNs := int64(hb.EchoTimestampNs)
		if echoNs > tb.lastEchoNs {
			tb.lastEchoNs = echoNs
			rtt := time.Now().UnixNano() - echoNs
			// Sanity-check: accept RTT between 1ms and 10s
			if rtt >= int64(time.Millisecond) && rtt < int64(10*time.Second) {
				tb.rttEstimateNs = rtt
			}
		}
	}

	// --- Effective delivery rate = network delivery rate ---
	// StorageFlushRate is intentionally excluded. The receiver uses a
	// pre-allocated full-file ring buffer, so disk lag never causes packet
	// loss — it only affects how quickly contiguous bytes are committed to
	// disk. Using min(network, storage) makes rawEffective collapse to near
	// zero whenever a single out-of-order packet stalls the contiguous flush
	// frontier (common on any path with ≥ a few ms of jitter), which falsely
	// triggers the delivery-collapse guard and permanently locks the CC into
	// Phase 2 with a near-zero peakRate ceiling.
	rawEffective := float64(hb.NetworkDeliveryRate)

	// --- EWMA smoothing on delivery rate ---
	if !tb.ewmaInit {
		tb.smoothedRate = rawEffective
		tb.ewmaInit = true
	} else {
		tb.smoothedRate = tb.ewmaAlpha*rawEffective + (1-tb.ewmaAlpha)*tb.smoothedRate
	}

	// Track peak observed delivery rate for auto-ceiling
	if rawEffective > tb.peakRate {
		tb.peakRate = rawEffective
	}

	oldRate := tb.rate
	lossBP := hb.LossRate // basis points: 100 = 1.00%
	tb.lastLossRate = float64(lossBP) / 100.0

	// Delivery-collapse guard: when the OS socket buffer is overwhelmed, the
	// receiver drops packets before the application layer sees them. The
	// receiver reports 0% loss (no FEC failures counted) while delivery
	// collapses to near zero. Without this check the CC sees 0% loss and
	// keeps probing upward, permanently locking into the 4× Phase 1 ceiling.
	//
	// Condition: delivery < 25% of current rate AND there are outstanding
	// NACKs (distinguishes "real congestion" from a cold-start empty window).
	// Threshold is 25% (not 50%) so that on high-latency paths (50ms+ RTT)
	// the ~50% in-flight fraction during the warm-up window never accidentally
	// triggers Phase 2 entry via measurement noise.
	// Action: hold rate and permanently enter Phase 2 so the tighter 1.5×
	// ceiling fires immediately, cutting the target rate to near link capacity.
	if hb.NACKCount > 0 && tb.rate > 0 && rawEffective < tb.rate*0.25 {
		if !tb.inPhase2 {
			tb.inPhase2 = true
			tb.logf("[cc_debug] → Phase 2 (additive): delivery collapse %.2f MB/s < 25%% of rate %.2f MB/s (NACKs=%d)",
				rawEffective/1e6, tb.rate/1e6, hb.NACKCount)
		}
		tb.collapseHoldStreak++
		// After 5 consecutive collapse HOLDs the rate is clearly unachievable —
		// force a decrease so the window-full stall can break. Reset the streak
		// so we can decrease again if needed after recovery.
		const collapseDecreaseThreshold = 5
		if tb.collapseHoldStreak >= collapseDecreaseThreshold {
			// Use peakRate (not smoothedRate) as the decrease base. During a
			// delivery collapse the EWMA gets poisoned by near-zero delivery
			// readings (e.g., 5 readings at 4 MB/s drops EWMA from 100 to 20).
			// peakRate reflects actual link capacity and gives a sensible target.
			newRate := tb.peakRate * tb.cc.DecreaseFrac
			tb.logf("[cc_debug] COLLAPSE-DECREASE (streak=%d): %.2f -> %.2f MB/s (peak=%.2f delivery=%.2f NACKs=%d)",
				tb.collapseHoldStreak, tb.rate/1e6, newRate/1e6, tb.peakRate/1e6, rawEffective/1e6, hb.NACKCount)
			tb.rate = newRate
			tb.collapseHoldStreak = 0
			tb.decreases.Add(1)
		} else {
			tb.holds.Add(1)
		}
		tb.decreaseStreak = 0
		// Skip the switch — fall through to ceiling/floor checks below.
		goto applyCeiling
	}
	tb.collapseHoldStreak = 0 // clear streak when not in collapse

	switch {
	case lossBP < 100:
		// < 1% loss: link has headroom, probe upward once per RTT.
		//
		// hbPerRTT = how many heartbeats fit within one RTT. If RTT < HB
		// interval (or RTT is unknown), allow increase every heartbeat.
		hbInterval := protocol.HeartbeatInterval(uint64(tb.rate))
		hbPerRTT := 1
		if tb.rttEstimateNs > 0 {
			rttDur := time.Duration(tb.rttEstimateNs)
			if rttDur > hbInterval {
				hbPerRTT = int(rttDur / hbInterval)
			}
		}

		if tb.heartbeatCount-tb.lastIncreaseHB >= hbPerRTT {
			if tb.inPhase2 {
				// Phase 2: additive increase — gentle probe near the ceiling.
				// S_new = S + MaxPayload / RTT
				effectiveRTTNs := tb.rttEstimateNs
				if effectiveRTTNs == 0 {
					effectiveRTTNs = int64(hbInterval)
				}
				rttSec := float64(effectiveRTTNs) / float64(time.Second)
				tb.rate += float64(protocol.MaxPayload) / rttSec
			} else {
				// Phase 1: multiplicative probe.
				tb.rate = oldRate * tb.cc.Phase1Multiplier
			}
			tb.lastIncreaseHB = tb.heartbeatCount
			tb.increases.Add(1)
			if !tb.atCeiling {
				tb.logf("[cc_debug] INCREASE (phase%d): %.2f -> %.2f MB/s (loss=%.2f%% delivery=%.2f)",
					map[bool]int{false: 1, true: 2}[tb.inPhase2],
					oldRate/1e6, tb.rate/1e6, float64(lossBP)/100, rawEffective/1e6)
			}
		}
		tb.decreaseStreak = 0

	case lossBP <= 500:
		// 1–5% loss: FEC is absorbing it, hold rate.
		// First entry into this zone permanently transitions to Phase 2.
		if !tb.inPhase2 {
			tb.inPhase2 = true
			tb.logf("[cc_debug] → Phase 2 (additive): loss=%.2f%% crossed hold zone", float64(lossBP)/100)
		}
		tb.decreaseStreak = 0
		tb.holds.Add(1)

	default:
		// > 5% loss: confirmed congestion, require two consecutive signals.
		tb.decreaseStreak++

		if tb.decreaseStreak >= 2 {
			// Drop to 85% of EWMA-smoothed delivery rate so queues can drain.
			tb.rate = tb.smoothedRate * tb.cc.DecreaseFrac
			tb.decreases.Add(1)
			tb.logf("[cc_debug] DECREASE: %.2f -> %.2f MB/s (loss=%.2f%% delivery=%.2f, streak=%d)",
				oldRate/1e6, tb.rate/1e6, float64(lossBP)/100, rawEffective/1e6, tb.decreaseStreak)
		} else {
			tb.holds.Add(1)
		}
	}

applyCeiling:
	// Apply explicit max rate cap if configured
	if tb.maxRate > 0 && tb.rate > tb.maxRate {
		tb.rate = tb.maxRate
	}

	// Auto-ceiling — two-tier based on phase:
	//
	//   Phase 1 (probe):    cap at 4× peak delivery
	//   Phase 2 (avoidance): cap at 1.5× peak delivery
	//
	// Phase 1 uses a generous 4× multiplier for two reasons:
	//  1. Delivery measurements lag during ramp-up. At 7.63 MB/s send rate
	//     the receiver may report only 4.19 MB/s delivery (measurement window
	//     hasn't caught up). A tight multiplier like 1.5× fires immediately,
	//     giving a ceiling below the current rate and locking the sender at
	//     ~5.68 MB/s for the entire transfer on a 110 MB/s Gigabit link.
	//  2. On a clean link where FEC absorbs all drops (LossRate = 0% always),
	//     Phase 2 is never entered. Without a Phase 1 ceiling the target rate
	//     grows exponentially without bound (observed: 345 trillion MB/s).
	//     4× bounds this at ~400 MB/s on a Gigabit LAN — effectively disabling
	//     pacing just as nodelay would, but without the absurd log output.
	//
	// Phase 2 uses 1.5× because by then the delivery rate was measured near
	// actual link capacity (loss triggered the phase transition at or near the
	// ceiling), so 1.5× is a reliable upper bound for additive probing.
	const phase1CeilingMult = 4.0
	const phase2CeilingMult = 1.5
	ceilingMult := phase1CeilingMult
	if tb.inPhase2 {
		ceilingMult = phase2CeilingMult
	}
	wasCapped := false
	if tb.peakRate > 0 && tb.rate > tb.peakRate*ceilingMult {
		tb.rate = tb.peakRate * ceilingMult
		wasCapped = true
	}

	if wasCapped && !tb.atCeiling {
		tb.atCeiling = true
		tb.logf("[cc_debug] CEILING: rate capped at %.2f MB/s (%.1fx peak delivery %.2f MB/s)",
			tb.rate/1e6, ceilingMult, tb.peakRate/1e6)
	} else if !wasCapped {
		tb.atCeiling = false
	}

	// Floor: never go below 10 KB/s
	if tb.rate < 10_000 {
		tb.rate = 10_000
	}

	return tb.rate
}

// ResetByteCounter resets the bytes-sent counter and returns the previous value.
func (tb *TokenBucket) ResetByteCounter() int64 {
	return tb.bytesSent.Swap(0)
}

// Stats returns congestion control statistics.
func (tb *TokenBucket) Stats() TokenBucketStats {
	tb.mu.Lock()
	rate := tb.rate
	tb.mu.Unlock()
	return TokenBucketStats{
		CurrentRate: rate,
		Increases:   tb.increases.Load(),
		Holds:       tb.holds.Load(),
		Decreases:   tb.decreases.Load(),
	}
}

// TokenBucketStats holds rate adjustment counters.
type TokenBucketStats struct {
	CurrentRate float64
	Increases   int64
	Holds       int64
	Decreases   int64
}
