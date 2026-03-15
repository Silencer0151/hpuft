package sender

import (
	"hpuft/protocol"
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

	// rttEstimateNs holds the most recent RTT estimate in nanoseconds.
	// Derived from EchoTimestampNs in heartbeats. 0 = unknown.
	rttEstimateNs int64

	// bytesSent tracks bytes sent in the current measurement window
	bytesSent atomic.Int64

	// stats
	increases atomic.Int64
	holds     atomic.Int64
	decreases atomic.Int64
}

// NewTokenBucket creates a rate controller starting at initialRate bytes/sec.
func NewTokenBucket(initialRate float64, cc protocol.CongestionConfig) *TokenBucket {
	return &TokenBucket{
		rate:      initialRate,
		cc:        cc,
		lastSend:  time.Now(),
		ewmaAlpha: 0.3, // 0.3 = moderate smoothing (reacts in ~3 samples)
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
	if hb.EchoTimestampNs > 0 {
		rtt := time.Now().UnixNano() - int64(hb.EchoTimestampNs)
		// Sanity-check: accept RTT between 1ms and 10s
		if rtt >= int64(time.Millisecond) && rtt < int64(10*time.Second) {
			tb.rttEstimateNs = rtt
		}
	}

	// --- Effective delivery rate = min(network, storage) ---
	rawEffective := float64(hb.NetworkDeliveryRate)
	if float64(hb.StorageFlushRate) < rawEffective {
		rawEffective = float64(hb.StorageFlushRate)
	}

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
				log.Printf("[congestion] INCREASE (phase%d): %.2f -> %.2f MB/s (loss=%.2f%% delivery=%.2f)",
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
			log.Printf("[congestion] → Phase 2 (additive): loss=%.2f%% crossed hold zone", float64(lossBP)/100)
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
			log.Printf("[congestion] DECREASE: %.2f -> %.2f MB/s (loss=%.2f%% delivery=%.2f, streak=%d)",
				oldRate/1e6, tb.rate/1e6, float64(lossBP)/100, rawEffective/1e6, tb.decreaseStreak)
		} else {
			tb.holds.Add(1)
		}
	}

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
		log.Printf("[congestion] CEILING: rate capped at %.2f MB/s (%.1fx peak delivery %.2f MB/s)",
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
