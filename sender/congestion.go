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
	// Credits accrue at `rate` bytes/sec over elapsed time;
	// each Pace call debits the packet size. When debt exceeds the
	// 1ms sleep threshold, we call time.Sleep and reset to zero.
	tokens float64

	// EWMA smoothed effective rate (dampens jitter)
	smoothedRate float64
	ewmaAlpha    float64 // weight of new sample (0.3 = moderate smoothing)
	ewmaInit     bool

	// Consecutive decrease signals required before acting
	decreaseStreak int

	// Peak observed rate for recovery reference
	peakRate float64

	// atCeiling suppresses log spam when rate is auto-capped
	atCeiling bool

	// heartbeatCount tracks how many heartbeats have been received.
	// The auto-ceiling is not applied during the warmup phase (first few
	// heartbeats) because early delivery-rate measurements reflect cold-start
	// conditions, not actual link capacity.
	heartbeatCount int

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
//
// This produces the correct long-term average rate without relying on
// sub-millisecond timer precision or busy-wait spin loops, both of which
// are unreliable under Go's asynchronous goroutine preemption on Windows.
func (tb *TokenBucket) Pace(packetBytes int) {
	tb.mu.Lock()
	rate := tb.rate

	if rate <= 0 {
		tb.mu.Unlock()
		tb.bytesSent.Add(int64(packetBytes))
		return
	}

	// Accrue credits for time elapsed since the last Pace call.
	// lastSend is set after any sleep so elapsed reflects only loop
	// overhead, keeping the measurement uncontaminated by sleep time.
	now := time.Now()
	tb.tokens += rate * now.Sub(tb.lastSend).Seconds()

	// Cap tokens at 2ms of burst budget to prevent large backlogs
	// during idle periods (e.g., between calibration and steady state).
	if maxBurst := rate * 0.002; tb.tokens > maxBurst {
		tb.tokens = maxBurst
	}

	// Debit this packet.
	tb.tokens -= float64(packetBytes)

	// If in deficit, compute the sleep needed to honour the target rate.
	var sleepDur time.Duration
	if tb.tokens < 0 {
		sleepDur = time.Duration(-tb.tokens / rate * float64(time.Second))
		if sleepDur >= time.Millisecond {
			// Large enough for a reliable OS sleep — clear the deficit.
			tb.tokens = 0
		} else {
			// Too small to sleep reliably; carry it forward.
			sleepDur = 0
		}
	}

	tb.mu.Unlock()
	tb.bytesSent.Add(int64(packetBytes))

	if sleepDur > 0 {
		time.Sleep(sleepDur)
	}

	// Update lastSend AFTER any sleep so the next call's elapsed only
	// measures loop overhead, not how long we slept.
	tb.mu.Lock()
	tb.lastSend = time.Now()
	tb.mu.Unlock()
}

// OnHeartbeat processes a heartbeat from the receiver and adjusts the
// sending rate. The algorithm is loss-driven rather than delivery-rate-ratio
// driven, because delivery rate is bounded by send rate and can never exceed
// it — making ratio-based increase thresholds unreachable under timing jitter.
//
// Decision logic:
//   - Loss < 1%  → INCREASE (1.5x) — link has headroom, probe aggressively
//   - Loss 1-5%  → HOLD — FEC is absorbing loss, stay the course
//   - Loss > 5%  → DECREASE (consecutive confirmation required)
//
// The effective delivery rate (min of network, storage) acts as a ceiling:
// we never increase above what the receiver can actually process.
//
// Returns the new rate in bytes/sec.
func (tb *TokenBucket) OnHeartbeat(hb *protocol.HeartbeatPayload) float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.heartbeatCount++

	// Effective rate = min(network, storage) per spec
	rawEffective := float64(hb.NetworkDeliveryRate)
	if float64(hb.StorageFlushRate) < rawEffective {
		rawEffective = float64(hb.StorageFlushRate)
	}

	// EWMA smoothing on delivery rate for decrease targeting
	if !tb.ewmaInit {
		tb.smoothedRate = rawEffective
		tb.ewmaInit = true
	} else {
		tb.smoothedRate = tb.ewmaAlpha*rawEffective + (1-tb.ewmaAlpha)*tb.smoothedRate
	}

	// Track peak observed delivery rate
	if rawEffective > tb.peakRate {
		tb.peakRate = rawEffective
	}

	oldRate := tb.rate
	lossBP := hb.LossRate // basis points: 100 = 1.00%

	switch {
	case lossBP < 100:
		// < 1% loss: link has headroom, probe upward
		newRate := oldRate * tb.cc.IncreaseMultiplier
		tb.rate = newRate
		tb.decreaseStreak = 0
		tb.increases.Add(1)
		// Only log if rate actually changed (not capped at ceiling)
		if !tb.atCeiling {
			log.Printf("[congestion] INCREASE: %.2f -> %.2f MB/s (loss=%.2f%% delivery=%.2f)",
				oldRate/1e6, tb.rate/1e6, float64(lossBP)/100, rawEffective/1e6)
		}

	case lossBP <= 500:
		// 1-5% loss: FEC is handling it, hold steady
		tb.decreaseStreak = 0
		tb.holds.Add(1)

	default:
		// > 5% loss: potential congestion, require consecutive confirmation
		tb.decreaseStreak++

		if tb.decreaseStreak >= 2 {
			// Confirmed: drop to smoothed delivery rate + headroom
			tb.rate = tb.smoothedRate * tb.cc.DecreaseHeadroom
			tb.decreases.Add(1)
			log.Printf("[congestion] DECREASE: %.2f -> %.2f MB/s (loss=%.2f%% delivery=%.2f, streak=%d)",
				oldRate/1e6, tb.rate/1e6, float64(lossBP)/100, rawEffective/1e6, tb.decreaseStreak)
		} else {
			tb.holds.Add(1)
		}
	}

	// Apply max rate cap if configured
	if tb.maxRate > 0 && tb.rate > tb.maxRate {
		tb.rate = tb.maxRate
	}

	// Auto-ceiling: cap at 4x peak observed delivery rate.
	// This prevents unbounded probing on clean links while still leaving
	// enough headroom for the rate to grow naturally ahead of measurements.
	// The ceiling is skipped for the first 3 heartbeats because early
	// delivery-rate samples reflect cold-start conditions (the calibration
	// burst window), not actual link capacity — applying the ceiling then
	// would lock the sender far below what the link can sustain.
	const ceilingWarmupHeartbeats = 5
	const ceilingMultiplier = 4.0
	wasCapped := false
	if tb.heartbeatCount > ceilingWarmupHeartbeats && tb.peakRate > 0 && tb.rate > tb.peakRate*ceilingMultiplier {
		tb.rate = tb.peakRate * ceilingMultiplier
		wasCapped = true
	}

	if wasCapped && !tb.atCeiling {
		tb.atCeiling = true
		log.Printf("[congestion] CEILING: rate capped at %.2f MB/s (%.0fx peak delivery %.2f MB/s)",
			tb.rate/1e6, ceilingMultiplier, tb.peakRate/1e6)
	} else if !wasCapped {
		tb.atCeiling = false
	}

	// Floor: never go below 10 KB/s (effectively a minimum viable rate)
	if tb.rate < 10_000 {
		tb.rate = 10_000
	}

	return tb.rate
}

// ResetByteCounter resets the bytes-sent counter and returns the previous value.
// Used by the measurement window to compute the actual send rate.
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
