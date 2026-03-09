package sender

import (
	"hpuft/protocol"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// TokenBucket controls the sending rate using a token-based pacer with
// dual-metric feedback from receiver heartbeats. It supports sub-millisecond
// inter-packet pacing via busy-wait spin when delays are too short for
// the OS scheduler (typically < 1ms on Windows, < 100µs on Linux).
type TokenBucket struct {
	mu sync.Mutex

	// rate is the current send rate in bytes per second
	rate float64

	// maxRate caps the upward probe (0 = unlimited)
	maxRate float64

	// congestion control config
	cc protocol.CongestionConfig

	// lastSend is the time of the last packet dispatch
	lastSend time.Time

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

// Pace blocks until enough time has elapsed to send a packet of the given
// size at the current rate. Uses busy-wait spin for sub-millisecond precision.
func (tb *TokenBucket) Pace(packetBytes int) {
	tb.mu.Lock()
	rate := tb.rate
	lastSend := tb.lastSend
	tb.mu.Unlock()

	if rate <= 0 {
		return // no pacing
	}

	// Calculate required inter-packet interval
	interval := time.Duration(float64(packetBytes) / rate * float64(time.Second))

	// How long since last send?
	elapsed := time.Since(lastSend)
	remaining := interval - elapsed

	if remaining > 0 {
		if remaining > 2*time.Millisecond {
			// For longer waits, sleep most of it then spin the rest
			time.Sleep(remaining - time.Millisecond)
			spinUntil(lastSend.Add(interval))
		} else {
			// Sub-ms: busy-wait spin for precision
			spinUntil(lastSend.Add(interval))
		}
	}

	tb.mu.Lock()
	tb.lastSend = time.Now()
	tb.mu.Unlock()

	tb.bytesSent.Add(int64(packetBytes))
}

// spinUntil busy-waits until the target time. Yields the CPU occasionally
// to avoid starving other goroutines.
func spinUntil(target time.Time) {
	for time.Now().Before(target) {
		runtime.Gosched()
	}
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

	// Auto-ceiling: cap at 2x peak observed delivery rate.
	// There's no point targeting exabytes when the hardware delivers 41 MB/s.
	// The peak delivery rate is the best estimate of actual link capacity.
	// need to allow some headroom above the peak to account for measurement jitter and FEC bursts
	// potential problems: the hard limit could mask real improvements in link capacity if the receiver's processing rate is the bottleneck, but in practice this should be rare and the ceiling can be raised or removed via config if needed
	wasCapped := false
	if tb.peakRate > 0 && tb.rate > tb.peakRate*2 {
		tb.rate = tb.peakRate * 2
		wasCapped = true
	}

	if wasCapped && !tb.atCeiling {
		tb.atCeiling = true
		log.Printf("[congestion] CEILING: rate capped at %.2f MB/s (2x peak delivery %.2f MB/s)",
			tb.rate/1e6, tb.peakRate/1e6)
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
