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
// sending rate according to the spec §6C rate adjustment algorithm,
// with EWMA smoothing to dampen jitter and a consecutive-decrease
// requirement to prevent single-heartbeat crashes.
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

	// EWMA smoothing: dampens single-heartbeat jitter on real networks.
	// smoothed = alpha * new_sample + (1-alpha) * old_smoothed
	if !tb.ewmaInit {
		tb.smoothedRate = rawEffective
		tb.ewmaInit = true
	} else {
		tb.smoothedRate = tb.ewmaAlpha*rawEffective + (1-tb.ewmaAlpha)*tb.smoothedRate
	}
	effectiveRate := tb.smoothedRate

	// Track peak for reference
	if effectiveRate > tb.peakRate {
		tb.peakRate = effectiveRate
	}

	oldRate := tb.rate

	switch {
	case effectiveRate >= tb.cc.IncreaseThreshold*oldRate:
		// Link and receiver keeping up — probe upward
		tb.rate = oldRate * tb.cc.IncreaseMultiplier
		tb.decreaseStreak = 0
		tb.increases.Add(1)
		log.Printf("[congestion] INCREASE: %.2f -> %.2f MB/s (smoothed=%.2f raw=%.2f)",
			oldRate/1e6, tb.rate/1e6, effectiveRate/1e6, rawEffective/1e6)

	case effectiveRate >= tb.cc.HoldThreshold*oldRate:
		// Minor variance — hold steady
		tb.decreaseStreak = 0
		tb.holds.Add(1)

	default:
		// Potential congestion signal — but require consecutive confirmation
		tb.decreaseStreak++

		if tb.decreaseStreak >= 2 {
			// Confirmed congestion: decrease to smoothed effective + headroom
			tb.rate = effectiveRate * tb.cc.DecreaseHeadroom
			tb.decreases.Add(1)
			log.Printf("[congestion] DECREASE: %.2f -> %.2f MB/s (smoothed=%.2f raw=%.2f, streak=%d)",
				oldRate/1e6, tb.rate/1e6, effectiveRate/1e6, rawEffective/1e6, tb.decreaseStreak)
		} else {
			// First signal — hold and wait for confirmation
			tb.holds.Add(1)
		}
	}

	// Apply max rate cap if configured
	if tb.maxRate > 0 && tb.rate > tb.maxRate {
		tb.rate = tb.maxRate
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
