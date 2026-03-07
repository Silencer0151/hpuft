package sender

import (
	"hpuft/protocol"
	"testing"
	"time"
)

func defaultCC() protocol.CongestionConfig {
	return protocol.DefaultCongestionConfig()
}

func TestTokenBucketInitialRate(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())
	if tb.Rate() != 1_000_000 {
		t.Fatalf("initial rate = %f, want 1000000", tb.Rate())
	}
}

func TestRateIncrease(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 960_000, // 0.96 * 1M -> above 0.95 threshold
		StorageFlushRate:    960_000,
	}

	newRate := tb.OnHeartbeat(hb)
	expected := 1_000_000 * 1.5
	if newRate != expected {
		t.Fatalf("after increase: rate = %f, want %f", newRate, expected)
	}

	stats := tb.Stats()
	if stats.Increases != 1 {
		t.Fatalf("increases = %d, want 1", stats.Increases)
	}
}

func TestRateHold(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 900_000, // 0.90 -> hold band
		StorageFlushRate:    900_000,
	}

	newRate := tb.OnHeartbeat(hb)
	if newRate != 1_000_000 {
		t.Fatalf("after hold: rate = %f, want 1000000", newRate)
	}

	stats := tb.Stats()
	if stats.Holds != 1 {
		t.Fatalf("holds = %d, want 1", stats.Holds)
	}
}

func TestRateDecreaseRequiresTwoConsecutive(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 500_000, // well below 0.85
		StorageFlushRate:    500_000,
	}

	// First signal: should HOLD (not decrease yet)
	rate1 := tb.OnHeartbeat(hb)
	if rate1 != 1_000_000 {
		t.Fatalf("first decrease signal should hold: rate = %f, want 1000000", rate1)
	}

	// Second signal: now should DECREASE
	rate2 := tb.OnHeartbeat(hb)
	// EWMA of constant 500K = 500K, so decrease to 500K * 1.05 = 525K
	expected := 500_000 * 1.05
	if rate2 != expected {
		t.Fatalf("after consecutive decrease: rate = %f, want %f", rate2, expected)
	}

	stats := tb.Stats()
	if stats.Holds != 1 {
		t.Fatalf("holds = %d, want 1 (first signal was held)", stats.Holds)
	}
	if stats.Decreases != 1 {
		t.Fatalf("decreases = %d, want 1", stats.Decreases)
	}
}

func TestIncreaseResetsDecreaseStreak(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	// One decrease signal (streak = 1)
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 500_000,
		StorageFlushRate:    500_000,
	})

	// Increase resets streak
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 2_000_000, // well above 0.95 * 1M
		StorageFlushRate:    2_000_000,
	})

	// New decrease signal after reset — should hold again (streak restarted)
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 100_000,
		StorageFlushRate:    100_000,
	})

	stats := tb.Stats()
	if stats.Decreases != 0 {
		t.Fatalf("decrease should not fire after streak reset: decreases=%d", stats.Decreases)
	}
}

func TestRateUsesMinOfNetworkAndStorage(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 2_000_000,
		StorageFlushRate:    400_000, // disk is bottleneck
	}

	// First signal: hold
	tb.OnHeartbeat(hb)
	// Second signal: decrease to smoothed(400K) * 1.05
	rate := tb.OnHeartbeat(hb)
	expected := 400_000 * 1.05
	if rate != expected {
		t.Fatalf("disk bottleneck: rate = %f, want %f", rate, expected)
	}
}

func TestRateFloor(t *testing.T) {
	tb := NewTokenBucket(100_000, defaultCC())

	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 100,
		StorageFlushRate:    100,
	}

	// Need two consecutive to trigger decrease
	tb.OnHeartbeat(hb)
	newRate := tb.OnHeartbeat(hb)
	if newRate != 10_000 {
		t.Fatalf("below floor: rate = %f, want 10000", newRate)
	}
}

func TestMaxRateCap(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())
	tb.SetMaxRate(1_200_000)

	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 960_000,
		StorageFlushRate:    960_000,
	}

	newRate := tb.OnHeartbeat(hb)
	if newRate != 1_200_000 {
		t.Fatalf("capped rate = %f, want 1200000", newRate)
	}
}

func TestEWMASmoothing(t *testing.T) {
	tb := NewTokenBucket(10_000_000, defaultCC()) // 10 MB/s

	// First heartbeat: EWMA initializes to raw value
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 9_600_000, // triggers increase
		StorageFlushRate:    9_600_000,
	})
	// Rate is now 15 MB/s

	// Send a jittery low heartbeat followed by a normal one
	// The smoothed rate should dampen the spike
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 1_000_000, // spike down (raw)
		StorageFlushRate:    1_000_000,
	})
	// EWMA: 0.3 * 1M + 0.7 * 9.6M = 7.02M
	// 7.02M / 15M = 0.468 -> below 0.85 -> streak=1, HOLD

	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 14_000_000, // back to normal
		StorageFlushRate:    14_000_000,
	})
	// EWMA: 0.3 * 14M + 0.7 * 7.02M = 4.2M + 4.914M = 9.114M
	// 9.114M / 15M = 0.608 -> below 0.85 but streak reset by... no, it's still below
	// Actually streak=2 now, so this would trigger decrease

	// The key insight: a single jittery heartbeat doesn't crash the rate
	// because the first one only starts the streak (holds)
	stats := tb.Stats()
	if stats.Decreases > 1 {
		t.Fatalf("EWMA should prevent multiple rapid decreases, got %d", stats.Decreases)
	}
}

func TestRapidRecoveryAfterDecrease(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	// Force a decrease (two consecutive low signals)
	low := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 500_000,
		StorageFlushRate:    500_000,
	}
	tb.OnHeartbeat(low)
	tb.OnHeartbeat(low)
	// Rate is now ~525K

	rateAfterDecrease := tb.Rate()

	// Now send high signals — should increase 1.5x each time
	high := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 10_000_000, // way above anything
		StorageFlushRate:    10_000_000,
	}

	tb.OnHeartbeat(high)
	rate1 := tb.Rate()
	if rate1 <= rateAfterDecrease {
		t.Fatalf("should increase after high signal: %f <= %f", rate1, rateAfterDecrease)
	}

	tb.OnHeartbeat(high)
	rate2 := tb.Rate()
	if rate2 <= rate1 {
		t.Fatalf("should keep increasing: %f <= %f", rate2, rate1)
	}
}

func TestResetByteCounter(t *testing.T) {
	tb := NewTokenBucket(1_000_000_000, defaultCC())
	tb.bytesSent.Store(5000)

	val := tb.ResetByteCounter()
	if val != 5000 {
		t.Fatalf("ResetByteCounter = %d, want 5000", val)
	}

	val = tb.ResetByteCounter()
	if val != 0 {
		t.Fatalf("second ResetByteCounter = %d, want 0", val)
	}
}

func TestPaceProducesDelay(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	tb.Pace(100)

	start := time.Now()
	tb.Pace(1000)
	elapsed := time.Since(start)

	if elapsed < 500*time.Microsecond {
		t.Fatalf("pace too fast: %v (expected ~1ms)", elapsed)
	}
	if elapsed > 10*time.Millisecond {
		t.Fatalf("pace too slow: %v (expected ~1ms)", elapsed)
	}
}

func TestCustomCongestionConfig(t *testing.T) {
	cc := protocol.CongestionConfig{
		IncreaseThreshold:  0.90,
		HoldThreshold:      0.70,
		IncreaseMultiplier: 2.0,
		DecreaseHeadroom:   1.10,
	}

	tb := NewTokenBucket(1_000_000, cc)

	// 0.91 * 1M = 910K >= 0.90 * 1M -> increase by 2x
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 910_000,
		StorageFlushRate:    910_000,
	})
	if tb.Rate() != 2_000_000 {
		t.Fatalf("custom 2x increase: rate = %f, want 2000000", tb.Rate())
	}

	// Two consecutive decreases with custom headroom
	low := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 500_000,
		StorageFlushRate:    500_000,
	}
	tb.OnHeartbeat(low)
	tb.OnHeartbeat(low)
	// EWMA of 500K after init at 910K then two 500Ks:
	// t1 (init): 910K
	// t2 (increase): streak reset, EWMA=0.3*910K+0.7*910K=910K... wait
	// Actually the first OnHeartbeat was 910K which triggered increase.
	// Then low=500K: EWMA = 0.3*500K + 0.7*910K = 150K+637K = 787K
	// 787K < 0.70 * 2M = 1.4M -> streak=1, HOLD
	// Then low=500K: EWMA = 0.3*500K + 0.7*787K = 150K+550.9K = 700.9K
	// 700.9K < 0.70 * 2M = 1.4M -> streak=2, DECREASE to 700.9K * 1.1
	expected := (0.3*500_000 + 0.7*(0.3*500_000+0.7*910_000)) * 1.10
	actual := tb.Rate()
	// Allow some float tolerance
	if actual < expected*0.99 || actual > expected*1.01 {
		t.Fatalf("custom headroom: rate = %f, want ~%f", actual, expected)
	}
}

func TestThresholdBoundaryIncrease(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	// Exactly at increase threshold
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 950_000,
		StorageFlushRate:    950_000,
	})
	if tb.Stats().Increases != 1 {
		t.Fatal("E == 0.95*S should trigger increase")
	}
}

func TestThresholdBoundaryHold(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	// Just below increase but in hold band
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 949_000,
		StorageFlushRate:    949_000,
	})
	if tb.Stats().Holds != 1 {
		t.Fatal("E = 0.949*S should trigger hold")
	}
}

func TestThresholdBoundaryHoldLower(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	// Exactly at hold threshold lower bound
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 850_000,
		StorageFlushRate:    850_000,
	})
	if tb.Stats().Holds != 1 {
		t.Fatal("E == 0.85*S should trigger hold")
	}
}

func TestThresholdBoundaryDecreaseSignal(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	// Just below hold threshold — first one should hold (streak=1)
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 849_000,
		StorageFlushRate:    849_000,
	})

	stats := tb.Stats()
	// First decrease signal is held due to consecutive requirement
	if stats.Holds != 1 {
		t.Fatalf("first below-threshold signal should hold, holds=%d decreases=%d",
			stats.Holds, stats.Decreases)
	}

	// Second one triggers actual decrease
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 849_000,
		StorageFlushRate:    849_000,
	})
	stats = tb.Stats()
	if stats.Decreases != 1 {
		t.Fatalf("second below-threshold signal should decrease, decreases=%d", stats.Decreases)
	}
}
