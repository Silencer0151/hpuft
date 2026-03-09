package sender

import (
	"hpuft/protocol"
	"testing"
	"time"
)

func defaultCC() protocol.CongestionConfig {
	return protocol.DefaultCongestionConfig()
}

func hbWithLoss(lossBP uint16) *protocol.HeartbeatPayload {
	return &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 10_000_000, // 10 MB/s (doesn't drive decisions anymore)
		StorageFlushRate:    10_000_000,
		LossRate:            lossBP,
	}
}

func TestTokenBucketInitialRate(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())
	if tb.Rate() != 1_000_000 {
		t.Fatalf("initial rate = %f, want 1000000", tb.Rate())
	}
}

func TestIncreaseOnZeroLoss(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	newRate := tb.OnHeartbeat(hbWithLoss(0)) // 0% loss
	expected := 1_000_000 * 1.5
	if newRate != expected {
		t.Fatalf("rate = %f, want %f", newRate, expected)
	}
	if tb.Stats().Increases != 1 {
		t.Fatal("expected 1 increase")
	}
}

func TestIncreaseOnLowLoss(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	// 0.5% loss (50 bp) — still under 1%, should increase
	newRate := tb.OnHeartbeat(hbWithLoss(50))
	expected := 1_000_000 * 1.5
	if newRate != expected {
		t.Fatalf("rate = %f, want %f", newRate, expected)
	}
}

func TestHoldOnModerateLoss(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	// 2% loss (200 bp) — FEC handles it, hold
	tb.OnHeartbeat(hbWithLoss(200))
	if tb.Rate() != 1_000_000 {
		t.Fatalf("rate = %f, want 1000000 (hold)", tb.Rate())
	}
	if tb.Stats().Holds != 1 {
		t.Fatal("expected 1 hold")
	}
}

func TestHoldAtBoundary(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	// Exactly 1% (100 bp) — boundary, should hold
	tb.OnHeartbeat(hbWithLoss(100))
	if tb.Rate() != 1_000_000 {
		t.Fatalf("rate = %f, want 1000000 (hold at 1%%)", tb.Rate())
	}

	// Exactly 5% (500 bp) — upper hold boundary, still hold
	tb.OnHeartbeat(hbWithLoss(500))
	if tb.Rate() != 1_000_000 {
		t.Fatalf("rate = %f, want 1000000 (hold at 5%%)", tb.Rate())
	}
}

func TestDecreaseRequiresConsecutive(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 500_000, // delivery matching what the link can do
		StorageFlushRate:    500_000,
		LossRate:            600, // 6% loss
	}

	// First > 5% loss signal — should hold (streak=1)
	tb.OnHeartbeat(hb)
	if tb.Rate() != 1_000_000 {
		t.Fatalf("first signal should hold: rate = %f", tb.Rate())
	}
	if tb.Stats().Holds != 1 {
		t.Fatal("first signal counted as hold")
	}

	// Second > 5% signal — now decrease
	tb.OnHeartbeat(hb)
	if tb.Rate() >= 1_000_000 {
		t.Fatalf("second signal should decrease: rate = %f", tb.Rate())
	}
	if tb.Stats().Decreases != 1 {
		t.Fatal("expected 1 decrease")
	}
}

func TestStreakResetOnLowLoss(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	// Start decrease streak with appropriate delivery rate
	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 500_000,
		StorageFlushRate:    500_000,
		LossRate:            600,
	}
	tb.OnHeartbeat(hb) // streak=1

	// Low loss resets streak
	tb.OnHeartbeat(hbWithLoss(0)) // increase, streak=0

	// New high loss — should hold again (streak=1, not 2)
	tb.OnHeartbeat(hb)
	if tb.Stats().Decreases != 0 {
		t.Fatal("streak should have reset — no decrease expected")
	}
}

func TestContinuousIncreaseOnCleanLink(t *testing.T) {
	// Simulates a LAN with zero loss — should ramp up and hit ceiling
	tb := NewTokenBucket(1_000_000, defaultCC()) // 1 MB/s start

	// hbWithLoss reports 10 MB/s delivery, so ceiling = 2 * 10M = 20M
	for i := 0; i < 10; i++ {
		tb.OnHeartbeat(hbWithLoss(0))
	}

	actual := tb.Rate()
	// Should hit the auto-ceiling at 4x peak delivery (40 MB/s), not 57.7 MB/s.
	// The ceiling uses a 4x multiplier to avoid the cold-start false-throttle
	// that 2x caused on the first heartbeat.
	expectedCeiling := 40_000_000.0
	if actual < expectedCeiling*0.99 || actual > expectedCeiling*1.01 {
		t.Fatalf("after 10 increases: rate = %.2f MB/s, want ~%.2f MB/s (ceiling)",
			actual/1e6, expectedCeiling/1e6)
	}

	if tb.Stats().Increases != 10 {
		t.Fatalf("increases = %d, want 10", tb.Stats().Increases)
	}
}

func TestDecreaseUsesDeliveryRate(t *testing.T) {
	tb := NewTokenBucket(10_000_000, defaultCC()) // 10 MB/s

	// High loss with low delivery rate
	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 2_000_000, // 2 MB/s actual delivery
		StorageFlushRate:    2_000_000,
		LossRate:            1000, // 10% loss
	}

	tb.OnHeartbeat(hb) // streak=1, hold
	tb.OnHeartbeat(hb) // streak=2, decrease

	// Should decrease to smoothed(2M) * 1.05 = 2.1M, not stay at 10M
	rate := tb.Rate()
	if rate > 3_000_000 {
		t.Fatalf("decrease should target delivery rate: got %.2f MB/s", rate/1e6)
	}
}

func TestFlowControlCeiling(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	// Zero loss but storage is slow — delivery rate caps us
	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 50_000_000,
		StorageFlushRate:    500_000, // disk bottleneck at 0.5 MB/s
		LossRate:            0,
	}

	// Will increase (zero loss), but on decrease the delivery rate ceiling kicks in
	tb.OnHeartbeat(hb) // increase to 1.5M

	// Now force a decrease with high loss and low storage
	hb.LossRate = 800
	tb.OnHeartbeat(hb) // streak=1
	tb.OnHeartbeat(hb) // streak=2, decrease to smoothed(min(50M, 500K)) * 1.05

	rate := tb.Rate()
	if rate > 1_000_000 {
		t.Fatalf("storage ceiling should limit rate: got %.2f MB/s", rate/1e6)
	}
}

func TestRateFloor(t *testing.T) {
	tb := NewTokenBucket(100_000, defaultCC())

	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 100,
		StorageFlushRate:    100,
		LossRate:            2000, // 20% loss
	}

	tb.OnHeartbeat(hb) // streak=1
	tb.OnHeartbeat(hb) // streak=2, decrease

	if tb.Rate() != 10_000 {
		t.Fatalf("floor: rate = %f, want 10000", tb.Rate())
	}
}

func TestMaxRateCap(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())
	tb.SetMaxRate(2_000_000)

	// Three increases: 1M -> 1.5M -> 2.25M (capped to 2M) -> 3M (capped to 2M)
	tb.OnHeartbeat(hbWithLoss(0))
	tb.OnHeartbeat(hbWithLoss(0))
	tb.OnHeartbeat(hbWithLoss(0))

	if tb.Rate() != 2_000_000 {
		t.Fatalf("capped: rate = %f, want 2000000", tb.Rate())
	}
}

func TestRapidRecoveryAfterDecrease(t *testing.T) {
	tb := NewTokenBucket(10_000_000, defaultCC())

	// Force decrease
	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 2_000_000,
		StorageFlushRate:    2_000_000,
		LossRate:            1500, // 15% loss
	}
	tb.OnHeartbeat(hb)
	tb.OnHeartbeat(hb)
	rateAfter := tb.Rate()

	// Recovery with zero loss
	tb.OnHeartbeat(hbWithLoss(0))
	rate1 := tb.Rate()
	if rate1 <= rateAfter {
		t.Fatalf("should increase after loss clears: %f <= %f", rate1, rateAfter)
	}

	tb.OnHeartbeat(hbWithLoss(0))
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
	if tb.ResetByteCounter() != 0 {
		t.Fatal("second reset should be 0")
	}
}

func TestPaceProducesDelay(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	tb.Pace(100)
	start := time.Now()
	tb.Pace(1000)
	elapsed := time.Since(start)

	if elapsed < 500*time.Microsecond || elapsed > 50*time.Millisecond {
		t.Fatalf("pace timing: %v (expected ~1ms)", elapsed)
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

	// Zero loss → increase by 2x (custom multiplier)
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 1_000_000,
		StorageFlushRate:    1_000_000,
		LossRate:            0,
	})
	if tb.Rate() != 2_000_000 {
		t.Fatalf("custom 2x: rate = %f, want 2000000", tb.Rate())
	}

	// Decrease with custom headroom: use consistent low delivery rate
	// so EWMA converges to ~500K
	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 500_000,
		StorageFlushRate:    500_000,
		LossRate:            800, // 8% loss
	}
	// Feed enough heartbeats for EWMA to converge toward 500K
	tb.OnHeartbeat(hb) // streak=1, hold
	tb.OnHeartbeat(hb) // streak=2, decrease
	tb.OnHeartbeat(hb) // streak continues, decrease again
	tb.OnHeartbeat(hb) // EWMA converging

	rate := tb.Rate()
	// Should be near 500K * 1.10 = 550K after EWMA converges
	if rate > 700_000 {
		t.Fatalf("custom headroom: rate = %f, expected < 700K", rate)
	}
}
