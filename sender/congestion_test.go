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
		NetworkDeliveryRate: 10_000_000, // 10 MB/s delivery
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

	newRate := tb.OnHeartbeat(hbWithLoss(0)) // 0% loss, Phase 1: 1.25×
	expected := 1_000_000 * 1.25
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
	expected := 1_000_000 * 1.25
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
		NetworkDeliveryRate: 500_000,
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
	// Clean link, zero loss — Phase 1 ramps up and hits the Phase 1 ceiling
	// at 4× peak delivery. hbWithLoss(0) reports 10 MB/s delivery, so
	// peakRate = 10 MB/s and ceiling = 40 MB/s.
	// 1 MB/s × 1.25^N > 40 MB/s → N ≈ 17. Run 20 iterations to be safe.
	tb := NewTokenBucket(1_000_000, defaultCC())

	for i := 0; i < 20; i++ {
		tb.OnHeartbeat(hbWithLoss(0))
	}

	actual := tb.Rate()
	expectedCeiling := 40_000_000.0
	if actual < expectedCeiling*0.99 || actual > expectedCeiling*1.01 {
		t.Fatalf("after 20 HBs: rate = %.2f MB/s, want ~%.2f MB/s (Phase 1 ceiling = 4× peak delivery 10 MB/s)",
			actual/1e6, expectedCeiling/1e6)
	}
}

func TestCeilingActivatesInPhase2(t *testing.T) {
	// Ceiling = 1.2× peak delivery should fire when Phase 2 is entered and
	// the current rate is above the ceiling. Start at 20 MB/s (above the
	// 1.2 × 10 MB/s = 12 MB/s ceiling), enter Phase 2 via hold zone, and
	// verify the ceiling is applied immediately on that same heartbeat.
	tb := NewTokenBucket(20_000_000, defaultCC())

	// Enter Phase 2: 2% loss (hold zone). Rate is held, but ceiling fires
	// because 20 MB/s > 1.2 × peakDelivery (10 MB/s) = 12 MB/s.
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 10_000_000,
		StorageFlushRate:    10_000_000,
		LossRate:            200,
	})
	if !tb.inPhase2 {
		t.Fatal("expected Phase 2 after hold-zone heartbeat")
	}

	expectedCeiling := 12_000_000.0
	actual := tb.Rate()
	if actual < expectedCeiling*0.99 || actual > expectedCeiling*1.01 {
		t.Fatalf("Phase 2 ceiling: rate = %.2f MB/s, want ~%.2f MB/s (1.2× peak delivery 10 MB/s)",
			actual/1e6, expectedCeiling/1e6)
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
	tb.OnHeartbeat(hb) // streak=2, decrease to smoothed(2M) × 0.85 ≈ 1.7M

	rate := tb.Rate()
	if rate > 3_000_000 {
		t.Fatalf("decrease should target delivery rate × 0.85: got %.2f MB/s", rate/1e6)
	}
}

func TestFlowControlCeiling(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())

	// Zero loss — rate probes up; StorageFlushRate is intentionally ignored.
	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 50_000_000,
		StorageFlushRate:    500_000, // slow disk: irrelevant to CC
		LossRate:            0,
	}

	tb.OnHeartbeat(hb) // increase: 1.25 MB/s, smoothedRate seeded at 50 MB/s

	// Two consecutive high-loss heartbeats trigger a decrease to smoothedRate × 0.85.
	hb.LossRate = 800
	tb.OnHeartbeat(hb) // streak=1, hold
	tb.OnHeartbeat(hb) // streak=2, decrease → smoothed(50 MB/s) × 0.85 ≈ 42.5 MB/s

	rate := tb.Rate()
	// Rate should have dropped to ~85% of the measured delivery ceiling, not
	// remain at the probed-up value. It will be far above the old storage
	// ceiling (0.5 MB/s) because disk rate no longer drives CC decisions.
	const wantBelow = 45_000_000.0
	const wantAbove = 30_000_000.0
	if rate > wantBelow || rate < wantAbove {
		t.Fatalf("rate after decrease: got %.2f MB/s, want %.1f–%.1f MB/s",
			rate/1e6, wantAbove/1e6, wantBelow/1e6)
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
	tb.OnHeartbeat(hb) // streak=2, decrease → floor

	if tb.Rate() != 10_000 {
		t.Fatalf("floor: rate = %f, want 10000", tb.Rate())
	}
}

func TestMaxRateCap(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())
	tb.SetMaxRate(2_000_000)

	// 1.25× per HB: 1M → 1.25M → 1.5625M → 1.953M → 2.441M (capped to 2M)
	tb.OnHeartbeat(hbWithLoss(0))
	tb.OnHeartbeat(hbWithLoss(0))
	tb.OnHeartbeat(hbWithLoss(0))
	tb.OnHeartbeat(hbWithLoss(0)) // 4th increase would exceed 2M cap

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

	// Recovery with zero loss — should increase (Phase 1 since loss never
	// entered the 1-5% hold zone in this test)
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
		Phase1Multiplier: 2.0,
		DecreaseFrac:     0.70,
	}
	tb := NewTokenBucket(1_000_000, cc)

	// Zero loss → increase by 2x (custom Phase1Multiplier)
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 1_000_000,
		StorageFlushRate:    1_000_000,
		LossRate:            0,
	})
	if tb.Rate() != 2_000_000 {
		t.Fatalf("custom 2x: rate = %f, want 2000000", tb.Rate())
	}

	// Decrease with custom DecreaseFrac=0.70: rate targets smoothed(500K) × 0.70
	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 500_000,
		StorageFlushRate:    500_000,
		LossRate:            800, // 8% loss
	}
	tb.OnHeartbeat(hb) // streak=1, hold
	tb.OnHeartbeat(hb) // streak=2, decrease
	tb.OnHeartbeat(hb) // streak continues, EWMA converging
	tb.OnHeartbeat(hb)

	rate := tb.Rate()
	// With DecreaseFrac=0.70 and EWMA converging toward 500K, expect rate < 600K
	if rate > 600_000 {
		t.Fatalf("custom DecreaseFrac=0.70: rate = %f, expected < 600K", rate)
	}
}

func TestCompoundCollapseDecrease(t *testing.T) {
	// When delivery collapses (delivery < 25% of rate with NACKs), the CC
	// holds for 5 consecutive heartbeats, then forces a decrease using
	// rate × DecreaseFrac (not peakRate). Successive decreases compound.
	tb := NewTokenBucket(100_000_000, defaultCC()) // 100 MB/s

	// Seed peakRate so ceiling is active
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 100_000_000,
		StorageFlushRate:    100_000_000,
		LossRate:            0,
	})

	// Now simulate delivery collapse: delivery << rate, with NACKs.
	collapseHB := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 1_000_000, // 1 MB/s — well below 25% of rate
		StorageFlushRate:    1_000_000,
		LossRate:            0, // 0% loss — the collapse is invisible to FEC
		NACKCount:           10,
	}

	// First 4 collapse heartbeats should hold (streak < 5)
	for i := 0; i < 4; i++ {
		tb.OnHeartbeat(collapseHB)
	}
	rateBeforeDecrease := tb.Rate()
	if tb.Stats().Decreases != 0 {
		t.Fatalf("expected 0 decreases during collapse hold, got %d", tb.Stats().Decreases)
	}

	// 5th collapse heartbeat triggers compound decrease: rate × 0.85
	tb.OnHeartbeat(collapseHB)
	rateAfterFirst := tb.Rate()
	if tb.Stats().Decreases != 1 {
		t.Fatalf("expected 1 decrease after 5 collapse holds, got %d", tb.Stats().Decreases)
	}
	expected := rateBeforeDecrease * 0.85
	if rateAfterFirst < expected*0.95 || rateAfterFirst > expected*1.05 {
		t.Fatalf("first collapse-decrease: got %.2f MB/s, want ~%.2f MB/s (%.2f × 0.85)",
			rateAfterFirst/1e6, expected/1e6, rateBeforeDecrease/1e6)
	}

	// Streak resets after decrease. 5 more collapse holds → second compound decrease.
	for i := 0; i < 4; i++ {
		tb.OnHeartbeat(collapseHB)
	}
	tb.OnHeartbeat(collapseHB) // triggers second decrease
	rateAfterSecond := tb.Rate()
	if tb.Stats().Decreases != 2 {
		t.Fatalf("expected 2 decreases, got %d", tb.Stats().Decreases)
	}

	// Second decrease compounds: rateAfterFirst × 0.85
	expected2 := rateAfterFirst * 0.85
	if rateAfterSecond < expected2*0.95 || rateAfterSecond > expected2*1.05 {
		t.Fatalf("second collapse-decrease: got %.2f MB/s, want ~%.2f MB/s (%.2f × 0.85)",
			rateAfterSecond/1e6, expected2/1e6, rateAfterFirst/1e6)
	}
}

func TestCollapseDecreaseFloor(t *testing.T) {
	// Compound collapse-decrease has a floor at 50% of peakRate.
	// Start with a low rate near the floor and verify it doesn't drop below.
	tb := NewTokenBucket(10_000_000, defaultCC()) // 10 MB/s

	// Seed peakRate at 10 MB/s
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 10_000_000,
		StorageFlushRate:    10_000_000,
		LossRate:            0,
	})

	// Set rate just above the floor (50% of peak = 5 MB/s)
	// Force Phase 2 entry + ceiling to bring rate down near peakRate×1.2 = 12 MB/s
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 10_000_000,
		StorageFlushRate:    10_000_000,
		LossRate:            200, // hold zone → Phase 2
	})

	collapseHB := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 100_000, // extreme collapse
		StorageFlushRate:    100_000,
		LossRate:            0,
		NACKCount:           50,
	}

	// Keep triggering collapse-decreases until we hit the floor
	for i := 0; i < 50; i++ {
		tb.OnHeartbeat(collapseHB)
	}

	rate := tb.Rate()
	floor := 10_000_000.0 * 0.50 // 5 MB/s
	if rate < floor*0.99 {
		t.Fatalf("rate %.2f MB/s dropped below floor %.2f MB/s (50%% of peakRate)",
			rate/1e6, floor/1e6)
	}
}

func TestPhase1CeilingAt4x(t *testing.T) {
	// Phase 1 ceiling = 4× peak delivery. This is already covered by
	// TestContinuousIncreaseOnCleanLink but we verify the exact multiplier
	// explicitly here with a different starting rate.
	tb := NewTokenBucket(5_000_000, defaultCC()) // 5 MB/s

	// Peak delivery = 20 MB/s → Phase 1 ceiling = 80 MB/s
	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 20_000_000,
		StorageFlushRate:    20_000_000,
		LossRate:            0,
	}

	// Run enough heartbeats to hit ceiling: 5M × 1.25^N > 80M → N ≈ 13
	for i := 0; i < 20; i++ {
		tb.OnHeartbeat(hb)
	}

	actual := tb.Rate()
	expectedCeiling := 80_000_000.0
	if actual < expectedCeiling*0.99 || actual > expectedCeiling*1.01 {
		t.Fatalf("Phase 1 ceiling: rate = %.2f MB/s, want ~%.2f MB/s (4× peak delivery 20 MB/s)",
			actual/1e6, expectedCeiling/1e6)
	}

	// Confirm still in Phase 1 (never entered hold zone)
	if tb.inPhase2 {
		t.Fatal("should still be in Phase 1 — no hold zone entry")
	}
}

func TestPhase2PermanentTransition(t *testing.T) {
	tb := NewTokenBucket(5_000_000, defaultCC())

	// Trigger hold zone (1-5%): permanently enters Phase 2
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 5_000_000,
		StorageFlushRate:    5_000_000,
		LossRate:            200, // 2% — hold zone
	})
	if !tb.inPhase2 {
		t.Fatal("should have entered Phase 2 after hold zone")
	}

	// After Phase 2 entry, zero loss → additive increase (not multiplicative)
	before := tb.Rate()
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 5_000_000,
		StorageFlushRate:    5_000_000,
		LossRate:            0,
	})
	after := tb.Rate()

	// Additive increase is small: MaxPayload / heartbeatInterval
	// Should increase, but much less than 25% (Phase 1 would be 1.25×)
	delta := after - before
	maxPhase1Delta := before * 0.25 // 25% of before rate
	if delta <= 0 {
		t.Fatalf("Phase 2: rate should increase, got delta=%.2f", delta)
	}
	if delta >= maxPhase1Delta {
		t.Fatalf("Phase 2: additive delta (%.2f KB/s) should be smaller than Phase 1 25%% (%.2f KB/s)",
			delta/1000, maxPhase1Delta/1000)
	}
}
