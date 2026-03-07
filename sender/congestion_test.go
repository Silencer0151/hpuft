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
	tb := NewTokenBucket(1_000_000, defaultCC()) // 1 MB/s
	if tb.Rate() != 1_000_000 {
		t.Fatalf("initial rate = %f, want 1000000", tb.Rate())
	}
}

func TestRateIncrease(t *testing.T) {
	// When effective rate >= 0.95 * send rate, rate should increase by 1.5x
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
	// When 0.85 * S <= E < 0.95 * S, rate should hold steady
	tb := NewTokenBucket(1_000_000, defaultCC())

	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 900_000, // 0.90 * 1M -> between 0.85 and 0.95
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

func TestRateDecrease(t *testing.T) {
	// When E < 0.85 * S, rate should decrease to E * 1.05
	tb := NewTokenBucket(1_000_000, defaultCC())

	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 500_000, // 0.50 * 1M -> well below 0.85
		StorageFlushRate:    500_000,
	}

	newRate := tb.OnHeartbeat(hb)
	expected := 500_000 * 1.05
	if newRate != expected {
		t.Fatalf("after decrease: rate = %f, want %f", newRate, expected)
	}

	stats := tb.Stats()
	if stats.Decreases != 1 {
		t.Fatalf("decreases = %d, want 1", stats.Decreases)
	}
}

func TestRateUsesMinOfNetworkAndStorage(t *testing.T) {
	// Effective rate = min(network, storage)
	// If network is fast but disk is slow, should pace to disk
	tb := NewTokenBucket(1_000_000, defaultCC())

	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 2_000_000, // network is fine
		StorageFlushRate:    400_000,   // disk is bottleneck -> E = 400K, way below 0.85*1M
	}

	newRate := tb.OnHeartbeat(hb)
	expected := 400_000 * 1.05
	if newRate != expected {
		t.Fatalf("disk bottleneck: rate = %f, want %f", newRate, expected)
	}
}

func TestRateFloor(t *testing.T) {
	// Rate should never drop below 10 KB/s
	tb := NewTokenBucket(100_000, defaultCC())

	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 100, // extremely low
		StorageFlushRate:    100,
	}

	newRate := tb.OnHeartbeat(hb)
	if newRate != 10_000 {
		t.Fatalf("below floor: rate = %f, want 10000", newRate)
	}
}

func TestMaxRateCap(t *testing.T) {
	tb := NewTokenBucket(1_000_000, defaultCC())
	tb.SetMaxRate(1_200_000) // cap at 1.2 MB/s

	// Trigger increase: 1M * 1.5 = 1.5M, should be capped to 1.2M
	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 960_000,
		StorageFlushRate:    960_000,
	}

	newRate := tb.OnHeartbeat(hb)
	if newRate != 1_200_000 {
		t.Fatalf("capped rate = %f, want 1200000", newRate)
	}
}

func TestMultipleHeartbeats(t *testing.T) {
	// Simulate a sequence: increase, increase, decrease, hold
	tb := NewTokenBucket(100_000, defaultCC())

	// Increase 1: 100K -> 150K
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 96_000,
		StorageFlushRate:    96_000,
	})
	rate1 := tb.Rate()
	if rate1 != 150_000 {
		t.Fatalf("after increase 1: rate = %f, want 150000", rate1)
	}

	// Increase 2: 150K -> 225K
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 145_000,
		StorageFlushRate:    145_000,
	})
	rate2 := tb.Rate()
	if rate2 != 225_000 {
		t.Fatalf("after increase 2: rate = %f, want 225000", rate2)
	}

	// Decrease: E = 100K, well below 0.85*225K=191K -> rate = 100K * 1.05 = 105K
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 100_000,
		StorageFlushRate:    100_000,
	})
	rate3 := tb.Rate()
	expected := 100_000 * 1.05
	if rate3 != expected {
		t.Fatalf("after decrease: rate = %f, want %f", rate3, expected)
	}

	// Hold: E = 95K, 0.85*105K=89.25K <= 95K < 0.95*105K=99.75K
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 95_000,
		StorageFlushRate:    95_000,
	})
	rate4 := tb.Rate()
	if rate4 != expected {
		t.Fatalf("after hold: rate = %f, want %f (unchanged)", rate4, expected)
	}

	stats := tb.Stats()
	if stats.Increases != 2 || stats.Holds != 1 || stats.Decreases != 1 {
		t.Fatalf("stats: +%d =%d -%d, want +2 =1 -1",
			stats.Increases, stats.Holds, stats.Decreases)
	}
}

func TestThresholdBoundaryExact(t *testing.T) {
	cc := defaultCC()
	tb := NewTokenBucket(1_000_000, cc)

	// Exactly at increase threshold: E = 0.95 * S
	hb := &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 950_000,
		StorageFlushRate:    950_000,
	}
	tb.OnHeartbeat(hb)
	if tb.Stats().Increases != 1 {
		t.Fatal("E == 0.95*S should trigger increase")
	}

	// Reset
	tb = NewTokenBucket(1_000_000, cc)

	// Just below increase threshold: E = 0.949 * S -> should hold
	hb = &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 949_000,
		StorageFlushRate:    949_000,
	}
	tb.OnHeartbeat(hb)
	if tb.Stats().Holds != 1 {
		t.Fatal("E = 0.949*S should trigger hold")
	}

	// Reset
	tb = NewTokenBucket(1_000_000, cc)

	// Exactly at hold threshold: E = 0.85 * S -> should hold
	hb = &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 850_000,
		StorageFlushRate:    850_000,
	}
	tb.OnHeartbeat(hb)
	if tb.Stats().Holds != 1 {
		t.Fatal("E == 0.85*S should trigger hold")
	}

	// Reset
	tb = NewTokenBucket(1_000_000, cc)

	// Just below hold threshold: E = 0.849 * S -> should decrease
	hb = &protocol.HeartbeatPayload{
		NetworkDeliveryRate: 849_000,
		StorageFlushRate:    849_000,
	}
	tb.OnHeartbeat(hb)
	if tb.Stats().Decreases != 1 {
		t.Fatal("E = 0.849*S should trigger decrease")
	}
}

func TestResetByteCounter(t *testing.T) {
	tb := NewTokenBucket(1_000_000_000, defaultCC()) // very fast, no pacing delay
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
	// At 1 MB/s, sending 1000 bytes should take ~1ms
	tb := NewTokenBucket(1_000_000, defaultCC())

	// Prime lastSend
	tb.Pace(100)

	start := time.Now()
	tb.Pace(1000) // should delay ~1ms
	elapsed := time.Since(start)

	// Allow generous bounds for CI environments (500µs to 10ms)
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

	// Decrease: E = 500K, below 0.70*2M=1.4M -> rate = 500K * 1.10 = 550K
	tb.OnHeartbeat(&protocol.HeartbeatPayload{
		NetworkDeliveryRate: 500_000,
		StorageFlushRate:    500_000,
	})
	expected := 500_000 * 1.10
	if tb.Rate() != expected {
		t.Fatalf("custom 1.10 headroom: rate = %f, want %f", tb.Rate(), expected)
	}
}
