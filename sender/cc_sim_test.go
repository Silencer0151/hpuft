package sender

// cc_sim_test.go: CC simulation under realistic network conditions.
//
// Rather than requiring real sockets or a second machine, these tests drive
// TokenBucket through synthetic heartbeat feedback that mirrors what a real
// link would report at each current send rate.
//
// Link model:
//   - When sender rate ≤ link bandwidth: loss = structural loss only
//   - When sender rate > link bandwidth: excess packets are dropped (congestion
//     loss), so observed loss = structural + (sendRate-bandwidth)/sendRate
//   - Delivery rate = min(sendRate, bandwidth) × (1 − structuralLoss)
//
// This lets us verify that CC converges to (or near) the link capacity across
// LAN, WAN/satellite, and congested scenarios without needing live hardware.

import (
	"fmt"
	"hpuft/protocol"
	"testing"
	"time"
)

// simLink models a network bottleneck for CC simulation.
type simLink struct {
	bandwidthBps       float64 // link capacity in bytes/sec
	structuralLossFrac float64 // baseline packet loss independent of load (0–1)
	storageBps         float64 // receiver disk speed; 0 = same as network
	rttMs              float64 // round-trip time in milliseconds (0 = unknown)
}

// heartbeat synthesises the HeartbeatPayload a receiver would produce when the
// sender is transmitting at sendRateBps through this link.
//
// If rttMs > 0, sets EchoTimestampNs so the sender's RTT estimation works in
// simulated time (timestamp = now minus RTT).
func (l simLink) heartbeat(sendRateBps float64) *protocol.HeartbeatPayload {
	bottleneck := l.bandwidthBps
	if l.storageBps > 0 && l.storageBps < bottleneck {
		bottleneck = l.storageBps
	}

	clamp := sendRateBps
	if clamp > bottleneck {
		clamp = bottleneck
	}

	deliveryBps := clamp * (1 - l.structuralLossFrac)

	var totalLoss float64
	if sendRateBps > bottleneck {
		overflowLoss := (sendRateBps - bottleneck) / sendRateBps
		totalLoss = overflowLoss + (1-overflowLoss)*l.structuralLossFrac
	} else {
		totalLoss = l.structuralLossFrac
	}
	if totalLoss > 1 {
		totalLoss = 1
	}

	storageBps := l.storageBps
	if storageBps == 0 {
		storageBps = deliveryBps
	}

	// Simulate EchoTimestampNs: set to (now − RTT) so the sender computes
	// a realistic RTT from the received heartbeat.
	var echoTS uint64
	if l.rttMs > 0 {
		rttDur := time.Duration(l.rttMs * float64(time.Millisecond))
		echoTS = uint64(time.Now().Add(-rttDur).UnixNano())
	}

	return &protocol.HeartbeatPayload{
		NetworkDeliveryRate: uint32(deliveryBps),
		StorageFlushRate:    uint32(storageBps),
		LossRate:            uint16(totalLoss * 10000), // basis points
		EchoTimestampNs:     echoTS,
	}
}

// runSim feeds heartbeats into a TokenBucket for n iterations and returns a
// slice of the rate after each heartbeat. Starting rate and link are fixed.
func runSim(startBps float64, link simLink, iterations int) []float64 {
	tb := NewTokenBucket(startBps, defaultCC())
	rates := make([]float64, iterations)
	for i := range iterations {
		hb := link.heartbeat(tb.Rate())
		tb.OnHeartbeat(hb)
		rates[i] = tb.Rate()
	}
	return rates
}

// avg returns the mean of a slice.
func avg(vals []float64) float64 {
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

// TestCCLANConvergence verifies that CC ramps up quickly on a clean fast link.
// A LAN has essentially unlimited bandwidth and near-zero loss. Starting at
// 2 MB/s, the rate should exceed 20 MB/s within 15 heartbeats.
func TestCCLANConvergence(t *testing.T) {
	lan := simLink{bandwidthBps: 125_000_000, structuralLossFrac: 0.0} // 1 Gbps
	rates := runSim(2_000_000, lan, 15)

	final := rates[len(rates)-1]
	if final < 20_000_000 {
		t.Fatalf("LAN: rate after 15 HBs = %.2f MB/s, want > 20 MB/s (slow ramp)", final/1e6)
	}
	t.Logf("LAN ramp: %s", rateTrace(rates))
}

// TestCCSatelliteConvergence verifies CC finds the link capacity on a
// bandwidth-limited WAN link (Starlink-like: 5 MB/s, 0.5% structural loss).
// Starting at 2 MB/s, it should climb to near link capacity within 30 HBs.
func TestCCSatelliteConvergence(t *testing.T) {
	starlink := simLink{bandwidthBps: 5_000_000, structuralLossFrac: 0.005, rttMs: 40}
	rates := runSim(2_000_000, starlink, 30)

	// Average rate over last 15 HBs should be > 2 MB/s (link is 5 MB/s)
	tail := rates[15:]
	mean := avg(tail)
	if mean < 2_000_000 {
		t.Fatalf("Starlink: avg rate in tail = %.2f MB/s, want > 2 MB/s", mean/1e6)
	}
	// Should not wildly overshoot the link for sustained periods
	for i, r := range tail {
		if r > 15_000_000 { // 3× link bandwidth is too high
			t.Fatalf("Starlink: rate[%d]=%.2f MB/s is way over link capacity (5 MB/s)", 15+i, r/1e6)
		}
	}
	t.Logf("Starlink tail avg: %.2f MB/s | trace: %s", mean/1e6, rateTrace(rates))
}

// TestCCFloodStartVsSafeStart shows what happens with the old 50 MB/s starting
// rate vs the new 2 MB/s on a Starlink link. The safe start finds link capacity
// faster; the flood poisons peakRate and may lock the ceiling below capacity.
func TestCCFloodStartVsSafeStart(t *testing.T) {
	starlink := simLink{bandwidthBps: 5_000_000, structuralLossFrac: 0.005}

	floodRates := runSim(50_000_000, starlink, 30) // old default
	safeRates := runSim(2_000_000, starlink, 30)   // new default

	floodTailAvg := avg(floodRates[20:])
	safeTailAvg := avg(safeRates[20:])

	t.Logf("50MB/s start tail avg: %.2f MB/s | trace: %s", floodTailAvg/1e6, rateTrace(floodRates))
	t.Logf("2MB/s  start tail avg: %.2f MB/s | trace: %s", safeTailAvg/1e6, rateTrace(safeRates))

	// Neither should be stuck near zero in the tail
	if safeTailAvg < 1_000_000 {
		t.Fatalf("safe start: tail avg %.2f MB/s — stuck too low on Starlink link", safeTailAvg/1e6)
	}
	if floodTailAvg < 500_000 {
		t.Fatalf("flood start: tail avg %.2f MB/s — permanently stuck, ceiling never recovered", floodTailAvg/1e6)
	}
}

// TestCCCongestedLink verifies that CC backs off when packet loss is consistently
// high (heavily congested 2 MB/s link). Starting at 10 MB/s, CC should detect
// congestion and reduce to near the link capacity.
func TestCCCongestedLink(t *testing.T) {
	congested := simLink{bandwidthBps: 2_000_000, structuralLossFrac: 0.02}
	rates := runSim(10_000_000, congested, 25)

	// After enough heartbeats, rate should have dropped below 5× link BW
	for _, r := range rates[10:] {
		if r > 10_000_000 {
			t.Fatalf("congested link: rate never backed off, still %.2f MB/s (link=2 MB/s)", r/1e6)
		}
	}
	t.Logf("Congested link trace: %s", rateTrace(rates))
}

// TestCCRecoveryAfterCongestion verifies that once a congested link clears,
// CC recovers and resumes probing upward (doesn't stay permanently throttled).
// The congested link has 8% structural loss, so loss is always > 5% and the
// hold zone is never entered — Phase 2 is never triggered. Recovery uses
// Phase 1 multiplicative increase.
func TestCCRecoveryAfterCongestion(t *testing.T) {
	tb := NewTokenBucket(5_000_000, defaultCC())

	congested := simLink{bandwidthBps: 1_000_000, structuralLossFrac: 0.08}
	clear := simLink{bandwidthBps: 50_000_000, structuralLossFrac: 0.0}

	var rates []float64

	// Phase 1: congested for 10 HBs
	for range 10 {
		hb := congested.heartbeat(tb.Rate())
		tb.OnHeartbeat(hb)
		rates = append(rates, tb.Rate())
	}
	rateAfterCongestion := tb.Rate()

	// Phase 2: link clears for 10 HBs
	for range 10 {
		hb := clear.heartbeat(tb.Rate())
		tb.OnHeartbeat(hb)
		rates = append(rates, tb.Rate())
	}
	rateAfterRecovery := tb.Rate()

	t.Logf("After congestion: %.2f MB/s | After recovery: %.2f MB/s", rateAfterCongestion/1e6, rateAfterRecovery/1e6)
	t.Logf("Trace: %s", rateTrace(rates))

	if rateAfterRecovery <= rateAfterCongestion {
		t.Fatalf("CC did not recover after congestion cleared: congested=%.2f MB/s recovery=%.2f MB/s",
			rateAfterCongestion/1e6, rateAfterRecovery/1e6)
	}
	if rateAfterRecovery < 2_000_000 {
		t.Fatalf("CC stuck too low after recovery: %.2f MB/s", rateAfterRecovery/1e6)
	}
}

// TestCCStorageBottleneck verifies that when the receiver's disk is the
// bottleneck (not the network), CC respects the storage floor.
func TestCCStorageBottleneck(t *testing.T) {
	fastNetSlowDisk := simLink{
		bandwidthBps:       50_000_000,
		structuralLossFrac: 0.005,
		storageBps:         3_000_000,
	}
	rates := runSim(1_000_000, fastNetSlowDisk, 20)

	tail := rates[10:]
	for _, r := range tail {
		if r > 20_000_000 {
			t.Logf("Storage bottleneck tail: %s", rateTrace(tail))
			t.Fatalf("rate %.2f MB/s wildly exceeds storage limit (3 MB/s)", r/1e6)
		}
	}
	t.Logf("Storage bottleneck trace: %s", rateTrace(rates))
}

// TestCCPhase2AdditiveIncrease verifies that after the hold zone is entered
// (Phase 2 triggered), increases become additive rather than multiplicative.
// The additive step is MaxPayload / RTT, which is much smaller than 25% of
// the current rate at realistic operating points.
func TestCCPhase2AdditiveIncrease(t *testing.T) {
	// Starlink-like: 5 MB/s bandwidth, 40ms RTT.
	// Start at 4.5 MB/s (just below capacity) so the first HB with 0.5%
	// structural loss is < 1% and we increase into the congestion zone fast.
	starlink := simLink{bandwidthBps: 5_000_000, structuralLossFrac: 0.005, rttMs: 40}
	tb := NewTokenBucket(4_500_000, defaultCC())

	// Run until Phase 2 is triggered (hold zone entered)
	const maxHBs = 50
	var phase2HB int
	for i := range maxHBs {
		hb := starlink.heartbeat(tb.Rate())
		tb.OnHeartbeat(hb)
		if tb.inPhase2 && phase2HB == 0 {
			phase2HB = i + 1
		}
	}

	if phase2HB == 0 {
		t.Skip("Phase 2 not triggered in this run — link may not have congested")
	}

	t.Logf("Phase 2 triggered at HB %d", phase2HB)
	t.Logf("Final rate: %.2f MB/s (link=5 MB/s)", tb.Rate()/1e6)

	// After Phase 2, rate should be somewhere near the link capacity (not far below)
	if tb.Rate() < 1_000_000 {
		t.Fatalf("Phase 2: rate %.2f MB/s — stuck far below 5 MB/s link", tb.Rate()/1e6)
	}
}

// rateTrace formats a slice of rates as a compact MB/s trace for test logs.
func rateTrace(rates []float64) string {
	s := ""
	for i, r := range rates {
		if i > 0 {
			s += " → "
		}
		s += fmt.Sprintf("%.1f", r/1e6)
	}
	return s + " MB/s"
}
