package sender

import (
	"hpuft/protocol"
	"testing"
)

func TestCalibrationSkippedWithInitialRate(t *testing.T) {
	cfg := protocol.DefaultCalibrationConfig()
	cs := NewCalibrationState(cfg, 5_000_000) // explicit rate

	if cs.IsActive() {
		t.Fatal("calibration should be inactive when InitialRate is set")
	}

	// Flags should be empty
	if cs.Flags() != 0 {
		t.Fatalf("Flags = %d, want 0", cs.Flags())
	}

	// Pace should return false (not handled)
	if cs.Pace() {
		t.Fatal("Pace should return false when inactive")
	}
}

func TestCalibrationBurstLifecycle(t *testing.T) {
	cfg := protocol.CalibrationConfig{
		BurstSize:    5,
		BurstSpacing: 0, // no actual sleep in test
	}
	cs := NewCalibrationState(cfg, 0)

	if !cs.IsActive() {
		t.Fatal("calibration should be active initially")
	}

	if cs.Flags() != protocol.FlagCalibrationBurst {
		t.Fatalf("Flags = %d, want %d", cs.Flags(), protocol.FlagCalibrationBurst)
	}

	// Send 4 packets — still active
	for i := 0; i < 4; i++ {
		cs.PacketSent()
		if !cs.IsActive() {
			t.Fatalf("should be active after %d/%d packets", i+1, 5)
		}
	}

	// Send 5th packet — should deactivate
	cs.PacketSent()
	if cs.IsActive() {
		t.Fatal("should be inactive after burst complete")
	}

	if cs.Flags() != 0 {
		t.Fatal("Flags should be 0 after burst complete")
	}
}

func TestCalibrationEndedByHeartbeat(t *testing.T) {
	cfg := protocol.CalibrationConfig{
		BurstSize:    50,
		BurstSpacing: 0,
	}
	cs := NewCalibrationState(cfg, 0)

	// Send 10 packets
	for i := 0; i < 10; i++ {
		cs.PacketSent()
	}

	if !cs.IsActive() {
		t.Fatal("should still be active (only 10/50 packets)")
	}

	// Heartbeat arrives — should end calibration
	cs.OnHeartbeat()

	if cs.IsActive() {
		t.Fatal("should be inactive after heartbeat")
	}

	// Second heartbeat should be a no-op
	cs.OnHeartbeat()
}

func TestCalibrationPacketSentAfterInactive(t *testing.T) {
	cfg := protocol.CalibrationConfig{
		BurstSize:    3,
		BurstSpacing: 0,
	}
	cs := NewCalibrationState(cfg, 0)

	for i := 0; i < 3; i++ {
		cs.PacketSent()
	}

	// Already inactive — PacketSent should be safe to call
	cs.PacketSent()
	cs.PacketSent()
	// No panic = pass
}

func TestStartingRate(t *testing.T) {
	chunkSize := protocol.MaxPayload

	// With explicit rate — always honoured regardless of spacing
	cfg := protocol.DefaultCalibrationConfig()
	rate := StartingRate(cfg, 10_000_000, chunkSize)
	if rate != 10_000_000 {
		t.Fatalf("explicit rate: got %f, want 10000000", rate)
	}

	// Wire-speed probe mode (BurstSpacing=0) — conservative 2 MB/s to avoid flooding WAN/satellite
	rate = StartingRate(cfg, 0, chunkSize) // default has BurstSpacing=0
	if rate != 2_000_000 {
		t.Fatalf("probe mode starting rate: got %f, want 2000000", rate)
	}

	// Fixed-spacing mode — rate = chunkSize / spacing
	fixedCfg := protocol.CalibrationConfig{BurstSize: 50, BurstSpacing: 1_000_000} // 1ms
	rate = StartingRate(fixedCfg, 0, chunkSize)
	expected := float64(chunkSize) / fixedCfg.BurstSpacing.Seconds()
	if rate != expected {
		t.Fatalf("fixed-spacing rate: got %f, want %f", rate, expected)
	}
}
