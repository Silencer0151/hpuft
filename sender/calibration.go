package sender

import (
	"hpuft/protocol"
	"log"
	"sync"
	"time"
)

// CalibrationState tracks whether we're in the calibration burst phase
// and controls pacing and flag behavior during the burst.
type CalibrationState struct {
	mu          sync.Mutex
	active      bool // true during the burst phase
	burstSize   int
	spacing     time.Duration
	packetsSent int

	// Set when the first heartbeat arrives, ending calibration early
	heartbeatReceived bool
}

// NewCalibrationState creates a calibration tracker. If initialRate is non-zero,
// calibration is skipped (returns an inactive state).
func NewCalibrationState(cfg protocol.CalibrationConfig, initialRate uint32) *CalibrationState {
	if initialRate > 0 {
		log.Printf("[calibration] skipped — using configured initial rate: %.2f MB/s",
			float64(initialRate)/1e6)
		return &CalibrationState{active: false}
	}

	if cfg.BurstSpacing == 0 {
		log.Printf("[calibration] burst: %d packets at wire speed (link-capacity probe)",
			cfg.BurstSize)
	} else {
		log.Printf("[calibration] burst: %d packets at %v spacing (%.2f MB/s probe rate)",
			cfg.BurstSize, cfg.BurstSpacing,
			float64(protocol.MaxPayload)/cfg.BurstSpacing.Seconds()/1e6)
	}

	return &CalibrationState{
		active:    true,
		burstSize: cfg.BurstSize,
		spacing:   cfg.BurstSpacing,
	}
}

// IsActive returns whether the calibration burst is still in progress.
func (cs *CalibrationState) IsActive() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.active
}

// Flags returns the flag value for the current packet.
// During calibration, this includes FlagCalibrationBurst.
func (cs *CalibrationState) Flags() protocol.Flag {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.active {
		return protocol.FlagCalibrationBurst
	}
	return 0
}

// PacketSent records that a packet was sent during calibration.
// If the burst is complete, it transitions to inactive.
func (cs *CalibrationState) PacketSent() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if !cs.active {
		return
	}
	cs.packetsSent++
	if cs.packetsSent >= cs.burstSize {
		cs.active = false
		log.Printf("[calibration] burst complete (%d packets sent), switching to adaptive pacing",
			cs.packetsSent)
	}
}

// OnHeartbeat marks that a heartbeat was received, ending calibration early.
// The first heartbeat gives us real metrics, so no reason to continue the burst.
func (cs *CalibrationState) OnHeartbeat() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if !cs.active || cs.heartbeatReceived {
		return
	}
	cs.heartbeatReceived = true
	cs.active = false
	log.Printf("[calibration] ended early by heartbeat after %d/%d burst packets",
		cs.packetsSent, cs.burstSize)
}

// Pace applies the appropriate inter-packet delay for the current phase.
// During calibration, uses fixed BurstSpacing (or none if zero) and returns true.
// After calibration, returns false (caller should use token bucket).
func (cs *CalibrationState) Pace() bool {
	cs.mu.Lock()
	active := cs.active
	spacing := cs.spacing
	cs.mu.Unlock()

	if !active {
		return false
	}
	if spacing > 0 {
		time.Sleep(spacing)
	}
	return true
}

// StartingRate returns the initial rate in bytes/sec for the token bucket.
// If an explicit InitialRate was configured, returns that.
// If BurstSpacing is zero (wire-speed probe mode), returns a high initial rate
// so the token bucket does not artificially throttle while waiting for the
// first heartbeat — congestion control will bring it down if needed.
// Otherwise returns the configured probe rate.
func StartingRate(cfg protocol.CalibrationConfig, initialRate uint32, chunkSize int) float64 {
	if initialRate > 0 {
		return float64(initialRate)
	}
	if cfg.BurstSpacing == 0 {
		return 50_000_000 // 50 MB/s: permissive start, CC adjusts on first heartbeat
	}
	return float64(chunkSize) / cfg.BurstSpacing.Seconds()
}
