package protocol

import "time"

// --- Packet Types ---

type PacketType uint8

const (
	PacketSessionReq       PacketType = 0x00
	PacketData             PacketType = 0x01
	PacketParity           PacketType = 0x02
	PacketHeartbeat        PacketType = 0x03
	PacketSessionReject    PacketType = 0x04
	PacketTransferComplete PacketType = 0x05
	PacketACKClose         PacketType = 0x06
)

func (p PacketType) String() string {
	switch p {
	case PacketSessionReq:
		return "SESSION_REQ"
	case PacketData:
		return "DATA"
	case PacketParity:
		return "PARITY"
	case PacketHeartbeat:
		return "HEARTBEAT"
	case PacketSessionReject:
		return "SESSION_REJECT"
	case PacketTransferComplete:
		return "TRANSFER_COMPLETE"
	case PacketACKClose:
		return "ACK_CLOSE"
	default:
		return "UNKNOWN"
	}
}

// --- Flags ---

type Flag uint8

const (
	FlagEndOfFile        Flag = 0x01
	FlagCalibrationBurst Flag = 0x02
)

// --- Session Reject Reason Codes ---

type RejectReason uint8

const (
	RejectSessionIDCollision RejectReason = 0x01
	RejectHashMismatch       RejectReason = 0x02
	RejectServerBusy         RejectReason = 0x03
)

func (r RejectReason) String() string {
	switch r {
	case RejectSessionIDCollision:
		return "SESSION_ID_COLLISION"
	case RejectHashMismatch:
		return "HASH_MISMATCH"
	case RejectServerBusy:
		return "SERVER_BUSY"
	default:
		return "UNKNOWN"
	}
}

// --- Wire Format Constants ---

const (
	HeaderSize = 24                      // bytes, 3 x 64-bit aligned
	MTUHardCap = 1400                    // bytes, total packet size
	MaxPayload = MTUHardCap - HeaderSize // 1376 bytes
)

// --- Header ---

// Header is the 24-byte fixed-width binary header for every HP-UDP datagram.
//
// Wire layout:
//   Offset  Size  Field
//   0x00    1     PacketType
//   0x01    4     SessionID
//   0x05    8     SequenceNum
//   0x0D    8     BlockGroup
//   0x15    2     PayloadLen
//   0x17    1     Flags
type Header struct {
	Type        PacketType
	SessionID   uint32
	SequenceNum uint64
	BlockGroup  uint64
	PayloadLen  uint16
	Flags       Flag
}

// Packet is a fully parsed datagram: header + payload bytes.
type Packet struct {
	Header  Header
	Payload []byte
}

// --- Session Request Payload ---

// SessionReqPayload is the structured payload carried inside a SESSION_REQ packet.
type SessionReqPayload struct {
	FileSize    uint64 // total file size in bytes
	Checksum    uint64 // xxHash64 of the complete file
	InitialRate uint32 // bytes/sec, 0 = use calibration mode
	FileName    string // null-terminated on the wire
}

// --- Heartbeat Payload ---

// HeartbeatPayload is the structured payload carried inside a HEARTBEAT packet.
type HeartbeatPayload struct {
	NetworkDeliveryRate uint32   // bytes/sec into ring buffer
	StorageFlushRate    uint32   // bytes/sec flushed to disk
	LossRate            uint16   // basis points (150 = 1.50%)
	HighestContiguous   uint64   // highest seqnum with all 0..N received
	NACKCount           uint16   // number of entries in NACKs
	EchoTimestampNs     uint64   // sender's last-sent-data Unix nanosecond timestamp, echoed for RTT measurement
	DispersionNs        uint64   // calibration burst dispersion: (last_cal_arrival − first_cal_arrival) in ns
	NACKs               []uint64 // unrecoverable sequence numbers
}

// --- FEC Configuration ---

// FECConfig holds the adaptive FEC parameters for a transfer session.
type FECConfig struct {
	BlockSize        int     // data packets per block group (default 100)
	InitialParityPct float64 // starting parity ratio (default 0.05)
	TailMinParity    int     // minimum parity packets for the tail block (default 2)
}

// DefaultFECConfig returns the spec defaults from Appendix A.
func DefaultFECConfig() FECConfig {
	return FECConfig{
		BlockSize:        100,
		InitialParityPct: 0.05,
		TailMinParity:    2,
	}
}

// ParityCount returns the number of parity packets to generate for a block
// of dataCount packets given the observed loss rate in basis points.
func (f FECConfig) ParityCount(dataCount int, lossBasisPoints uint16) int {
	var ratio float64
	switch {
	case lossBasisPoints < 50: // < 0.5%
		ratio = 0.02
	case lossBasisPoints < 200: // 0.5% - 2%
		ratio = 0.05
	case lossBasisPoints < 500: // 2% - 5%
		ratio = 0.10
	case lossBasisPoints < 1000: // 5% - 10%
		ratio = 0.15
	default: // > 10%
		ratio = 0.20
	}

	count := int(float64(dataCount) * ratio)
	if count < f.TailMinParity {
		count = f.TailMinParity
	}
	return count
}

// --- Congestion Control Configuration ---

// CongestionConfig holds the tunable parameters for the rate adjustment algorithm.
type CongestionConfig struct {
	// Phase1Multiplier is the multiplicative increase factor applied once per RTT
	// during Phase 1 (loss < 1%, link ceiling not yet found). Default: 1.25.
	Phase1Multiplier float64

	// DecreaseFrac is the fraction of the EWMA-smoothed effective delivery rate
	// the sender targets on confirmed congestion (loss > 5% for two consecutive
	// heartbeats). Values < 1.0 undershoot the measured ceiling so router queues
	// can drain. Default: 0.85.
	DecreaseFrac float64
}

// DefaultCongestionConfig returns the spec defaults from Appendix A.
func DefaultCongestionConfig() CongestionConfig {
	return CongestionConfig{
		Phase1Multiplier: 1.25,
		DecreaseFrac:     0.85,
	}
}

// --- Calibration Configuration ---

// CalibrationConfig holds parameters for the initial calibration burst.
type CalibrationConfig struct {
	BurstSize    int           // number of packets in the burst (default 50)
	BurstSpacing time.Duration // interval between burst packets (default 1ms)
}

// DefaultCalibrationConfig returns the spec defaults from Appendix A.
func DefaultCalibrationConfig() CalibrationConfig {
	return CalibrationConfig{
		BurstSize:    10, // 10 packets at wire speed: enough to measure dispersion without flooding
		BurstSpacing: 0,  // 0 = send at wire speed to probe actual link capacity
	}
}

// --- Session / Timeout Configuration ---

// SessionConfig holds session lifecycle and timeout parameters.
type SessionConfig struct {
	InactivityMultiplier    int           // timeout = multiplier * heartbeat interval (default 5)
	SenderProbeInterval     time.Duration // interval between probe packets (default 500ms)
	SenderProbeTimeout      time.Duration // total time in probe state before teardown (default 10s)
	LingerDuration          time.Duration // post-transfer linger on both sides (default 3s)
	ReceiverTeardownRetries int           // TRANSFER_COMPLETE retransmit count (default 3)
	StaleIDReservation      time.Duration // how long purged SessionIDs stay reserved (default 10s)
}

// DefaultSessionConfig returns the spec defaults from Appendix A.
func DefaultSessionConfig() SessionConfig {
	return SessionConfig{
		InactivityMultiplier:    5,
		SenderProbeInterval:     500 * time.Millisecond,
		SenderProbeTimeout:      10 * time.Second,
		LingerDuration:          3 * time.Second,
		ReceiverTeardownRetries: 3,
		StaleIDReservation:      10 * time.Second,
	}
}

// --- Heartbeat Interval Tiers ---

// HeartbeatInterval returns the appropriate heartbeat interval for the given
// effective send rate in bytes per second, per the spec §6A table.
func HeartbeatInterval(sendRateBytesPerSec uint64) time.Duration {
	switch {
	case sendRateBytesPerSec < 10_000_000: // < 10 MB/s
		return 100 * time.Millisecond
	case sendRateBytesPerSec < 100_000_000: // 10 - 100 MB/s
		return 50 * time.Millisecond
	case sendRateBytesPerSec < 1_000_000_000: // 100 MB/s - 1 GB/s
		return 25 * time.Millisecond
	default: // > 1 GB/s
		return 10 * time.Millisecond
	}
}
