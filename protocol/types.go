package protocol

import (
	"net"
	"time"
)

// RawPacket carries a wire packet plus the address it arrived from. Used by
// dispatchers (e.g. the connect REPL, the serve daemon) that own the socket
// and forward packets into a transfer goroutine: the receiver/sender needs
// the source address to lock its heartbeat destination onto the peer's
// data-plane ephemeral port (see receiver/receiver.go UpdatePeerAddr).
type RawPacket struct {
	Data []byte
	Src  *net.UDPAddr
}

// --- Packet Types ---

type PacketType uint8

const (
	// Connection layer
	PacketHello    PacketType = 0x00 // was PacketSessionReq (0x00)
	PacketWelcome  PacketType = 0x01 // was PacketSessionAccept (0x0A)
	PacketRequest  PacketType = 0x02 // NEW
	PacketResponse PacketType = 0x03 // NEW
	PacketReject   PacketType = 0x04 // was PacketSessionReject (0x04)

	// Data layer (renumbered from v5)
	PacketData      PacketType = 0x05 // was 0x01
	PacketParity    PacketType = 0x06 // was 0x02
	PacketHeartbeat PacketType = 0x07 // was 0x03
	PacketComplete  PacketType = 0x08 // was PacketTransferComplete (0x05)
	PacketAckClose  PacketType = 0x09 // was PacketACKClose (0x06)

	// Keepalive
	PacketPing PacketType = 0x0A // NEW
	PacketPong PacketType = 0x0B // NEW
)

func (p PacketType) String() string {
	switch p {
	case PacketHello:
		return "HELLO"
	case PacketWelcome:
		return "WELCOME"
	case PacketRequest:
		return "REQUEST"
	case PacketResponse:
		return "RESPONSE"
	case PacketReject:
		return "REJECT"
	case PacketData:
		return "DATA"
	case PacketParity:
		return "PARITY"
	case PacketHeartbeat:
		return "HEARTBEAT"
	case PacketComplete:
		return "COMPLETE"
	case PacketAckClose:
		return "ACK_CLOSE"
	case PacketPing:
		return "PING"
	case PacketPong:
		return "PONG"
	default:
		return "UNKNOWN"
	}
}

// --- Flags ---

type Flag uint8

const (
	FlagEndOfFile        Flag = 0x01
	FlagCalibrationBurst Flag = 0x02
	// FlagEncrypted removed — v6 is always encrypted
)

// --- Operation Codes (REQUEST payload byte 0) ---

type OpCode uint8

const (
	OpPut    OpCode = 0x01
	OpGet    OpCode = 0x02
	OpList   OpCode = 0x03
	OpDelete OpCode = 0x04
)

// --- Response Status ---

type ResponseStatus uint8

const (
	StatusOK    ResponseStatus = 0x00
	StatusError ResponseStatus = 0x01
)

// --- Reason Codes ---

type Reason uint8

const (
	ReasonConnectionIDCollision Reason = 0x01 // was RejectSessionIDCollision
	ReasonHashMismatch          Reason = 0x02
	ReasonServerBusy            Reason = 0x03
	ReasonFileNotFound          Reason = 0x04
	ReasonFileExists            Reason = 0x05
	ReasonClientDisconnect      Reason = 0x06 // was 0x08 in v5
	ReasonInvalidRequest        Reason = 0x07
	ReasonDeleteDenied          Reason = 0x08
)

func (r Reason) String() string {
	switch r {
	case ReasonConnectionIDCollision:
		return "CONNECTION_ID_COLLISION"
	case ReasonHashMismatch:
		return "HASH_MISMATCH"
	case ReasonServerBusy:
		return "SERVER_BUSY"
	case ReasonFileNotFound:
		return "FILE_NOT_FOUND"
	case ReasonFileExists:
		return "FILE_EXISTS"
	case ReasonClientDisconnect:
		return "CLIENT_DISCONNECT"
	case ReasonInvalidRequest:
		return "INVALID_REQUEST"
	case ReasonDeleteDenied:
		return "DELETE_DENIED"
	default:
		return "UNKNOWN"
	}
}

// --- Wire Format Constants ---

const (
	HeaderSize = 32                                   // bytes, 4 x 64-bit aligned
	MTUHardCap = 1400                                 // bytes, total packet size
	MaxPayload = MTUHardCap - HeaderSize - GCMTagSize // 1352 bytes; v6 always encrypts
)

// --- Header ---

// Header is the 32-byte fixed-width binary header for every HP-UDP datagram.
//
// Wire layout:
//
//	Offset  Size  Field
//	0x00    1     PacketType
//	0x01    4     ConnectionID
//	0x05    8     SequenceNum
//	0x0D    8     BlockGroup
//	0x15    2     PayloadLen
//	0x17    1     Flags
//	0x18    8     SenderTimestampNs
//
// SenderTimestampNs carries the sender's time.Now().UnixNano() at the moment
// a DATA or PARITY packet is built. The receiver echoes this value back in
// EchoTimestampNs so the sender can compute RTT = now - SenderTimestampNs
// using only its own clock, avoiding cross-machine clock-skew errors.
// Non-data packets leave this field zero.
type Header struct {
	Type              PacketType
	ConnectionID      uint32 // was SessionID
	SequenceNum       uint64
	BlockGroup        uint64
	PayloadLen        uint16
	Flags             Flag
	SenderTimestampNs uint64
}

// Packet is a fully parsed datagram: header + payload bytes.
type Packet struct {
	Header  Header
	Payload []byte
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
	SenderHeartbeatTimeout  time.Duration // max silence from receiver during data phase before abort (default 3s)
	LingerDuration          time.Duration // post-transfer linger on both sides (default 3s)
	ReceiverTeardownRetries int           // TRANSFER_COMPLETE retransmit count (default 3)
	ConnectionIDReservation time.Duration // how long torn-down ConnectionIDs stay reserved (default 10s)
}

// DefaultSessionConfig returns the spec defaults from Appendix A.
func DefaultSessionConfig() SessionConfig {
	return SessionConfig{
		InactivityMultiplier:    5,
		SenderProbeInterval:     500 * time.Millisecond,
		SenderProbeTimeout:      10 * time.Second,
		SenderHeartbeatTimeout:  3 * time.Second,
		LingerDuration:          3 * time.Second,
		ReceiverTeardownRetries: 3,
		ConnectionIDReservation: 10 * time.Second,
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
