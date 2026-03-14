package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var (
	ErrPayloadTooShort = errors.New("payload too short for expected structure")
)

// --- SESSION_REQ Payload ---
//
// Wire layout:
//   Offset  Size      Field
//   0       8 bytes   FileSize
//   8       8 bytes   Checksum (xxHash64 / placeholder)
//   16      4 bytes   InitialRate (0 = calibration mode)
//   20      variable  FileName (null-terminated)

const SessionReqFixedSize = 20 // bytes before the filename

// MarshalSessionReq serializes a SessionReqPayload into bytes.
func MarshalSessionReq(p *SessionReqPayload) []byte {
	nameBytes := []byte(p.FileName)
	buf := make([]byte, SessionReqFixedSize+len(nameBytes)+1) // +1 for null terminator

	binary.BigEndian.PutUint64(buf[0:8], p.FileSize)
	binary.BigEndian.PutUint64(buf[8:16], p.Checksum)
	binary.BigEndian.PutUint32(buf[16:20], p.InitialRate)
	copy(buf[20:], nameBytes)
	buf[len(buf)-1] = 0 // null terminator

	return buf
}

// UnmarshalSessionReq parses a SessionReqPayload from bytes.
func UnmarshalSessionReq(data []byte) (SessionReqPayload, error) {
	if len(data) < SessionReqFixedSize+1 {
		return SessionReqPayload{}, fmt.Errorf("%w: need at least %d bytes, got %d",
			ErrPayloadTooShort, SessionReqFixedSize+1, len(data))
	}

	p := SessionReqPayload{
		FileSize:    binary.BigEndian.Uint64(data[0:8]),
		Checksum:    binary.BigEndian.Uint64(data[8:16]),
		InitialRate: binary.BigEndian.Uint32(data[16:20]),
	}

	// Find null terminator for filename
	nameData := data[SessionReqFixedSize:]
	for i, b := range nameData {
		if b == 0 {
			p.FileName = string(nameData[:i])
			return p, nil
		}
	}

	// No null terminator found — use all remaining bytes
	p.FileName = string(nameData)
	return p, nil
}

// --- HEARTBEAT Payload ---
//
// Wire layout:
//   Offset  Size      Field
//   0       4 bytes   NetworkDeliveryRate
//   4       4 bytes   StorageFlushRate
//   8       2 bytes   LossRate (basis points)
//   10      8 bytes   HighestContiguous
//   18      2 bytes   NACKCount
//   20      8 bytes   EchoTimestampNs  (sender's last-data-send time, echoed for RTT)
//   28      8 bytes   DispersionNs     (calibration burst arrival spread in nanoseconds)
//   36      8*N bytes NACKArray

const HeartbeatFixedSize = 36 // bytes before the NACK array

// MarshalHeartbeat serializes a HeartbeatPayload into bytes.
func MarshalHeartbeat(p *HeartbeatPayload) []byte {
	buf := make([]byte, HeartbeatFixedSize+8*len(p.NACKs))

	binary.BigEndian.PutUint32(buf[0:4], p.NetworkDeliveryRate)
	binary.BigEndian.PutUint32(buf[4:8], p.StorageFlushRate)
	binary.BigEndian.PutUint16(buf[8:10], p.LossRate)
	binary.BigEndian.PutUint64(buf[10:18], p.HighestContiguous)
	binary.BigEndian.PutUint16(buf[18:20], uint16(len(p.NACKs)))
	binary.BigEndian.PutUint64(buf[20:28], p.EchoTimestampNs)
	binary.BigEndian.PutUint64(buf[28:36], p.DispersionNs)

	for i, seq := range p.NACKs {
		binary.BigEndian.PutUint64(buf[36+8*i:44+8*i], seq)
	}

	return buf
}

// UnmarshalHeartbeat parses a HeartbeatPayload from bytes.
func UnmarshalHeartbeat(data []byte) (HeartbeatPayload, error) {
	if len(data) < HeartbeatFixedSize {
		return HeartbeatPayload{}, fmt.Errorf("%w: need at least %d bytes, got %d",
			ErrPayloadTooShort, HeartbeatFixedSize, len(data))
	}

	p := HeartbeatPayload{
		NetworkDeliveryRate: binary.BigEndian.Uint32(data[0:4]),
		StorageFlushRate:    binary.BigEndian.Uint32(data[4:8]),
		LossRate:            binary.BigEndian.Uint16(data[8:10]),
		HighestContiguous:   binary.BigEndian.Uint64(data[10:18]),
		NACKCount:           binary.BigEndian.Uint16(data[18:20]),
		EchoTimestampNs:     binary.BigEndian.Uint64(data[20:28]),
		DispersionNs:        binary.BigEndian.Uint64(data[28:36]),
	}

	nackCount := int(p.NACKCount)
	expectedLen := HeartbeatFixedSize + 8*nackCount
	if len(data) < expectedLen {
		return HeartbeatPayload{}, fmt.Errorf("%w: need %d bytes for %d NACKs, got %d",
			ErrPayloadTooShort, expectedLen, nackCount, len(data))
	}

	if nackCount > 0 {
		p.NACKs = make([]uint64, nackCount)
		for i := 0; i < nackCount; i++ {
			p.NACKs[i] = binary.BigEndian.Uint64(data[36+8*i : 44+8*i])
		}
	}

	return p, nil
}
