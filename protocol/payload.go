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
// Wire layout (unencrypted):
//   Offset  Size      Field
//   0       8 bytes   FileSize
//   8       8 bytes   Checksum (xxHash64 / placeholder)
//   16      4 bytes   InitialRate (0 = calibration mode)
//   20      variable  FileName (null-terminated)
//
// Wire layout (encrypted, Encrypted=true):
//   Offset  Size      Field
//   0       8 bytes   FileSize
//   8       8 bytes   Checksum
//   16      4 bytes   InitialRate
//   20      32 bytes  PubKey (X25519 ephemeral public key)
//   52      variable  FileName (null-terminated)

const (
	SessionReqFixedSize    = 20 // bytes before optional PubKey and filename
	SessionReqEncFixedSize = 52 // SessionReqFixedSize + PubKeySize
)

// MarshalSessionReq serializes a SessionReqPayload into bytes.
func MarshalSessionReq(p *SessionReqPayload) []byte {
	nameBytes := []byte(p.FileName)
	var buf []byte
	if p.Encrypted {
		buf = make([]byte, SessionReqEncFixedSize+len(nameBytes)+1)
		binary.BigEndian.PutUint64(buf[0:8], p.FileSize)
		binary.BigEndian.PutUint64(buf[8:16], p.Checksum)
		binary.BigEndian.PutUint32(buf[16:20], p.InitialRate)
		copy(buf[20:52], p.PubKey[:])
		copy(buf[52:], nameBytes)
	} else {
		buf = make([]byte, SessionReqFixedSize+len(nameBytes)+1)
		binary.BigEndian.PutUint64(buf[0:8], p.FileSize)
		binary.BigEndian.PutUint64(buf[8:16], p.Checksum)
		binary.BigEndian.PutUint32(buf[16:20], p.InitialRate)
		copy(buf[20:], nameBytes)
	}
	buf[len(buf)-1] = 0
	return buf
}

// UnmarshalSessionReq parses a SessionReqPayload from bytes.
func UnmarshalSessionReq(data []byte, encrypted bool) (SessionReqPayload, error) {
	minSize := SessionReqFixedSize + 1
	if encrypted {
		minSize = SessionReqEncFixedSize + 1
	}
	if len(data) < minSize {
		return SessionReqPayload{}, fmt.Errorf("%w: need at least %d bytes, got %d",
			ErrPayloadTooShort, minSize, len(data))
	}

	p := SessionReqPayload{
		FileSize:    binary.BigEndian.Uint64(data[0:8]),
		Checksum:    binary.BigEndian.Uint64(data[8:16]),
		InitialRate: binary.BigEndian.Uint32(data[16:20]),
		Encrypted:   encrypted,
	}

	var nameData []byte
	if encrypted {
		copy(p.PubKey[:], data[20:52])
		nameData = data[52:]
	} else {
		nameData = data[20:]
	}

	for i, b := range nameData {
		if b == 0 {
			p.FileName = string(nameData[:i])
			return p, nil
		}
	}
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

// --- PULL_REQ Payload ---
//
// Wire layout (unencrypted):
//   Offset  Size      Field
//   0       variable  FileName (null-terminated)
//
// Wire layout (encrypted):
//   Offset  Size      Field
//   0       32 bytes  PubKey (X25519 ephemeral public key)
//   32      variable  FileName (null-terminated)

// PullReqPayload is the structured payload carried inside a PULL_REQ packet.
type PullReqPayload struct {
	PubKey    [32]byte // only on wire when Encrypted=true
	Encrypted bool
	FileName  string // file to request from the serve daemon
}

// MarshalPullReq serializes a PullReqPayload into bytes.
func MarshalPullReq(p *PullReqPayload) []byte {
	nameBytes := []byte(p.FileName)
	var buf []byte
	if p.Encrypted {
		buf = make([]byte, PubKeySize+len(nameBytes)+1)
		copy(buf[0:PubKeySize], p.PubKey[:])
		copy(buf[PubKeySize:], nameBytes)
	} else {
		buf = make([]byte, len(nameBytes)+1)
		copy(buf, nameBytes)
	}
	buf[len(buf)-1] = 0
	return buf
}

// UnmarshalPullReq parses a PullReqPayload from bytes.
func UnmarshalPullReq(data []byte, encrypted bool) (PullReqPayload, error) {
	if len(data) == 0 {
		return PullReqPayload{}, fmt.Errorf("empty PULL_REQ payload")
	}
	p := PullReqPayload{Encrypted: encrypted}
	var nameData []byte
	if encrypted {
		if len(data) < PubKeySize+1 {
			return PullReqPayload{}, fmt.Errorf("%w: encrypted PULL_REQ too short", ErrPayloadTooShort)
		}
		copy(p.PubKey[:], data[:PubKeySize])
		nameData = data[PubKeySize:]
	} else {
		nameData = data
	}
	for i, b := range nameData {
		if b == 0 {
			p.FileName = string(nameData[:i])
			return p, nil
		}
	}
	p.FileName = string(nameData)
	return p, nil
}

// --- PUSH_REQ Payload ---
//
// Wire layout (unencrypted):
//
//	Offset  Size      Field
//	0       8 bytes   FileSize
//	8       8 bytes   FileHash (xxHash64 of full file)
//	16      4 bytes   InitialRate (bytes/sec; 0 = calibration)
//	20      variable  FileName (null-terminated)
//
// Wire layout (encrypted):
//
//	Offset  Size      Field
//	0       8 bytes   FileSize
//	8       8 bytes   FileHash
//	16      4 bytes   InitialRate
//	20      32 bytes  PubKey (X25519 ephemeral public key)
//	52      variable  FileName (null-terminated)
const (
	PushReqFixedSize    = 20 // FileSize + FileHash + InitialRate
	PushReqEncFixedSize = 52 // PushReqFixedSize + PubKey
)

// PushReqPayload is sent by the push client to the serve daemon.
type PushReqPayload struct {
	FileSize    uint64
	FileHash    uint64   // xxHash64 of the full file
	InitialRate uint32   // bytes/sec; 0 = use calibration
	PubKey      [32]byte // only on wire when Encrypted=true
	Encrypted   bool
	FileName    string
}

// MarshalPushReq serializes a PushReqPayload into bytes.
func MarshalPushReq(p *PushReqPayload) []byte {
	nameBytes := []byte(p.FileName)
	var buf []byte
	if p.Encrypted {
		buf = make([]byte, PushReqEncFixedSize+len(nameBytes)+1)
		binary.BigEndian.PutUint64(buf[0:8], p.FileSize)
		binary.BigEndian.PutUint64(buf[8:16], p.FileHash)
		binary.BigEndian.PutUint32(buf[16:20], p.InitialRate)
		copy(buf[20:52], p.PubKey[:])
		copy(buf[52:], nameBytes)
	} else {
		buf = make([]byte, PushReqFixedSize+len(nameBytes)+1)
		binary.BigEndian.PutUint64(buf[0:8], p.FileSize)
		binary.BigEndian.PutUint64(buf[8:16], p.FileHash)
		binary.BigEndian.PutUint32(buf[16:20], p.InitialRate)
		copy(buf[20:], nameBytes)
	}
	buf[len(buf)-1] = 0
	return buf
}

// UnmarshalPushReq parses a PushReqPayload from bytes.
func UnmarshalPushReq(data []byte, encrypted bool) (PushReqPayload, error) {
	minSize := PushReqFixedSize + 1
	if encrypted {
		minSize = PushReqEncFixedSize + 1
	}
	if len(data) < minSize {
		return PushReqPayload{}, fmt.Errorf("%w: need at least %d bytes, got %d",
			ErrPayloadTooShort, minSize, len(data))
	}
	p := PushReqPayload{
		FileSize:    binary.BigEndian.Uint64(data[0:8]),
		FileHash:    binary.BigEndian.Uint64(data[8:16]),
		InitialRate: binary.BigEndian.Uint32(data[16:20]),
		Encrypted:   encrypted,
	}
	var nameData []byte
	if encrypted {
		copy(p.PubKey[:], data[20:52])
		nameData = data[52:]
	} else {
		nameData = data[20:]
	}
	for i, b := range nameData {
		if b == 0 {
			p.FileName = string(nameData[:i])
			return p, nil
		}
	}
	p.FileName = string(nameData)
	return p, nil
}

// --- PUSH_ACCEPT Payload ---
//
// Wire layout (unencrypted):
//
//	No payload. The server uses a single shared socket; data flows on the
//	same address:port that received the PUSH_REQ.
//
// Wire layout (encrypted):
//
//	Offset  Size     Field
//	0       32 bytes PubKey (receiver's X25519 ephemeral public key)
const (
	PushAcceptEncFixedSize = 32 // PubKey only
)

// PushAcceptPayload is sent by serve to accept an incoming push.
type PushAcceptPayload struct {
	PubKey    [32]byte // only on wire when Encrypted=true
	Encrypted bool
}

// MarshalPushAccept serializes a PushAcceptPayload into bytes.
func MarshalPushAccept(p *PushAcceptPayload) []byte {
	if p.Encrypted {
		buf := make([]byte, PushAcceptEncFixedSize)
		copy(buf[0:32], p.PubKey[:])
		return buf
	}
	return []byte{} // no payload in unencrypted mode
}

// UnmarshalPushAccept parses a PushAcceptPayload from bytes.
func UnmarshalPushAccept(data []byte, encrypted bool) (PushAcceptPayload, error) {
	if encrypted {
		if len(data) < PushAcceptEncFixedSize {
			return PushAcceptPayload{}, fmt.Errorf("%w: need at least %d bytes, got %d",
				ErrPayloadTooShort, PushAcceptEncFixedSize, len(data))
		}
		var p PushAcceptPayload
		p.Encrypted = true
		copy(p.PubKey[:], data[0:32])
		return p, nil
	}
	return PushAcceptPayload{}, nil // unencrypted: no payload
}

// --- PULL_ACCEPT Payload ---
//
// Wire layout:
//
//	Offset  Size    Field
//	0       2 bytes Port (ephemeral or fixed data port the sender will use)
//
// PullAcceptPayload is sent by serve in response to PULL_REQ when a dedicated
// data port is configured. The client sends a probe packet to this port so its
// NAT allows the server's sender traffic through.
type PullAcceptPayload struct {
	Port uint16
}

// MarshalPullAccept serializes a PullAcceptPayload into bytes.
func MarshalPullAccept(p *PullAcceptPayload) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf[0:2], p.Port)
	return buf
}

// UnmarshalPullAccept parses a PullAcceptPayload from bytes.
func UnmarshalPullAccept(data []byte) (PullAcceptPayload, error) {
	if len(data) < 2 {
		return PullAcceptPayload{}, fmt.Errorf("%w: need at least 2 bytes, got %d",
			ErrPayloadTooShort, len(data))
	}
	return PullAcceptPayload{Port: binary.BigEndian.Uint16(data[0:2])}, nil
}

// --- RESUME_REQ Payload ---
//
// Wire layout (unencrypted):
//
//	Offset  Size      Field
//	0       8 bytes   FullHash (xxHash64 of complete source file)
//	8       8 bytes   FileSize
//	16      8 bytes   ResumeOffset (byte offset to resume from)
//	24      8 bytes   PartialHash (xxHash64 of bytes 0..ResumeOffset)
//	32      variable  FileName (null-terminated)
//
// Wire layout (encrypted):
//
//	Offset  Size      Field
//	0       8 bytes   FullHash
//	8       8 bytes   FileSize
//	16      8 bytes   ResumeOffset
//	24      8 bytes   PartialHash
//	32      32 bytes  PubKey (X25519 ephemeral public key)
//	64      variable  FileName (null-terminated)
const (
	ResumeReqFixedSize    = 32 // 4 × uint64
	ResumeReqEncFixedSize = 64 // ResumeReqFixedSize + PubKeySize
)

// ResumeReqPayload is sent by the receiver to request resuming an
// interrupted transfer from the last checkpoint.
type ResumeReqPayload struct {
	FullHash     uint64   // xxHash64 of the complete source file
	FileSize     uint64   // total expected file size
	ResumeOffset uint64   // byte offset to resume from
	PartialHash  uint64   // xxHash64 of bytes 0..ResumeOffset
	PubKey       [32]byte // only on wire when Encrypted=true
	Encrypted    bool
	FileName     string // original file name
}

// MarshalResumeReq serializes a ResumeReqPayload into bytes.
func MarshalResumeReq(p *ResumeReqPayload) []byte {
	nameBytes := []byte(p.FileName)
	var buf []byte
	if p.Encrypted {
		buf = make([]byte, ResumeReqEncFixedSize+len(nameBytes)+1)
		binary.BigEndian.PutUint64(buf[0:8], p.FullHash)
		binary.BigEndian.PutUint64(buf[8:16], p.FileSize)
		binary.BigEndian.PutUint64(buf[16:24], p.ResumeOffset)
		binary.BigEndian.PutUint64(buf[24:32], p.PartialHash)
		copy(buf[32:64], p.PubKey[:])
		copy(buf[64:], nameBytes)
	} else {
		buf = make([]byte, ResumeReqFixedSize+len(nameBytes)+1)
		binary.BigEndian.PutUint64(buf[0:8], p.FullHash)
		binary.BigEndian.PutUint64(buf[8:16], p.FileSize)
		binary.BigEndian.PutUint64(buf[16:24], p.ResumeOffset)
		binary.BigEndian.PutUint64(buf[24:32], p.PartialHash)
		copy(buf[32:], nameBytes)
	}
	buf[len(buf)-1] = 0
	return buf
}

// UnmarshalResumeReq parses a ResumeReqPayload from bytes.
func UnmarshalResumeReq(data []byte, encrypted bool) (ResumeReqPayload, error) {
	minSize := ResumeReqFixedSize + 1
	if encrypted {
		minSize = ResumeReqEncFixedSize + 1
	}
	if len(data) < minSize {
		return ResumeReqPayload{}, fmt.Errorf("%w: need at least %d bytes, got %d",
			ErrPayloadTooShort, minSize, len(data))
	}
	p := ResumeReqPayload{
		FullHash:     binary.BigEndian.Uint64(data[0:8]),
		FileSize:     binary.BigEndian.Uint64(data[8:16]),
		ResumeOffset: binary.BigEndian.Uint64(data[16:24]),
		PartialHash:  binary.BigEndian.Uint64(data[24:32]),
		Encrypted:    encrypted,
	}
	var nameData []byte
	if encrypted {
		copy(p.PubKey[:], data[32:64])
		nameData = data[64:]
	} else {
		nameData = data[32:]
	}
	for i, b := range nameData {
		if b == 0 {
			p.FileName = string(nameData[:i])
			return p, nil
		}
	}
	p.FileName = string(nameData)
	return p, nil
}

// --- RESUME_ACCEPT Payload ---
//
// Wire layout (unencrypted):
//
//	Offset  Size     Field
//	0       8 bytes  ResumeSequenceNum (confirmed starting sequence)
//
// Wire layout (encrypted):
//
//	Offset  Size     Field
//	0       8 bytes  ResumeSequenceNum
//	8       32 bytes PubKey (sender's ephemeral public key for resumed session)
const (
	ResumeAcceptFixedSize    = 8  // ResumeSequenceNum only
	ResumeAcceptEncFixedSize = 40 // ResumeSequenceNum + PubKeySize
)

// ResumeAcceptPayload is sent by the sender to confirm a resume request.
type ResumeAcceptPayload struct {
	ResumeSequenceNum uint64   // starting sequence for the resumed transfer
	PubKey            [32]byte // only on wire when Encrypted=true
	Encrypted         bool
}

// MarshalResumeAccept serializes a ResumeAcceptPayload into bytes.
func MarshalResumeAccept(p *ResumeAcceptPayload) []byte {
	if p.Encrypted {
		buf := make([]byte, ResumeAcceptEncFixedSize)
		binary.BigEndian.PutUint64(buf[0:8], p.ResumeSequenceNum)
		copy(buf[8:40], p.PubKey[:])
		return buf
	}
	buf := make([]byte, ResumeAcceptFixedSize)
	binary.BigEndian.PutUint64(buf[0:8], p.ResumeSequenceNum)
	return buf
}

// UnmarshalResumeAccept parses a ResumeAcceptPayload from bytes.
func UnmarshalResumeAccept(data []byte, encrypted bool) (ResumeAcceptPayload, error) {
	minSize := ResumeAcceptFixedSize
	if encrypted {
		minSize = ResumeAcceptEncFixedSize
	}
	if len(data) < minSize {
		return ResumeAcceptPayload{}, fmt.Errorf("%w: need at least %d bytes, got %d",
			ErrPayloadTooShort, minSize, len(data))
	}
	p := ResumeAcceptPayload{
		ResumeSequenceNum: binary.BigEndian.Uint64(data[0:8]),
		Encrypted:         encrypted,
	}
	if encrypted {
		copy(p.PubKey[:], data[8:40])
	}
	return p, nil
}

// --- LIST_REQ / LIST_RESP Payloads ---
//
// LIST_REQ has no payload (header only).
//
// LIST_RESP wire layout (v5.2):
//
//	Each line: filename\tsize\n
//	size is the file's byte count as a decimal integer string.
//	Truncated to MaxPayload if the list is very large.

// ListEntry holds a filename and its size for LIST_RESP.
type ListEntry struct {
	Name string
	Size uint64
}

// MarshalListResp serializes a slice of ListEntry into a LIST_RESP payload.
func MarshalListResp(entries []ListEntry) []byte {
	if len(entries) == 0 {
		return []byte{}
	}
	var b []byte
	for _, e := range entries {
		line := e.Name + "\t" + uint64ToDecimal(e.Size) + "\n"
		if len(b)+len(line) > MaxPayload {
			break
		}
		b = append(b, line...)
	}
	return b
}

// uint64ToDecimal converts a uint64 to its decimal string representation
// without using fmt to avoid an import dependency in this package.
func uint64ToDecimal(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// UnmarshalListResp parses a LIST_RESP payload into a slice of ListEntry.
// Each line is expected to be "filename\tsize\n".
func UnmarshalListResp(data []byte) []ListEntry {
	if len(data) == 0 {
		return nil
	}
	var entries []ListEntry
	start := 0
	for i := 0; i <= len(data); i++ {
		if i == len(data) || data[i] == '\n' {
			if i > start {
				line := string(data[start:i])
				// Split on tab: field 0 = name, field 1 = size
				tabIdx := -1
				for j := 0; j < len(line); j++ {
					if line[j] == '\t' {
						tabIdx = j
						break
					}
				}
				if tabIdx >= 0 {
					name := line[:tabIdx]
					size := decimalToUint64(line[tabIdx+1:])
					entries = append(entries, ListEntry{Name: name, Size: size})
				}
			}
			start = i + 1
		}
	}
	return entries
}

// decimalToUint64 parses a decimal string to uint64, returning 0 on error.
func decimalToUint64(s string) uint64 {
	var n uint64
	for _, c := range s {
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + uint64(c-'0')
	}
	return n
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

// --- SESSION_ACCEPT Payload ---
//
// Wire layout (encrypted mode only, packet type 0x0A):
//
//	Offset  Size     Field
//	0       32 bytes PubKey (receiver's ephemeral X25519 public key)

const SessionAcceptSize = 32

// SessionAcceptPayload is sent by the receiver in response to an encrypted
// SESSION_REQ. It carries the receiver's ephemeral public key so both sides
// can derive the session key via X25519 + HKDF-SHA256.
type SessionAcceptPayload struct {
	PubKey [32]byte
}

// MarshalSessionAccept serializes a SessionAcceptPayload into bytes.
func MarshalSessionAccept(p *SessionAcceptPayload) []byte {
	buf := make([]byte, SessionAcceptSize)
	copy(buf, p.PubKey[:])
	return buf
}

// UnmarshalSessionAccept parses a SessionAcceptPayload from bytes.
func UnmarshalSessionAccept(data []byte) (SessionAcceptPayload, error) {
	if len(data) < SessionAcceptSize {
		return SessionAcceptPayload{}, fmt.Errorf("%w: need %d bytes, got %d",
			ErrPayloadTooShort, SessionAcceptSize, len(data))
	}
	var p SessionAcceptPayload
	copy(p.PubKey[:], data[:SessionAcceptSize])
	return p, nil
}
