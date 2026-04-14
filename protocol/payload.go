package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var (
	ErrPayloadTooShort = errors.New("payload too short for expected structure")
)

// --- HEARTBEAT Payload ---
//
// Wire layout:
//
//	Offset  Size      Field
//	0       4 bytes   NetworkDeliveryRate
//	4       4 bytes   StorageFlushRate
//	8       2 bytes   LossRate (basis points)
//	10      8 bytes   EchoTimestampNs
//	18      8 bytes   DispersionNs
//	26      8 bytes   HighestContiguous
//	34      2 bytes   NACKCount
//	36      8*N bytes NACKArray

const HeartbeatFixedSize = 36

// MarshalHeartbeat serializes a HeartbeatPayload into bytes.
func MarshalHeartbeat(p *HeartbeatPayload) []byte {
	buf := make([]byte, HeartbeatFixedSize+8*len(p.NACKs))

	binary.BigEndian.PutUint32(buf[0:4], p.NetworkDeliveryRate)
	binary.BigEndian.PutUint32(buf[4:8], p.StorageFlushRate)
	binary.BigEndian.PutUint16(buf[8:10], p.LossRate)
	binary.BigEndian.PutUint64(buf[10:18], p.EchoTimestampNs)
	binary.BigEndian.PutUint64(buf[18:26], p.DispersionNs)
	binary.BigEndian.PutUint64(buf[26:34], p.HighestContiguous)
	binary.BigEndian.PutUint16(buf[34:36], uint16(len(p.NACKs)))

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
		EchoTimestampNs:     binary.BigEndian.Uint64(data[10:18]),
		DispersionNs:        binary.BigEndian.Uint64(data[18:26]),
		HighestContiguous:   binary.BigEndian.Uint64(data[26:34]),
		NACKCount:           binary.BigEndian.Uint16(data[34:36]),
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

// --- LIST body helpers ---
//
// LIST body wire layout (shared by v6 LIST RESPONSE and legacy LIST_RESP):
//
//	Each line: filename\tsize\n
//	size is the file's byte count as a decimal integer string.

// ListEntry holds a filename and its size for LIST responses.
type ListEntry struct {
	Name string
	Size uint64
}

// MarshalListBody serializes a slice of ListEntry into the tab-separated
// format used inside v6 LIST RESPONSE bodies.
// Each line: "filename\tsize\n". Truncated to MaxPayload if needed.
func MarshalListBody(entries []ListEntry) []byte {
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

// UnmarshalListBody parses the tab-separated LIST body format used in v6
// RESPONSE payloads. Each line is expected to be "filename\tsize\n".
func UnmarshalListBody(data []byte) []ListEntry {
	if len(data) == 0 {
		return nil
	}
	var entries []ListEntry
	start := 0
	for i := 0; i <= len(data); i++ {
		if i == len(data) || data[i] == '\n' {
			if i > start {
				line := string(data[start:i])
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

// --- v6 Request Builders ---

// BuildPutRequest encodes a PUT request plaintext body.
// Layout: OpPut(1) | FileSize(8) | FileHash(8) | InitialRate(4) | FileName(null-term)
func BuildPutRequest(fileSize, fileHash uint64, initialRate uint32, fileName string) []byte {
	name := []byte(fileName)
	buf := make([]byte, 1+8+8+4+len(name)+1)
	buf[0] = byte(OpPut)
	binary.BigEndian.PutUint64(buf[1:9], fileSize)
	binary.BigEndian.PutUint64(buf[9:17], fileHash)
	binary.BigEndian.PutUint32(buf[17:21], initialRate)
	copy(buf[21:], name)
	return buf
}

// BuildGetRequest encodes a GET request plaintext body.
// Layout: OpGet(1) | FileName(null-term)
func BuildGetRequest(fileName string) []byte {
	name := []byte(fileName)
	buf := make([]byte, 1+len(name)+1)
	buf[0] = byte(OpGet)
	copy(buf[1:], name)
	return buf
}

// BuildListRequest encodes a LIST request plaintext body.
// Layout: OpList(1)
func BuildListRequest() []byte {
	return []byte{byte(OpList)}
}

// BuildDeleteRequest encodes a DELETE request plaintext body.
// Layout: OpDelete(1) | FileName(null-term)
func BuildDeleteRequest(fileName string) []byte {
	name := []byte(fileName)
	buf := make([]byte, 1+len(name)+1)
	buf[0] = byte(OpDelete)
	copy(buf[1:], name)
	return buf
}

// ParseRequest decodes an incoming request plaintext body.
// Returns (opcode, body after the op byte, error).
func ParseRequest(plaintext []byte) (OpCode, []byte, error) {
	if len(plaintext) == 0 {
		return 0, nil, errors.New("empty request payload")
	}
	return OpCode(plaintext[0]), plaintext[1:], nil
}

// --- v6 Response Builders ---

// BuildGetResponseOK encodes a success body for a GET response.
// Layout: StatusOK(1) | FileSize(8) | FileHash(8) | InitialRate(4)
func BuildGetResponseOK(fileSize, fileHash uint64, initialRate uint32) []byte {
	buf := make([]byte, 1+8+8+4)
	buf[0] = byte(StatusOK)
	binary.BigEndian.PutUint64(buf[1:9], fileSize)
	binary.BigEndian.PutUint64(buf[9:17], fileHash)
	binary.BigEndian.PutUint32(buf[17:21], initialRate)
	return buf
}

// BuildPutResponseOK encodes a success body for a PUT response.
// Layout: StatusOK(1)
func BuildPutResponseOK() []byte {
	return []byte{byte(StatusOK)}
}

// BuildListResponseOK encodes a success body for a LIST response.
// Layout: StatusOK(1) | list body (tab-separated lines from MarshalListBody)
func BuildListResponseOK(body []byte) []byte {
	buf := make([]byte, 1+len(body))
	buf[0] = byte(StatusOK)
	copy(buf[1:], body)
	return buf
}

// BuildDeleteResponseOK encodes a success body for a DELETE response.
// Layout: StatusOK(1)
func BuildDeleteResponseOK() []byte {
	return []byte{byte(StatusOK)}
}

// BuildResponseError encodes an error body for any response.
// Layout: StatusError(1) | Reason(1) | optional message (null-terminated)
func BuildResponseError(reason Reason, msg string) []byte {
	if msg == "" {
		return []byte{byte(StatusError), byte(reason)}
	}
	msgBytes := []byte(msg)
	buf := make([]byte, 2+len(msgBytes)+1)
	buf[0] = byte(StatusError)
	buf[1] = byte(reason)
	copy(buf[2:], msgBytes)
	return buf
}

// ParseResponse decodes an incoming response plaintext body.
// For StatusOK, body holds the op-specific payload (bytes after the status byte).
// For StatusError, body is nil; reason carries the error code.
func ParseResponse(plaintext []byte) (ResponseStatus, Reason, []byte, error) {
	if len(plaintext) == 0 {
		return 0, 0, nil, errors.New("empty response payload")
	}
	status := ResponseStatus(plaintext[0])
	switch status {
	case StatusOK:
		return status, 0, plaintext[1:], nil
	case StatusError:
		if len(plaintext) < 2 {
			return status, 0, nil, errors.New("error response missing reason byte")
		}
		return status, Reason(plaintext[1]), nil, nil
	default:
		return status, 0, nil, fmt.Errorf("unknown response status: 0x%02x", status)
	}
}
