package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var (
	ErrBufferTooSmall  = errors.New("buffer smaller than header size")
	ErrPayloadTooLarge = errors.New("payload exceeds maximum size")
	ErrPacketTooSmall  = errors.New("raw packet smaller than header size")
)

// MarshalHeader writes a 24-byte header into dst.
// dst must be at least HeaderSize bytes. Returns HeaderSize on success.
func MarshalHeader(dst []byte, h *Header) (int, error) {
	if len(dst) < HeaderSize {
		return 0, ErrBufferTooSmall
	}
	if h.PayloadLen > MaxPayload {
		return 0, fmt.Errorf("%w: %d > %d", ErrPayloadTooLarge, h.PayloadLen, MaxPayload)
	}

	dst[0] = byte(h.Type)
	binary.BigEndian.PutUint32(dst[1:5], h.SessionID)
	binary.BigEndian.PutUint64(dst[5:13], h.SequenceNum)
	binary.BigEndian.PutUint64(dst[13:21], h.BlockGroup)
	binary.BigEndian.PutUint16(dst[21:23], h.PayloadLen)
	dst[23] = byte(h.Flags)

	return HeaderSize, nil
}

// UnmarshalHeader reads a 24-byte header from src.
func UnmarshalHeader(src []byte) (Header, error) {
	if len(src) < HeaderSize {
		return Header{}, ErrBufferTooSmall
	}

	h := Header{
		Type:        PacketType(src[0]),
		SessionID:   binary.BigEndian.Uint32(src[1:5]),
		SequenceNum: binary.BigEndian.Uint64(src[5:13]),
		BlockGroup:  binary.BigEndian.Uint64(src[13:21]),
		PayloadLen:  binary.BigEndian.Uint16(src[21:23]),
		Flags:       Flag(src[23]),
	}

	return h, nil
}

// MarshalPacket serializes a complete packet (header + payload) into a new byte slice.
func MarshalPacket(p *Packet) ([]byte, error) {
	if len(p.Payload) > MaxPayload {
		return nil, fmt.Errorf("%w: %d > %d", ErrPayloadTooLarge, len(p.Payload), MaxPayload)
	}

	p.Header.PayloadLen = uint16(len(p.Payload))
	buf := make([]byte, HeaderSize+len(p.Payload))

	if _, err := MarshalHeader(buf, &p.Header); err != nil {
		return nil, err
	}
	copy(buf[HeaderSize:], p.Payload)

	return buf, nil
}

// UnmarshalPacket parses a raw datagram into a Packet.
func UnmarshalPacket(raw []byte) (Packet, error) {
	if len(raw) < HeaderSize {
		return Packet{}, ErrPacketTooSmall
	}

	h, err := UnmarshalHeader(raw)
	if err != nil {
		return Packet{}, err
	}

	payloadEnd := HeaderSize + int(h.PayloadLen)
	if payloadEnd > len(raw) {
		return Packet{}, fmt.Errorf("payload length %d extends beyond packet bounds (%d bytes available)", h.PayloadLen, len(raw)-HeaderSize)
	}

	payload := make([]byte, h.PayloadLen)
	copy(payload, raw[HeaderSize:payloadEnd])

	return Packet{Header: h, Payload: payload}, nil
}
