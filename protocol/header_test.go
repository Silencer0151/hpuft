package protocol

import (
	"bytes"
	"testing"
)

func TestHeaderRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		header Header
	}{
		{
			name: "DATA packet with typical values",
			header: Header{
				Type:        PacketData,
				SessionID:   0xDEADBEEF,
				SequenceNum: 42,
				BlockGroup:  0,
				PayloadLen:  1376,
				Flags:       0,
			},
		},
		{
			name: "SESSION_REQ packet",
			header: Header{
				Type:        PacketSessionReq,
				SessionID:   0x12345678,
				SequenceNum: 0,
				BlockGroup:  0,
				PayloadLen:  128,
				Flags:       0,
			},
		},
		{
			name: "DATA packet with EOF flag",
			header: Header{
				Type:        PacketData,
				SessionID:   0xCAFEBABE,
				SequenceNum: 999999,
				BlockGroup:  9999,
				PayloadLen:  500,
				Flags:       FlagEndOfFile,
			},
		},
		{
			name: "DATA packet with calibration flag",
			header: Header{
				Type:        PacketData,
				SessionID:   0x00000001,
				SequenceNum: 1,
				BlockGroup:  0,
				PayloadLen:  1376,
				Flags:       FlagCalibrationBurst,
			},
		},
		{
			name: "PARITY packet",
			header: Header{
				Type:        PacketParity,
				SessionID:   0xAAAAAAAA,
				SequenceNum: 100,
				BlockGroup:  1,
				PayloadLen:  1376,
				Flags:       0,
			},
		},
		{
			name: "HEARTBEAT packet zero payload",
			header: Header{
				Type:        PacketHeartbeat,
				SessionID:   0xBBBBBBBB,
				SequenceNum: 0,
				BlockGroup:  0,
				PayloadLen:  0,
				Flags:       0,
			},
		},
		{
			name: "max 64-bit sequence number",
			header: Header{
				Type:        PacketData,
				SessionID:   0xFFFFFFFF,
				SequenceNum: ^uint64(0),
				BlockGroup:  ^uint64(0),
				PayloadLen:  1,
				Flags:       FlagEndOfFile | FlagCalibrationBurst,
			},
		},
		{
			name:   "all zeros",
			header: Header{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, HeaderSize)

			n, err := MarshalHeader(buf, &tc.header)
			if err != nil {
				t.Fatalf("MarshalHeader: %v", err)
			}
			if n != HeaderSize {
				t.Fatalf("MarshalHeader wrote %d bytes, want %d", n, HeaderSize)
			}

			got, err := UnmarshalHeader(buf)
			if err != nil {
				t.Fatalf("UnmarshalHeader: %v", err)
			}

			if got != tc.header {
				t.Errorf("round-trip mismatch:\n  got:  %+v\n  want: %+v", got, tc.header)
			}
		})
	}
}

func TestPacketRoundTrip(t *testing.T) {
	payload := make([]byte, 1376)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	original := Packet{
		Header: Header{
			Type:        PacketData,
			SessionID:   0xDEADBEEF,
			SequenceNum: 12345678,
			BlockGroup:  123456,
			Flags:       0,
		},
		Payload: payload,
	}

	raw, err := MarshalPacket(&original)
	if err != nil {
		t.Fatalf("MarshalPacket: %v", err)
	}

	if len(raw) != HeaderSize+len(payload) {
		t.Fatalf("marshaled size = %d, want %d", len(raw), HeaderSize+len(payload))
	}

	got, err := UnmarshalPacket(raw)
	if err != nil {
		t.Fatalf("UnmarshalPacket: %v", err)
	}

	if got.Header != original.Header {
		t.Errorf("header mismatch:\n  got:  %+v\n  want: %+v", got.Header, original.Header)
	}
	if !bytes.Equal(got.Payload, original.Payload) {
		t.Error("payload mismatch")
	}
}

func TestMarshalHeaderBufferTooSmall(t *testing.T) {
	buf := make([]byte, HeaderSize-1)
	_, err := MarshalHeader(buf, &Header{})
	if err != ErrBufferTooSmall {
		t.Errorf("expected ErrBufferTooSmall, got %v", err)
	}
}

func TestUnmarshalHeaderBufferTooSmall(t *testing.T) {
	buf := make([]byte, HeaderSize-1)
	_, err := UnmarshalHeader(buf)
	if err != ErrBufferTooSmall {
		t.Errorf("expected ErrBufferTooSmall, got %v", err)
	}
}

func TestMarshalPayloadTooLarge(t *testing.T) {
	p := Packet{
		Header:  Header{Type: PacketData},
		Payload: make([]byte, MaxPayload+1),
	}
	_, err := MarshalPacket(&p)
	if err == nil {
		t.Error("expected error for oversized payload, got nil")
	}
}

func TestUnmarshalPacketPayloadBoundsCheck(t *testing.T) {
	// Create a packet that claims a larger payload than actually present
	buf := make([]byte, HeaderSize+10)
	h := Header{
		Type:       PacketData,
		PayloadLen: 100, // claims 100 bytes but only 10 follow
	}
	MarshalHeader(buf, &h)

	_, err := UnmarshalPacket(buf)
	if err == nil {
		t.Error("expected error for truncated payload, got nil")
	}
}

func TestHeaderSize(t *testing.T) {
	// Sanity check: header must be exactly 24 bytes (3 x 8-byte words)
	if HeaderSize != 24 {
		t.Errorf("HeaderSize = %d, want 24", HeaderSize)
	}
	if HeaderSize%8 != 0 {
		t.Errorf("HeaderSize %d is not 64-bit aligned", HeaderSize)
	}
}

func TestMaxPayload(t *testing.T) {
	if MaxPayload != MTUHardCap-HeaderSize {
		t.Errorf("MaxPayload = %d, want %d", MaxPayload, MTUHardCap-HeaderSize)
	}
	if MaxPayload != 1376 {
		t.Errorf("MaxPayload = %d, want 1376", MaxPayload)
	}
}
