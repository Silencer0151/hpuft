package protocol

import (
	"testing"
)

func TestSessionReqRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		payload SessionReqPayload
	}{
		{
			name: "typical file transfer",
			payload: SessionReqPayload{
				FileSize:    1024 * 1024 * 500, // 500 MB
				Checksum:    0xDEADBEEFCAFEBABE,
				InitialRate: 0, // calibration mode
				FileName:    "darksouls3_2022.09.05.mp4",
			},
		},
		{
			name: "explicit initial rate",
			payload: SessionReqPayload{
				FileSize:    1024,
				Checksum:    0x1234567890ABCDEF,
				InitialRate: 1_000_000_000, // 1 GB/s
				FileName:    "test.txt",
			},
		},
		{
			name: "long filename",
			payload: SessionReqPayload{
				FileSize:    42,
				Checksum:    1,
				InitialRate: 0,
				FileName:    "/home/user/documents/projects/hpuft/testdata/very_long_filename_that_tests_boundaries.bin",
			},
		},
		{
			name: "single char filename",
			payload: SessionReqPayload{
				FileSize:    1,
				Checksum:    0,
				InitialRate: 0,
				FileName:    "x",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := MarshalSessionReq(&tc.payload)
			got, err := UnmarshalSessionReq(data)
			if err != nil {
				t.Fatalf("UnmarshalSessionReq: %v", err)
			}
			if got != tc.payload {
				t.Errorf("round-trip mismatch:\n  got:  %+v\n  want: %+v", got, tc.payload)
			}
		})
	}
}

func TestSessionReqTooShort(t *testing.T) {
	_, err := UnmarshalSessionReq(make([]byte, 10))
	if err == nil {
		t.Error("expected error for short payload")
	}
}

func TestHeartbeatRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		payload HeartbeatPayload
	}{
		{
			name: "no NACKs",
			payload: HeartbeatPayload{
				NetworkDeliveryRate: 500_000_000,
				StorageFlushRate:    400_000_000,
				LossRate:            50, // 0.50%
				HighestContiguous:   999,
				NACKCount:           0,
				EchoTimestampNs:     1_700_000_000_000_000_000,
				DispersionNs:        8_500_000,
				NACKs:               nil,
			},
		},
		{
			name: "with NACKs",
			payload: HeartbeatPayload{
				NetworkDeliveryRate: 1_000_000_000,
				StorageFlushRate:    800_000_000,
				LossRate:            200, // 2.00%
				HighestContiguous:   5000,
				NACKCount:           3,
				EchoTimestampNs:     0,
				DispersionNs:        0,
				NACKs:               []uint64{42, 99, 5001},
			},
		},
		{
			name: "high contiguous with 64-bit seq",
			payload: HeartbeatPayload{
				NetworkDeliveryRate: 100_000,
				StorageFlushRate:    100_000,
				LossRate:            0,
				HighestContiguous:   ^uint64(0) - 1,
				NACKCount:           1,
				NACKs:               []uint64{^uint64(0)},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := MarshalHeartbeat(&tc.payload)
			got, err := UnmarshalHeartbeat(data)
			if err != nil {
				t.Fatalf("UnmarshalHeartbeat: %v", err)
			}

			if got.NetworkDeliveryRate != tc.payload.NetworkDeliveryRate ||
				got.StorageFlushRate != tc.payload.StorageFlushRate ||
				got.LossRate != tc.payload.LossRate ||
				got.HighestContiguous != tc.payload.HighestContiguous ||
				got.NACKCount != tc.payload.NACKCount ||
				got.EchoTimestampNs != tc.payload.EchoTimestampNs ||
				got.DispersionNs != tc.payload.DispersionNs {
				t.Errorf("fixed field mismatch:\n  got:  %+v\n  want: %+v", got, tc.payload)
			}

			if len(got.NACKs) != len(tc.payload.NACKs) {
				t.Fatalf("NACK count mismatch: got %d, want %d", len(got.NACKs), len(tc.payload.NACKs))
			}
			for i := range got.NACKs {
				if got.NACKs[i] != tc.payload.NACKs[i] {
					t.Errorf("NACK[%d] = %d, want %d", i, got.NACKs[i], tc.payload.NACKs[i])
				}
			}
		})
	}
}

func TestHeartbeatTooShort(t *testing.T) {
	_, err := UnmarshalHeartbeat(make([]byte, 10))
	if err == nil {
		t.Error("expected error for short payload")
	}
}

func TestHeartbeatTruncatedNACKs(t *testing.T) {
	// Create a heartbeat claiming 5 NACKs but only providing data for 2.
	// NACKCount is at wire offset 18-19 (unchanged by v3.1 layout).
	p := HeartbeatPayload{
		NACKCount: 5,
		NACKs:     []uint64{1, 2}, // only 2 → marshal produces HeartbeatFixedSize+16 bytes
	}
	data := MarshalHeartbeat(&p)
	// Lie about NACKCount: claim 5 when only 2 are present.
	data[18] = 0
	data[19] = 5

	_, err := UnmarshalHeartbeat(data)
	if err == nil {
		t.Error("expected error for truncated NACK array")
	}
}
