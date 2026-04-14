package protocol

import (
	"testing"
)

// --- Heartbeat (data plane, unchanged) ---

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
	p := HeartbeatPayload{
		NACKCount: 5,
		NACKs:     []uint64{1, 2},
	}
	data := MarshalHeartbeat(&p)
	data[34] = 0
	data[35] = 5

	_, err := UnmarshalHeartbeat(data)
	if err == nil {
		t.Error("expected error for truncated NACK array")
	}
}

// --- Request builders (v6 control plane) ---

func TestBuildPutRequestRoundTrip(t *testing.T) {
	tests := []struct {
		name        string
		fileSize    uint64
		fileHash    uint64
		initialRate uint32
		fileName    string
	}{
		{"typical", 500 * 1024 * 1024, 0xDEADBEEFCAFEBABE, 0, "video.mp4"},
		{"explicit rate", 1024, 0x1234567890ABCDEF, 1_000_000_000, "test.txt"},
		{"long filename", 42, 1, 0, "a/b/c/very_long_filename_that_tests_boundary_conditions.bin"},
		{"empty filename", 0, 0, 0, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw := BuildPutRequest(tc.fileSize, tc.fileHash, tc.initialRate, tc.fileName)

			op, body, err := ParseRequest(raw)
			if err != nil {
				t.Fatalf("ParseRequest: %v", err)
			}
			if op != OpPut {
				t.Fatalf("op = %v, want OpPut", op)
			}
			// body: FileSize(8) | FileHash(8) | InitialRate(4) | FileName(null-term)
			if len(body) < 20 {
				t.Fatalf("body too short: %d", len(body))
			}
			gotSize := uint64(body[0])<<56 | uint64(body[1])<<48 | uint64(body[2])<<40 | uint64(body[3])<<32 |
				uint64(body[4])<<24 | uint64(body[5])<<16 | uint64(body[6])<<8 | uint64(body[7])
			gotHash := uint64(body[8])<<56 | uint64(body[9])<<48 | uint64(body[10])<<40 | uint64(body[11])<<32 |
				uint64(body[12])<<24 | uint64(body[13])<<16 | uint64(body[14])<<8 | uint64(body[15])
			gotRate := uint32(body[16])<<24 | uint32(body[17])<<16 | uint32(body[18])<<8 | uint32(body[19])

			// parse null-terminated filename
			nameBytes := body[20:]
			gotName := ""
			found := false
			for i, b := range nameBytes {
				if b == 0 {
					gotName = string(nameBytes[:i])
					found = true
					break
				}
			}
			if !found {
				gotName = string(nameBytes)
			}

			if gotSize != tc.fileSize || gotHash != tc.fileHash || gotRate != tc.initialRate || gotName != tc.fileName {
				t.Errorf("round-trip mismatch: size=%d hash=%x rate=%d name=%q",
					gotSize, gotHash, gotRate, gotName)
			}
		})
	}
}

func TestBuildGetRequestRoundTrip(t *testing.T) {
	for _, name := range []string{"file.txt", "sub/dir/file.bin", ""} {
		raw := BuildGetRequest(name)
		op, body, err := ParseRequest(raw)
		if err != nil {
			t.Fatalf("ParseRequest: %v", err)
		}
		if op != OpGet {
			t.Fatalf("op = %v, want OpGet", op)
		}
		gotName := ""
		for i, b := range body {
			if b == 0 {
				gotName = string(body[:i])
				break
			}
		}
		if gotName != name {
			t.Errorf("filename: got %q, want %q", gotName, name)
		}
	}
}

func TestBuildListRequestRoundTrip(t *testing.T) {
	raw := BuildListRequest()
	op, _, err := ParseRequest(raw)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if op != OpList {
		t.Fatalf("op = %v, want OpList", op)
	}
}

func TestBuildDeleteRequestRoundTrip(t *testing.T) {
	for _, name := range []string{"file.txt", "path/to/file.bin"} {
		raw := BuildDeleteRequest(name)
		op, body, err := ParseRequest(raw)
		if err != nil {
			t.Fatalf("ParseRequest: %v", err)
		}
		if op != OpDelete {
			t.Fatalf("op = %v, want OpDelete", op)
		}
		gotName := ""
		for i, b := range body {
			if b == 0 {
				gotName = string(body[:i])
				break
			}
		}
		if gotName != name {
			t.Errorf("filename: got %q, want %q", gotName, name)
		}
	}
}

func TestParseRequestEmpty(t *testing.T) {
	_, _, err := ParseRequest(nil)
	if err == nil {
		t.Error("expected error for empty request")
	}
}

// --- Response builders (v6 control plane) ---

func TestBuildGetResponseOKRoundTrip(t *testing.T) {
	fileSize := uint64(500 * 1024 * 1024)
	fileHash := uint64(0xDEADBEEFCAFEBABE)
	initialRate := uint32(1_000_000_000)

	raw := BuildGetResponseOK(fileSize, fileHash, initialRate)
	status, reason, body, err := ParseResponse(raw)
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	if status != StatusOK || reason != 0 {
		t.Fatalf("status=%v reason=%v, want OK/0", status, reason)
	}
	if len(body) < 20 {
		t.Fatalf("body too short: %d", len(body))
	}
	gotSize := uint64(body[0])<<56 | uint64(body[1])<<48 | uint64(body[2])<<40 | uint64(body[3])<<32 |
		uint64(body[4])<<24 | uint64(body[5])<<16 | uint64(body[6])<<8 | uint64(body[7])
	gotHash := uint64(body[8])<<56 | uint64(body[9])<<48 | uint64(body[10])<<40 | uint64(body[11])<<32 |
		uint64(body[12])<<24 | uint64(body[13])<<16 | uint64(body[14])<<8 | uint64(body[15])
	gotRate := uint32(body[16])<<24 | uint32(body[17])<<16 | uint32(body[18])<<8 | uint32(body[19])
	if gotSize != fileSize || gotHash != fileHash || gotRate != initialRate {
		t.Errorf("round-trip mismatch: size=%d hash=%x rate=%d", gotSize, gotHash, gotRate)
	}
}

func TestBuildPutResponseOKRoundTrip(t *testing.T) {
	raw := BuildPutResponseOK()
	status, _, _, err := ParseResponse(raw)
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	if status != StatusOK {
		t.Fatalf("status=%v, want OK", status)
	}
}

func TestBuildDeleteResponseOKRoundTrip(t *testing.T) {
	raw := BuildDeleteResponseOK()
	status, _, _, err := ParseResponse(raw)
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	if status != StatusOK {
		t.Fatalf("status=%v, want OK", status)
	}
}

func TestBuildListResponseOKRoundTrip(t *testing.T) {
	entries := []ListEntry{
		{Name: "foo.txt", Size: 1024},
		{Name: "bar.bin", Size: 2048},
	}
	body := MarshalListBody(entries)
	raw := BuildListResponseOK(body)

	status, _, respBody, err := ParseResponse(raw)
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	if status != StatusOK {
		t.Fatalf("status=%v, want OK", status)
	}
	got := UnmarshalListBody(respBody)
	if len(got) != len(entries) {
		t.Fatalf("entry count: got %d, want %d", len(got), len(entries))
	}
	for i, e := range entries {
		if got[i].Name != e.Name || got[i].Size != e.Size {
			t.Errorf("entry[%d]: got %+v, want %+v", i, got[i], e)
		}
	}
}

func TestBuildResponseErrorRoundTrip(t *testing.T) {
	tests := []struct {
		reason Reason
		msg    string
	}{
		{ReasonFileNotFound, "no such file"},
		{ReasonServerBusy, ""},
		{ReasonHashMismatch, "hash mismatch on transfer"},
	}

	for _, tc := range tests {
		raw := BuildResponseError(tc.reason, tc.msg)
		status, gotReason, body, err := ParseResponse(raw)
		if err != nil {
			t.Fatalf("ParseResponse: %v", err)
		}
		if status != StatusError {
			t.Fatalf("status=%v, want Error", status)
		}
		if gotReason != tc.reason {
			t.Errorf("reason=%v, want %v", gotReason, tc.reason)
		}
		if body != nil {
			t.Errorf("body should be nil for error response, got %v", body)
		}
	}
}

func TestParseResponseEmpty(t *testing.T) {
	_, _, _, err := ParseResponse(nil)
	if err == nil {
		t.Error("expected error for empty response")
	}
}

func TestParseResponseErrorMissingReason(t *testing.T) {
	_, _, _, err := ParseResponse([]byte{byte(StatusError)})
	if err == nil {
		t.Error("expected error for missing reason byte")
	}
}

// --- MarshalListBody / UnmarshalListBody ---

func TestListBodyRoundTrip(t *testing.T) {
	entries := []ListEntry{
		{Name: "alpha.txt", Size: 100},
		{Name: "beta.bin", Size: 999999},
		{Name: "gamma", Size: 0},
	}
	data := MarshalListBody(entries)
	got := UnmarshalListBody(data)
	if len(got) != len(entries) {
		t.Fatalf("count: got %d, want %d", len(got), len(entries))
	}
	for i, e := range entries {
		if got[i].Name != e.Name || got[i].Size != e.Size {
			t.Errorf("[%d]: got %+v, want %+v", i, got[i], e)
		}
	}
}

func TestListBodyEmpty(t *testing.T) {
	data := MarshalListBody(nil)
	got := UnmarshalListBody(data)
	if len(got) != 0 {
		t.Errorf("expected empty result, got %v", got)
	}
}
