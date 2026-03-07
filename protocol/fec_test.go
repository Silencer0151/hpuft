package protocol

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"
)

// --- GF(2^8) Arithmetic Tests ---

func TestGFExpLogConsistency(t *testing.T) {
	// For all non-zero elements, exp(log(x)) == x
	for i := 1; i < 256; i++ {
		x := byte(i)
		got := gfExp[gfLog[x]]
		if got != x {
			t.Fatalf("exp(log(%d)) = %d, want %d", x, got, x)
		}
	}
}

func TestGFMulIdentity(t *testing.T) {
	for i := 0; i < 256; i++ {
		x := byte(i)
		if gfMul(x, 1) != x {
			t.Fatalf("gfMul(%d, 1) = %d", x, gfMul(x, 1))
		}
		if gfMul(1, x) != x {
			t.Fatalf("gfMul(1, %d) = %d", x, gfMul(1, x))
		}
	}
}

func TestGFMulZero(t *testing.T) {
	for i := 0; i < 256; i++ {
		x := byte(i)
		if gfMul(x, 0) != 0 {
			t.Fatalf("gfMul(%d, 0) = %d", x, gfMul(x, 0))
		}
		if gfMul(0, x) != 0 {
			t.Fatalf("gfMul(0, %d) = %d", x, gfMul(0, x))
		}
	}
}

func TestGFMulCommutative(t *testing.T) {
	for i := 0; i < 256; i++ {
		for j := 0; j < 256; j++ {
			a, b := byte(i), byte(j)
			if gfMul(a, b) != gfMul(b, a) {
				t.Fatalf("gfMul(%d,%d) != gfMul(%d,%d)", a, b, b, a)
			}
		}
	}
}

func TestGFMulInverse(t *testing.T) {
	for i := 1; i < 256; i++ {
		x := byte(i)
		inv := gfInv(x)
		product := gfMul(x, inv)
		if product != 1 {
			t.Fatalf("gfMul(%d, gfInv(%d)) = %d, want 1", x, x, product)
		}
	}
}

func TestGFDivRoundTrip(t *testing.T) {
	for i := 1; i < 256; i++ {
		for j := 1; j < 256; j++ {
			a, b := byte(i), byte(j)
			quotient := gfDiv(a, b)
			product := gfMul(quotient, b)
			if product != a {
				t.Fatalf("gfMul(gfDiv(%d,%d), %d) = %d, want %d", a, b, b, product, a)
			}
		}
	}
}

func TestGFPow(t *testing.T) {
	// x^0 = 1 for all x
	for i := 0; i < 256; i++ {
		if gfPow(byte(i), 0) != 1 {
			t.Fatalf("gfPow(%d, 0) = %d, want 1", i, gfPow(byte(i), 0))
		}
	}
	// x^1 = x for all x
	for i := 0; i < 256; i++ {
		if gfPow(byte(i), 1) != byte(i) {
			t.Fatalf("gfPow(%d, 1) = %d", i, gfPow(byte(i), 1))
		}
	}
	// 0^n = 0 for n > 0
	for n := 1; n < 10; n++ {
		if gfPow(0, n) != 0 {
			t.Fatalf("gfPow(0, %d) = %d, want 0", n, gfPow(0, n))
		}
	}
}

// --- Reed-Solomon Encoder Tests ---

func TestNewRSEncoderValidation(t *testing.T) {
	_, err := NewRSEncoder(0, 5)
	if err == nil {
		t.Error("expected error for 0 data shards")
	}
	_, err = NewRSEncoder(5, 0)
	if err == nil {
		t.Error("expected error for 0 parity shards")
	}
	_, err = NewRSEncoder(200, 100)
	if err == nil {
		t.Error("expected error for total > 255")
	}
}

func TestEncodeDecodeNoLoss(t *testing.T) {
	enc, err := NewRSEncoder(10, 3)
	if err != nil {
		t.Fatal(err)
	}

	data := makeRandomShards(10, 1376)
	parity, err := enc.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	if len(parity) != 3 {
		t.Fatalf("got %d parity shards, want 3", len(parity))
	}

	// With all shards present, reconstruct should be a no-op
	allShards := append(cloneShards(data), parity...)
	err = enc.Reconstruct(allShards)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if !bytes.Equal(allShards[i], data[i]) {
			t.Fatalf("shard %d corrupted after no-loss reconstruct", i)
		}
	}
}

func TestReconstructSingleDataLoss(t *testing.T) {
	enc, err := NewRSEncoder(10, 3)
	if err != nil {
		t.Fatal(err)
	}

	data := makeRandomShards(10, 1376)
	parity, err := enc.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	// Drop each data shard one at a time and verify recovery
	for drop := 0; drop < 10; drop++ {
		allShards := append(cloneShards(data), cloneShards(parity)...)
		allShards[drop] = nil

		err = enc.Reconstruct(allShards)
		if err != nil {
			t.Fatalf("reconstruct failed with shard %d dropped: %v", drop, err)
		}
		if !bytes.Equal(allShards[drop], data[drop]) {
			t.Fatalf("shard %d not correctly recovered", drop)
		}
	}
}

func TestReconstructMaxLoss(t *testing.T) {
	k, m := 10, 5
	enc, err := NewRSEncoder(k, m)
	if err != nil {
		t.Fatal(err)
	}

	data := makeRandomShards(k, 1376)
	parity, err := enc.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	// Drop exactly m data shards (the maximum recoverable)
	allShards := append(cloneShards(data), cloneShards(parity)...)
	for i := 0; i < m; i++ {
		allShards[i] = nil // drop first m data shards
	}

	err = enc.Reconstruct(allShards)
	if err != nil {
		t.Fatalf("reconstruct failed at max erasure: %v", err)
	}
	for i := 0; i < k; i++ {
		if !bytes.Equal(allShards[i], data[i]) {
			t.Fatalf("data shard %d mismatch after max-loss recovery", i)
		}
	}
}

func TestReconstructTooManyLost(t *testing.T) {
	k, m := 10, 3
	enc, err := NewRSEncoder(k, m)
	if err != nil {
		t.Fatal(err)
	}

	data := makeRandomShards(k, 1376)
	parity, err := enc.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	// Drop m+1 shards — should fail
	allShards := append(cloneShards(data), cloneShards(parity)...)
	for i := 0; i < m+1; i++ {
		allShards[i] = nil
	}

	err = enc.Reconstruct(allShards)
	if err == nil {
		t.Fatal("expected error when too many shards are lost")
	}
}

func TestReconstructMixedDataAndParityLoss(t *testing.T) {
	k, m := 10, 5
	enc, err := NewRSEncoder(k, m)
	if err != nil {
		t.Fatal(err)
	}

	data := makeRandomShards(k, 1376)
	parity, err := enc.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	// Drop 2 data shards and 3 parity shards (5 total = m, should work)
	allShards := append(cloneShards(data), cloneShards(parity)...)
	allShards[0] = nil  // data 0
	allShards[5] = nil  // data 5
	allShards[10] = nil // parity 0
	allShards[12] = nil // parity 2
	allShards[14] = nil // parity 4

	err = enc.Reconstruct(allShards)
	if err != nil {
		t.Fatalf("reconstruct failed with mixed loss: %v", err)
	}
	if !bytes.Equal(allShards[0], data[0]) {
		t.Fatal("data shard 0 not recovered")
	}
	if !bytes.Equal(allShards[5], data[5]) {
		t.Fatal("data shard 5 not recovered")
	}
}

func TestTailBlockSmall(t *testing.T) {
	// Simulate a tail block with only 3 data packets
	enc, err := NewRSEncoder(3, 2)
	if err != nil {
		t.Fatal(err)
	}

	data := makeRandomShards(3, 500) // smaller payload
	parity, err := enc.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	// Drop 2 data shards (max recoverable with 2 parity)
	allShards := append(cloneShards(data), cloneShards(parity)...)
	allShards[0] = nil
	allShards[2] = nil

	err = enc.Reconstruct(allShards)
	if err != nil {
		t.Fatalf("tail block reconstruct failed: %v", err)
	}
	if !bytes.Equal(allShards[0], data[0]) {
		t.Fatal("tail shard 0 not recovered")
	}
	if !bytes.Equal(allShards[2], data[2]) {
		t.Fatal("tail shard 2 not recovered")
	}
}

func TestTailBlockSinglePacket(t *testing.T) {
	// Edge case: a tail block of exactly 1 data packet + 2 parity (spec minimum)
	enc, err := NewRSEncoder(1, 2)
	if err != nil {
		t.Fatal(err)
	}

	data := makeRandomShards(1, 1376)
	parity, err := enc.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	// Drop the only data shard — should recover from either parity
	allShards := append(cloneShards(data), cloneShards(parity)...)
	allShards[0] = nil

	err = enc.Reconstruct(allShards)
	if err != nil {
		t.Fatalf("single-packet tail block reconstruct failed: %v", err)
	}
	if !bytes.Equal(allShards[0], data[0]) {
		t.Fatal("single data shard not recovered")
	}
}

func TestEncodeShardLengthMismatch(t *testing.T) {
	enc, err := NewRSEncoder(5, 2)
	if err != nil {
		t.Fatal(err)
	}

	data := makeRandomShards(5, 100)
	data[3] = make([]byte, 50) // wrong length

	_, err = enc.Encode(data)
	if err == nil {
		t.Error("expected error for mismatched shard lengths")
	}
}

func TestEncodeWrongShardCount(t *testing.T) {
	enc, err := NewRSEncoder(5, 2)
	if err != nil {
		t.Fatal(err)
	}

	data := makeRandomShards(3, 100) // wrong count
	_, err = enc.Encode(data)
	if err == nil {
		t.Error("expected error for wrong data shard count")
	}
}

// --- Spec-Aligned FEC Config Test ---

func TestFECConfigParityCount(t *testing.T) {
	cfg := DefaultFECConfig()

	tests := []struct {
		lossBP    uint16 // basis points
		dataCount int
		wantMin   int
		wantMax   int
	}{
		{0, 100, 2, 2},      // < 0.5% → 2%
		{49, 100, 2, 2},     // < 0.5% → 2%
		{50, 100, 5, 5},     // 0.5% → 5%
		{199, 100, 5, 5},    // < 2% → 5%
		{200, 100, 10, 10},  // 2% → 10%
		{499, 100, 10, 10},  // < 5% → 10%
		{500, 100, 15, 15},  // 5% → 15%
		{999, 100, 15, 15},  // < 10% → 15%
		{1000, 100, 20, 20}, // 10% → 20%
		{2000, 100, 20, 20}, // > 10% → 20%
		// Tail block: small data count should still get at least TailMinParity
		{0, 3, 2, 2}, // 2% of 3 = 0, but min is 2
		{0, 1, 2, 2}, // 2% of 1 = 0, but min is 2
	}

	for _, tc := range tests {
		name := fmt.Sprintf("loss_%dbp_data_%d", tc.lossBP, tc.dataCount)
		t.Run(name, func(t *testing.T) {
			got := cfg.ParityCount(tc.dataCount, tc.lossBP)
			if got < tc.wantMin || got > tc.wantMax {
				t.Errorf("ParityCount(%d, %d) = %d, want [%d, %d]", tc.dataCount, tc.lossBP, got, tc.wantMin, tc.wantMax)
			}
		})
	}
}

// --- Benchmarks ---

func BenchmarkEncode100x10(b *testing.B) {
	enc, _ := NewRSEncoder(100, 10)
	data := makeRandomShards(100, 1376)
	b.ResetTimer()
	b.SetBytes(int64(100 * 1376))
	for i := 0; i < b.N; i++ {
		enc.Encode(data)
	}
}

func BenchmarkReconstruct100x10_5Lost(b *testing.B) {
	enc, _ := NewRSEncoder(100, 10)
	data := makeRandomShards(100, 1376)
	parity, _ := enc.Encode(data)
	b.ResetTimer()
	b.SetBytes(int64(100 * 1376))
	for i := 0; i < b.N; i++ {
		allShards := append(cloneShards(data), cloneShards(parity)...)
		// Drop 5 data shards
		for j := 0; j < 5; j++ {
			allShards[j] = nil
		}
		enc.Reconstruct(allShards)
	}
}

// --- Helpers ---

func makeRandomShards(count, size int) [][]byte {
	shards := make([][]byte, count)
	for i := range shards {
		shards[i] = make([]byte, size)
		rand.Read(shards[i])
	}
	return shards
}

func cloneShards(shards [][]byte) [][]byte {
	out := make([][]byte, len(shards))
	for i, s := range shards {
		if s != nil {
			out[i] = make([]byte, len(s))
			copy(out[i], s)
		}
	}
	return out
}
