package receiver

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

const testChunkSize = 100 // small chunk size for readable tests

func TestNewReceiveBuffer(t *testing.T) {
	tests := []struct {
		fileSize        uint64
		chunkSize       int
		wantTotalChunks uint64
	}{
		{1000, 100, 10}, // exact fit
		{1050, 100, 11}, // partial final chunk
		{100, 100, 1},   // single chunk
		{1, 100, 1},     // tiny file, one partial chunk
		{99, 100, 1},    // just under one chunk
		{101, 100, 2},   // just over one chunk
	}

	for _, tc := range tests {
		name := fmt.Sprintf("file_%d_chunk_%d", tc.fileSize, tc.chunkSize)
		t.Run(name, func(t *testing.T) {
			rb := NewReceiveBuffer(tc.fileSize, tc.chunkSize)
			stats := rb.Stats()
			if stats.TotalChunks != tc.wantTotalChunks {
				t.Errorf("TotalChunks = %d, want %d", stats.TotalChunks, tc.wantTotalChunks)
			}
			if stats.HighestContiguous != -1 {
				t.Errorf("initial HighestContiguous = %d, want -1", stats.HighestContiguous)
			}
			if stats.PacketsReceived != 0 {
				t.Errorf("initial PacketsReceived = %d, want 0", stats.PacketsReceived)
			}
		})
	}
}

func TestInsertSequential(t *testing.T) {
	rb := NewReceiveBuffer(500, testChunkSize) // 5 chunks

	for i := uint64(0); i < 5; i++ {
		payload := makePayload(testChunkSize, byte(i))
		isNew, err := rb.Insert(i, payload)
		if err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
		if !isNew {
			t.Fatalf("Insert(%d) returned duplicate", i)
		}
		if rb.HighestContiguous() != int64(i) {
			t.Fatalf("after Insert(%d): HighestContiguous = %d, want %d", i, rb.HighestContiguous(), i)
		}
	}

	if !rb.IsComplete() {
		t.Error("buffer should be complete after all inserts")
	}
}

func TestInsertOutOfOrder(t *testing.T) {
	rb := NewReceiveBuffer(500, testChunkSize) // 5 chunks

	// Insert in reverse order: 4, 3, 2, 1, 0
	order := []uint64{4, 3, 2, 1, 0}

	for _, seq := range order[:4] {
		payload := makePayload(testChunkSize, byte(seq))
		isNew, err := rb.Insert(seq, payload)
		if err != nil {
			t.Fatalf("Insert(%d): %v", seq, err)
		}
		if !isNew {
			t.Fatalf("Insert(%d) returned duplicate", seq)
		}
		// Contiguous should still be -1 since seq 0 hasn't arrived
		if rb.HighestContiguous() != -1 {
			t.Fatalf("HighestContiguous = %d before seq 0 arrives, want -1", rb.HighestContiguous())
		}
	}

	// Insert seq 0 — should trigger contiguous advance all the way to 4
	payload := makePayload(testChunkSize, byte(0))
	isNew, err := rb.Insert(0, payload)
	if err != nil {
		t.Fatalf("Insert(0): %v", err)
	}
	if !isNew {
		t.Fatal("Insert(0) returned duplicate")
	}
	if rb.HighestContiguous() != 4 {
		t.Fatalf("HighestContiguous = %d after all inserted, want 4", rb.HighestContiguous())
	}
}

func TestInsertWithGaps(t *testing.T) {
	rb := NewReceiveBuffer(1000, testChunkSize) // 10 chunks

	// Insert 0, 1, 2, then skip 3, insert 4, 5
	for _, seq := range []uint64{0, 1, 2, 4, 5} {
		rb.Insert(seq, makePayload(testChunkSize, byte(seq)))
	}

	if rb.HighestContiguous() != 2 {
		t.Fatalf("HighestContiguous = %d, want 2 (gap at 3)", rb.HighestContiguous())
	}

	// Fill the gap
	rb.Insert(3, makePayload(testChunkSize, byte(3)))
	if rb.HighestContiguous() != 5 {
		t.Fatalf("HighestContiguous = %d after filling gap, want 5", rb.HighestContiguous())
	}
}

func TestInsertDuplicate(t *testing.T) {
	rb := NewReceiveBuffer(500, testChunkSize)

	payload := makePayload(testChunkSize, 0xAA)
	isNew, _ := rb.Insert(0, payload)
	if !isNew {
		t.Fatal("first insert should not be duplicate")
	}

	isNew, _ = rb.Insert(0, makePayload(testChunkSize, 0xBB))
	if isNew {
		t.Fatal("second insert of same seq should be duplicate")
	}

	stats := rb.Stats()
	if stats.Duplicates != 1 {
		t.Fatalf("Duplicates = %d, want 1", stats.Duplicates)
	}
	if stats.PacketsReceived != 1 {
		t.Fatalf("PacketsReceived = %d, want 1 (duplicate shouldn't count)", stats.PacketsReceived)
	}

	// Verify original data wasn't overwritten
	data := rb.ReadContiguous()
	if data[0] != 0xAA {
		t.Fatal("duplicate insert overwrote original data")
	}
}

func TestInsertOutOfRange(t *testing.T) {
	rb := NewReceiveBuffer(500, testChunkSize) // 5 chunks, valid seqs 0-4

	_, err := rb.Insert(5, makePayload(testChunkSize, 0))
	if err != ErrSequenceOutOfRange {
		t.Fatalf("Insert(5) err = %v, want ErrSequenceOutOfRange", err)
	}

	_, err = rb.Insert(100, makePayload(testChunkSize, 0))
	if err != ErrSequenceOutOfRange {
		t.Fatalf("Insert(100) err = %v, want ErrSequenceOutOfRange", err)
	}
}

func TestInsertPayloadTooLarge(t *testing.T) {
	rb := NewReceiveBuffer(500, testChunkSize)

	oversized := make([]byte, testChunkSize+1)
	_, err := rb.Insert(0, oversized)
	if err != ErrPayloadTooLarge {
		t.Fatalf("err = %v, want ErrPayloadTooLarge", err)
	}
}

func TestInsertAfterClose(t *testing.T) {
	rb := NewReceiveBuffer(500, testChunkSize)
	rb.Close()

	_, err := rb.Insert(0, makePayload(testChunkSize, 0))
	if err != ErrBufferClosed {
		t.Fatalf("Insert after Close: err = %v, want ErrBufferClosed", err)
	}
}

func TestReadContiguous(t *testing.T) {
	rb := NewReceiveBuffer(500, testChunkSize) // 5 chunks

	// Nothing to read initially
	if data := rb.ReadContiguous(); data != nil {
		t.Fatal("ReadContiguous should return nil before any inserts")
	}

	// Insert chunks 0, 1, 2
	for i := uint64(0); i < 3; i++ {
		rb.Insert(i, makePayload(testChunkSize, byte(i)))
	}

	data := rb.ReadContiguous()
	if data == nil {
		t.Fatal("ReadContiguous returned nil with contiguous data available")
	}
	if len(data) != 3*testChunkSize {
		t.Fatalf("ReadContiguous len = %d, want %d", len(data), 3*testChunkSize)
	}

	// Verify each chunk's content
	for i := 0; i < 3; i++ {
		chunk := data[i*testChunkSize : (i+1)*testChunkSize]
		expected := makePayload(testChunkSize, byte(i))
		if !bytes.Equal(chunk, expected) {
			t.Fatalf("chunk %d content mismatch", i)
		}
	}
}

func TestReadContiguousAndAdvance(t *testing.T) {
	rb := NewReceiveBuffer(500, testChunkSize) // 5 chunks

	// Insert all 5 chunks
	for i := uint64(0); i < 5; i++ {
		rb.Insert(i, makePayload(testChunkSize, byte(i)))
	}

	// Read first 3 chunks
	data := rb.ReadContiguous()
	if len(data) != 5*testChunkSize {
		t.Fatalf("first read len = %d, want %d", len(data), 5*testChunkSize)
	}

	// Advance reader by 3
	rb.AdvanceReader(3)
	if rb.ReadCursor() != 3 {
		t.Fatalf("ReadCursor = %d, want 3", rb.ReadCursor())
	}

	// Next read should return chunks 3 and 4
	data = rb.ReadContiguous()
	if len(data) != 2*testChunkSize {
		t.Fatalf("second read len = %d, want %d", len(data), 2*testChunkSize)
	}

	// Verify it's chunk 3's data
	expected := makePayload(testChunkSize, byte(3))
	if !bytes.Equal(data[:testChunkSize], expected) {
		t.Fatal("second read returned wrong data")
	}

	// Advance past everything
	rb.AdvanceReader(2)
	data = rb.ReadContiguous()
	if data != nil {
		t.Fatal("should return nil after reader catches up")
	}
}

func TestReadContiguousWithGap(t *testing.T) {
	rb := NewReceiveBuffer(500, testChunkSize) // 5 chunks

	// Insert 0, 1, skip 2, insert 3, 4
	rb.Insert(0, makePayload(testChunkSize, 0))
	rb.Insert(1, makePayload(testChunkSize, 1))
	rb.Insert(3, makePayload(testChunkSize, 3))
	rb.Insert(4, makePayload(testChunkSize, 4))

	// Only 0 and 1 are contiguous
	data := rb.ReadContiguous()
	if len(data) != 2*testChunkSize {
		t.Fatalf("read with gap: len = %d, want %d", len(data), 2*testChunkSize)
	}
}

func TestMissingInRange(t *testing.T) {
	rb := NewReceiveBuffer(1000, testChunkSize) // 10 chunks

	// Insert 0, 1, 3, 5, 7
	for _, seq := range []uint64{0, 1, 3, 5, 7} {
		rb.Insert(seq, makePayload(testChunkSize, byte(seq)))
	}

	missing := rb.MissingInRange(0, 10)
	expected := []uint64{2, 4, 6, 8, 9}
	if len(missing) != len(expected) {
		t.Fatalf("MissingInRange = %v, want %v", missing, expected)
	}
	for i, v := range missing {
		if v != expected[i] {
			t.Fatalf("MissingInRange[%d] = %d, want %d", i, v, expected[i])
		}
	}

	// Partial range
	missing = rb.MissingInRange(2, 6)
	expected = []uint64{2, 4}
	if len(missing) != len(expected) {
		t.Fatalf("MissingInRange(2,6) = %v, want %v", missing, expected)
	}
}

func TestMissingInRangeClampsToBounds(t *testing.T) {
	rb := NewReceiveBuffer(300, testChunkSize) // 3 chunks

	// Ask for range beyond total chunks
	missing := rb.MissingInRange(0, 100)
	if len(missing) != 3 {
		t.Fatalf("clamped MissingInRange: got %d missing, want 3", len(missing))
	}
}

func TestIsPresent(t *testing.T) {
	rb := NewReceiveBuffer(500, testChunkSize)

	rb.Insert(2, makePayload(testChunkSize, 2))

	if rb.IsPresent(0) {
		t.Error("seq 0 should not be present")
	}
	if !rb.IsPresent(2) {
		t.Error("seq 2 should be present")
	}
	if rb.IsPresent(99) {
		t.Error("out of range seq should not be present")
	}
}

func TestPartialFinalChunk(t *testing.T) {
	// File is 250 bytes with 100-byte chunks → 3 chunks, last is 50 bytes
	rb := NewReceiveBuffer(250, testChunkSize)

	stats := rb.Stats()
	if stats.TotalChunks != 3 {
		t.Fatalf("TotalChunks = %d, want 3", stats.TotalChunks)
	}

	// Insert a partial final chunk (50 bytes instead of 100)
	rb.Insert(0, makePayload(testChunkSize, 0))
	rb.Insert(1, makePayload(testChunkSize, 1))
	rb.Insert(2, makePayload(50, 2)) // partial

	if !rb.IsComplete() {
		t.Error("buffer should be complete")
	}

	data := rb.ReadContiguous()
	if len(data) != 3*testChunkSize { // backing buffer is always full-sized
		t.Fatalf("ReadContiguous len = %d, want %d", len(data), 3*testChunkSize)
	}

	// Verify partial chunk data is correct and padding is zero
	lastChunk := data[2*testChunkSize : 3*testChunkSize]
	for i := 0; i < 50; i++ {
		if lastChunk[i] != 2 {
			t.Fatalf("partial chunk byte %d = %d, want 2", i, lastChunk[i])
		}
	}
	for i := 50; i < testChunkSize; i++ {
		if lastChunk[i] != 0 {
			t.Fatalf("padding byte %d = %d, want 0", i, lastChunk[i])
		}
	}
}

func TestConcurrentInserts(t *testing.T) {
	numChunks := uint64(1000)
	rb := NewReceiveBuffer(numChunks*uint64(testChunkSize), testChunkSize)

	// Build all payloads upfront
	payloads := make([][]byte, numChunks)
	for i := uint64(0); i < numChunks; i++ {
		payloads[i] = makePayload(testChunkSize, byte(i%256))
	}

	// Shuffle insertion order
	order := make([]uint64, numChunks)
	for i := range order {
		order[i] = uint64(i)
	}
	rng := rand.New(rand.NewSource(42))
	rng.Shuffle(len(order), func(i, j int) {
		order[i], order[j] = order[j], order[i]
	})

	// Insert from 8 goroutines
	var wg sync.WaitGroup
	workers := 8
	perWorker := len(order) / workers

	for w := 0; w < workers; w++ {
		wg.Add(1)
		start := w * perWorker
		end := start + perWorker
		if w == workers-1 {
			end = len(order)
		}
		go func(indices []uint64) {
			defer wg.Done()
			for _, seq := range indices {
				rb.Insert(seq, payloads[seq])
			}
		}(order[start:end])
	}

	wg.Wait()

	if !rb.IsComplete() {
		t.Fatalf("buffer not complete after concurrent inserts, HighestContiguous = %d", rb.HighestContiguous())
	}

	stats := rb.Stats()
	if stats.PacketsReceived != numChunks {
		t.Fatalf("PacketsReceived = %d, want %d", stats.PacketsReceived, numChunks)
	}
	if stats.Duplicates != 0 {
		t.Fatalf("Duplicates = %d, want 0", stats.Duplicates)
	}

	// Verify all data
	data := rb.ReadContiguous()
	for i := uint64(0); i < numChunks; i++ {
		chunk := data[i*uint64(testChunkSize) : (i+1)*uint64(testChunkSize)]
		if !bytes.Equal(chunk, payloads[i]) {
			t.Fatalf("chunk %d data mismatch after concurrent inserts", i)
		}
	}
}

func TestHighestReceivedSequential(t *testing.T) {
	rb := NewReceiveBuffer(500, testChunkSize)

	// No packets yet
	if rb.HighestReceived() != 0 {
		t.Fatalf("initial HighestReceived = %d, want 0", rb.HighestReceived())
	}

	rb.Insert(0, makePayload(testChunkSize, 0))
	if rb.HighestReceived() != 0 {
		t.Fatalf("after seq 0: HighestReceived = %d, want 0", rb.HighestReceived())
	}

	rb.Insert(1, makePayload(testChunkSize, 1))
	if rb.HighestReceived() != 1 {
		t.Fatalf("after seq 1: HighestReceived = %d, want 1", rb.HighestReceived())
	}

	rb.Insert(4, makePayload(testChunkSize, 4))
	if rb.HighestReceived() != 4 {
		t.Fatalf("after seq 4: HighestReceived = %d, want 4", rb.HighestReceived())
	}
}

func TestHighestReceivedOutOfOrder(t *testing.T) {
	rb := NewReceiveBuffer(1000, testChunkSize)

	// Insert in reverse: 9, 5, 2, 7
	rb.Insert(9, makePayload(testChunkSize, 9))
	if rb.HighestReceived() != 9 {
		t.Fatalf("after seq 9: HighestReceived = %d, want 9", rb.HighestReceived())
	}

	rb.Insert(5, makePayload(testChunkSize, 5))
	if rb.HighestReceived() != 9 {
		t.Fatalf("after seq 5: HighestReceived = %d, want 9 (unchanged)", rb.HighestReceived())
	}

	rb.Insert(2, makePayload(testChunkSize, 2))
	if rb.HighestReceived() != 9 {
		t.Fatalf("after seq 2: HighestReceived = %d, want 9 (unchanged)", rb.HighestReceived())
	}

	rb.Insert(7, makePayload(testChunkSize, 7))
	if rb.HighestReceived() != 9 {
		t.Fatalf("after seq 7: HighestReceived = %d, want 9 (unchanged)", rb.HighestReceived())
	}
}

func TestHighestReceivedDuplicateNoRegression(t *testing.T) {
	rb := NewReceiveBuffer(500, testChunkSize)

	rb.Insert(4, makePayload(testChunkSize, 4))
	if rb.HighestReceived() != 4 {
		t.Fatalf("after seq 4: HighestReceived = %d, want 4", rb.HighestReceived())
	}

	// Duplicate insert of a lower seq should not change HighestReceived
	rb.Insert(2, makePayload(testChunkSize, 2))
	rb.Insert(2, makePayload(testChunkSize, 2)) // duplicate
	if rb.HighestReceived() != 4 {
		t.Fatalf("after dup: HighestReceived = %d, want 4", rb.HighestReceived())
	}
}

// --- Benchmarks ---

func BenchmarkInsertSequential(b *testing.B) {
	payload := makePayload(1376, 0xAA)
	numChunks := uint64(10000)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		rb := NewReceiveBuffer(numChunks*1376, 1376)
		for i := uint64(0); i < numChunks; i++ {
			rb.Insert(i, payload)
		}
	}
}

func BenchmarkInsertRandom(b *testing.B) {
	payload := makePayload(1376, 0xAA)
	numChunks := uint64(10000)
	rng := rand.New(rand.NewSource(42))
	order := make([]uint64, numChunks)
	for i := range order {
		order[i] = uint64(i)
	}
	rng.Shuffle(len(order), func(i, j int) {
		order[i], order[j] = order[j], order[i]
	})

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		rb := NewReceiveBuffer(numChunks*1376, 1376)
		for _, seq := range order {
			rb.Insert(seq, payload)
		}
	}
}

// --- Helpers ---

func makePayload(size int, fill byte) []byte {
	p := make([]byte, size)
	for i := range p {
		p[i] = fill
	}
	return p
}
