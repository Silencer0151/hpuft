package receiver

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestWriterBasicFlushAndHash(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "out.bin")

	// Create a known data pattern: 3 full chunks
	chunkSize := 100
	fileSize := uint64(300)
	rb := NewReceiveBuffer(fileSize, chunkSize)

	data := make([]byte, 300)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Insert all 3 chunks
	for i := 0; i < 3; i++ {
		rb.Insert(uint64(i), data[i*chunkSize:(i+1)*chunkSize])
	}

	writer, err := NewDiskWriter(rb, outPath, fileSize, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	hash, err := writer.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// Verify file content
	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("file content mismatch")
	}

	// Verify hash matches direct computation
	expectedHash := HashBytes(data)
	if hash != expectedHash {
		t.Fatalf("hash = 0x%016X, want 0x%016X", hash, expectedHash)
	}
}

func TestWriterPartialFinalChunk(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "partial.bin")

	chunkSize := 100
	fileSize := uint64(250) // 2 full chunks + 50 byte partial
	rb := NewReceiveBuffer(fileSize, chunkSize)

	data := make([]byte, 250)
	rand.Read(data)

	// Insert 2 full chunks and 1 partial
	rb.Insert(0, data[0:100])
	rb.Insert(1, data[100:200])
	rb.Insert(2, data[200:250]) // only 50 bytes

	writer, err := NewDiskWriter(rb, outPath, fileSize, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	hash, err := writer.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// Verify file is exactly 250 bytes, not 300
	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 250 {
		t.Fatalf("file size = %d, want 250", len(got))
	}
	if !bytes.Equal(got, data) {
		t.Fatal("file content mismatch")
	}

	expectedHash := HashBytes(data)
	if hash != expectedHash {
		t.Fatalf("hash = 0x%016X, want 0x%016X", hash, expectedHash)
	}
}

func TestWriterIncrementalFlush(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "incremental.bin")

	chunkSize := 100
	fileSize := uint64(500)
	rb := NewReceiveBuffer(fileSize, chunkSize)

	data := make([]byte, 500)
	rand.Read(data)

	writer, err := NewDiskWriter(rb, outPath, fileSize, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	// Insert first 2 chunks and flush
	rb.Insert(0, data[0:100])
	rb.Insert(1, data[100:200])

	n, err := writer.Flush()
	if err != nil {
		t.Fatalf("first flush: %v", err)
	}
	if n != 200 {
		t.Fatalf("first flush wrote %d bytes, want 200", n)
	}
	if writer.BytesWritten() != 200 {
		t.Fatalf("BytesWritten = %d, want 200", writer.BytesWritten())
	}

	// Flush with nothing new — should write 0
	n, err = writer.Flush()
	if err != nil {
		t.Fatalf("empty flush: %v", err)
	}
	if n != 0 {
		t.Fatalf("empty flush wrote %d bytes, want 0", n)
	}

	// Insert remaining chunks
	for i := 2; i < 5; i++ {
		rb.Insert(uint64(i), data[i*chunkSize:(i+1)*chunkSize])
	}

	// Finalize should flush the rest and produce correct hash
	hash, err := writer.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("file content mismatch after incremental flush")
	}

	expectedHash := HashBytes(data)
	if hash != expectedHash {
		t.Fatalf("hash = 0x%016X, want 0x%016X", hash, expectedHash)
	}
}

func TestWriterOutOfOrderInsertThenFlush(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "ooo.bin")

	chunkSize := 100
	fileSize := uint64(400)
	rb := NewReceiveBuffer(fileSize, chunkSize)

	data := make([]byte, 400)
	rand.Read(data)

	writer, err := NewDiskWriter(rb, outPath, fileSize, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	// Insert out of order: 3, 1, 0, 2
	rb.Insert(3, data[300:400])
	rb.Insert(1, data[100:200])

	// Flush — nothing contiguous yet (seq 0 missing)
	n, _ := writer.Flush()
	if n != 0 {
		t.Fatalf("flush before seq 0: wrote %d, want 0", n)
	}

	rb.Insert(0, data[0:100])
	// Now 0, 1 are contiguous (gap at 2)
	n, _ = writer.Flush()
	if n != 200 {
		t.Fatalf("flush after seq 0: wrote %d, want 200", n)
	}

	rb.Insert(2, data[200:300])
	// Now 0-3 all contiguous

	hash, err := writer.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("file content mismatch")
	}

	expectedHash := HashBytes(data)
	if hash != expectedHash {
		t.Fatalf("hash = 0x%016X, want 0x%016X", hash, expectedHash)
	}
}

func TestWriterFinalizeIdempotent(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "idem.bin")

	chunkSize := 100
	fileSize := uint64(100)
	rb := NewReceiveBuffer(fileSize, chunkSize)
	rb.Insert(0, make([]byte, 100))

	writer, err := NewDiskWriter(rb, outPath, fileSize, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	hash1, err := writer.Finalize()
	if err != nil {
		t.Fatal(err)
	}

	hash2, err := writer.Finalize()
	if err != nil {
		t.Fatal(err)
	}

	if hash1 != hash2 {
		t.Fatalf("second Finalize returned different hash: 0x%X vs 0x%X", hash1, hash2)
	}
}

func TestWriterBytesWrittenTracking(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "tracking.bin")

	chunkSize := 50
	fileSize := uint64(200)
	rb := NewReceiveBuffer(fileSize, chunkSize)

	writer, err := NewDiskWriter(rb, outPath, fileSize, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	if writer.BytesWritten() != 0 {
		t.Fatalf("initial BytesWritten = %d, want 0", writer.BytesWritten())
	}

	// Insert and flush chunk by chunk
	for i := 0; i < 4; i++ {
		chunk := make([]byte, 50)
		chunk[0] = byte(i)
		rb.Insert(uint64(i), chunk)
		writer.Flush()

		expected := uint64((i + 1) * 50)
		if writer.BytesWritten() != expected {
			t.Fatalf("after chunk %d: BytesWritten = %d, want %d", i, writer.BytesWritten(), expected)
		}
	}
}

func TestHashFileConsistency(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hashtest.bin")

	data := make([]byte, 4096)
	rand.Read(data)
	os.WriteFile(path, data, 0644)

	fileHash, err := HashFile(path)
	if err != nil {
		t.Fatal(err)
	}

	memHash := HashBytes(data)
	if fileHash != memHash {
		t.Fatalf("HashFile=0x%016X != HashBytes=0x%016X", fileHash, memHash)
	}
}

func TestWriterSingleByteFile(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "tiny.bin")

	chunkSize := 100
	fileSize := uint64(1)
	rb := NewReceiveBuffer(fileSize, chunkSize)

	data := []byte{0x42}
	rb.Insert(0, data)

	writer, err := NewDiskWriter(rb, outPath, fileSize, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	hash, err := writer.Finalize()
	if err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 0x42 {
		t.Fatalf("got %v, want [0x42]", got)
	}

	if hash != HashBytes(data) {
		t.Fatal("hash mismatch for single byte file")
	}
}
