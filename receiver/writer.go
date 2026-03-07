package receiver

import (
	"encoding/binary"
	"hash"
	"io"
	"os"
	"sync"

	"github.com/cespare/xxhash/v2"
)

// DONE: Replaced hash/crc64 with xxHash64 (github.com/cespare/xxhash/v2)
// for spec compliance. The streaming interface is identical — swap the
// constructor and the protocol will behave the same. Used crc64 as a
// placeholder since we couldn't pull external deps in the oldt build env.

//var crc64Table = crc64.MakeTable(crc64.ECMA)

// DiskWriter handles asynchronous flushing of contiguous buffer regions
// to disk with pipelined hash computation. It reads from a ReceiveBuffer
// and writes to a file, updating a streaming hash as data is flushed.
type DiskWriter struct {
	mu sync.Mutex

	buf      *ReceiveBuffer
	file     *os.File
	hasher   hash.Hash64
	filePath string

	// bytesWritten tracks total bytes flushed to disk
	bytesWritten uint64

	// fileSize is the expected total file size (for truncating padding on the final chunk)
	fileSize uint64

	// chunkSize is the size of each buffer slot
	chunkSize int

	// done is closed when the writer has flushed all data and the hash is final
	done chan struct{}

	// finalHash stores the computed hash after completion
	finalHash uint64
	hashReady bool

	// err stores any write error
	err error
}

// NewDiskWriter creates a writer that will flush data from buf to outputPath.
func NewDiskWriter(buf *ReceiveBuffer, outputPath string, fileSize uint64, chunkSize int) (*DiskWriter, error) {
	f, err := os.Create(outputPath)
	if err != nil {
		return nil, err
	}

	return &DiskWriter{
		buf:       buf,
		file:      f,
		hasher:    xxhash.New(),
		filePath:  outputPath,
		fileSize:  fileSize,
		chunkSize: chunkSize,
		done:      make(chan struct{}),
	}, nil
}

// Flush reads any new contiguous data from the buffer and writes it to disk,
// updating the streaming hash. Returns the number of bytes written in this
// call. This is meant to be called periodically (e.g., on a timer or after
// each heartbeat interval).
//
// Safe for concurrent use, but typically called from a single goroutine.
func (dw *DiskWriter) Flush() (int, error) {
	dw.mu.Lock()
	defer dw.mu.Unlock()

	if dw.err != nil {
		return 0, dw.err
	}

	data := dw.buf.ReadContiguous()
	if len(data) == 0 {
		return 0, nil
	}

	// Calculate how much of this data is actual file content vs padding
	writeLen := len(data)
	remaining := dw.fileSize - dw.bytesWritten
	if uint64(writeLen) > remaining {
		writeLen = int(remaining)
	}

	toWrite := data[:writeLen]

	// Write to file
	n, err := dw.file.Write(toWrite)
	if err != nil {
		dw.err = err
		return n, err
	}

	// Update streaming hash
	dw.hasher.Write(toWrite)

	dw.bytesWritten += uint64(n)

	// Advance the buffer reader by the number of complete chunks consumed
	chunksConsumed := uint64(len(data)) / uint64(dw.chunkSize)
	dw.buf.AdvanceReader(chunksConsumed)

	return n, nil
}

// Finalize performs the final flush, computes the hash, and closes the file.
// Returns the computed hash. Should be called after the buffer reports IsComplete.
func (dw *DiskWriter) Finalize() (uint64, error) {
	dw.mu.Lock()
	defer dw.mu.Unlock()

	if dw.hashReady {
		return dw.finalHash, nil
	}

	// Final flush of any remaining data
	data := dw.buf.ReadContiguous()
	if len(data) > 0 {
		remaining := dw.fileSize - dw.bytesWritten
		writeLen := len(data)
		if uint64(writeLen) > remaining {
			writeLen = int(remaining)
		}

		if writeLen > 0 {
			n, err := dw.file.Write(data[:writeLen])
			if err != nil {
				dw.err = err
				return 0, err
			}
			dw.hasher.Write(data[:writeLen])
			dw.bytesWritten += uint64(n)
		}

		chunksConsumed := uint64(len(data)) / uint64(dw.chunkSize)
		dw.buf.AdvanceReader(chunksConsumed)
	}

	// Sync to disk
	if err := dw.file.Sync(); err != nil {
		dw.err = err
		return 0, err
	}

	dw.finalHash = dw.hasher.Sum64()
	dw.hashReady = true
	close(dw.done)

	return dw.finalHash, nil
}

// BytesWritten returns the total bytes flushed to disk so far.
func (dw *DiskWriter) BytesWritten() uint64 {
	dw.mu.Lock()
	defer dw.mu.Unlock()
	return dw.bytesWritten
}

// Err returns any error that occurred during writing.
func (dw *DiskWriter) Err() error {
	dw.mu.Lock()
	defer dw.mu.Unlock()
	return dw.err
}

// Close closes the underlying file. Should be called after Finalize or on error cleanup.
func (dw *DiskWriter) Close() error {
	return dw.file.Close()
}

// Done returns a channel that is closed when Finalize completes.
func (dw *DiskWriter) Done() <-chan struct{} {
	return dw.done
}

// --- Utility: compute hash of an entire file (for sender side) ---

// HashFile computes the 64-bit hash of the file at the given path.
// Uses the same hash algorithm as DiskWriter for consistency.
func HashFile(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	h := xxhash.New()
	if _, err := io.Copy(h, f); err != nil {
		return 0, err
	}

	return h.Sum64(), nil
}

// HashBytes computes the 64-bit hash of a byte slice.
func HashBytes(data []byte) uint64 {
	h := xxhash.New()
	h.Write(data)
	return h.Sum64()
}

// HashUint64ToBytes converts a uint64 hash to big-endian bytes.
func HashUint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
