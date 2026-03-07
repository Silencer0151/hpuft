package receiver

import (
	"errors"
	"hpuft/protocol"
	"sync"
)

var (
	ErrBufferClosed       = errors.New("buffer: closed")
	ErrSequenceOutOfRange = errors.New("buffer: sequence number out of range")
	ErrPayloadTooLarge    = errors.New("buffer: payload exceeds chunk size")
)

// ReceiveBuffer is a fixed-size, sequence-indexed buffer that supports
// out-of-order insertion and contiguous-region reads for disk flushing.
//
// Packets are placed at offsets derived from their SequenceNum. A bitmap
// tracks which slots are filled. The buffer reports HighestContiguous
// (all slots 0..N filled) for heartbeat generation, and provides a
// ReadContiguous method for the async disk writer.
type ReceiveBuffer struct {
	mu sync.Mutex

	data    []byte // flat backing buffer: totalChunks * chunkSize bytes
	present []bool // present[i] = true if sequence i has been written

	chunkSize   int    // bytes per chunk (protocol.MaxPayload for data)
	totalChunks uint64 // total expected sequence numbers

	// highestContiguous is the largest N such that all slots 0..N are present.
	// Starts at -1 (nothing contiguous yet). Stored as int64 to allow -1 sentinel.
	highestContiguous int64

	// highestReceived is the largest SequenceNum we've seen inserted.
	highestReceived uint64
	hasAnyPacket    bool

	// readCursor tracks how far the disk writer has consumed.
	// Everything in [readCursor, highestContiguous] is available to read.
	readCursor uint64

	// stats
	packetsReceived uint64
	duplicates      uint64

	closed bool
}

// NewReceiveBuffer creates a buffer sized for the given file transfer.
//
// fileSize is the total file size in bytes. chunkSize is the payload size
// per packet (typically protocol.MaxPayload). The buffer pre-allocates the
// full backing array so that packet placement is a direct memcpy with no
// allocation.
func NewReceiveBuffer(fileSize uint64, chunkSize int) *ReceiveBuffer {
	if chunkSize <= 0 {
		chunkSize = protocol.MaxPayload
	}

	totalChunks := fileSize / uint64(chunkSize)
	if fileSize%uint64(chunkSize) != 0 {
		totalChunks++ // partial final chunk
	}

	return &ReceiveBuffer{
		data:              make([]byte, totalChunks*uint64(chunkSize)),
		present:           make([]bool, totalChunks),
		chunkSize:         chunkSize,
		totalChunks:       totalChunks,
		highestContiguous: -1,
		readCursor:        0,
	}
}

// Insert places a packet's payload at the slot determined by its SequenceNum.
// Returns true if this was a new insertion, false if it was a duplicate.
// Safe for concurrent use.
func (rb *ReceiveBuffer) Insert(seqNum uint64, payload []byte) (isNew bool, err error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.closed {
		return false, ErrBufferClosed
	}
	if seqNum >= rb.totalChunks {
		return false, ErrSequenceOutOfRange
	}
	if len(payload) > rb.chunkSize {
		return false, ErrPayloadTooLarge
	}

	if rb.present[seqNum] {
		rb.duplicates++
		return false, nil
	}

	// Place payload at exact offset
	offset := seqNum * uint64(rb.chunkSize)
	copy(rb.data[offset:offset+uint64(len(payload))], payload)

	// Zero-pad if payload is shorter than chunkSize (final chunk of file)
	if len(payload) < rb.chunkSize {
		end := offset + uint64(rb.chunkSize)
		for i := offset + uint64(len(payload)); i < end; i++ {
			rb.data[i] = 0
		}
	}

	rb.present[seqNum] = true
	rb.packetsReceived++

	// Track highest sequence number seen
	if !rb.hasAnyPacket || seqNum > rb.highestReceived {
		rb.highestReceived = seqNum
		rb.hasAnyPacket = true
	}

	// Advance highestContiguous if this insertion extends the contiguous run
	rb.advanceContiguous()

	return true, nil
}

// advanceContiguous pushes highestContiguous forward as far as possible.
// Must be called with mu held.
func (rb *ReceiveBuffer) advanceContiguous() {
	next := uint64(rb.highestContiguous + 1)
	for next < rb.totalChunks && rb.present[next] {
		next++
	}
	rb.highestContiguous = int64(next) - 1
}

// HighestContiguous returns the largest N such that all sequences 0..N
// have been received. Returns -1 if sequence 0 has not arrived yet.
func (rb *ReceiveBuffer) HighestContiguous() int64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.highestContiguous
}

// HighestReceived returns the largest SequenceNum that has been inserted.
// Returns 0 if no packets have been received.
func (rb *ReceiveBuffer) HighestReceived() uint64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.highestReceived
}

// ReadContiguous returns a slice of the buffer covering all contiguous data
// that has not yet been read (from readCursor to highestContiguous inclusive).
//
// Returns nil if no new contiguous data is available. The returned slice is
// a direct view into the backing buffer — the caller must finish using it
// before the next call to ReadContiguous or Close.
//
// After reading, call AdvanceReader to move the cursor forward.
func (rb *ReceiveBuffer) ReadContiguous() []byte {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.highestContiguous < 0 || rb.readCursor > uint64(rb.highestContiguous) {
		return nil
	}

	startOffset := rb.readCursor * uint64(rb.chunkSize)
	endOffset := (uint64(rb.highestContiguous) + 1) * uint64(rb.chunkSize)

	return rb.data[startOffset:endOffset]
}

// AdvanceReader moves the read cursor forward by n chunks.
// Called by the disk writer after successfully flushing data.
func (rb *ReceiveBuffer) AdvanceReader(n uint64) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.readCursor += n
}

// ReadCursor returns the current read cursor position.
func (rb *ReceiveBuffer) ReadCursor() uint64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.readCursor
}

// IsPresent checks whether a given sequence number has been received.
func (rb *ReceiveBuffer) IsPresent(seqNum uint64) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if seqNum >= rb.totalChunks {
		return false
	}
	return rb.present[seqNum]
}

// MissingInRange returns all sequence numbers in [start, end) that have
// not been received. Used by the heartbeat generator to build NACK arrays
// after FEC recovery has been attempted.
func (rb *ReceiveBuffer) MissingInRange(start, end uint64) []uint64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if end > rb.totalChunks {
		end = rb.totalChunks
	}

	var missing []uint64
	for i := start; i < end; i++ {
		if !rb.present[i] {
			missing = append(missing, i)
		}
	}
	return missing
}

// Stats returns buffer statistics.
func (rb *ReceiveBuffer) Stats() BufferStats {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return BufferStats{
		TotalChunks:       rb.totalChunks,
		PacketsReceived:   rb.packetsReceived,
		Duplicates:        rb.duplicates,
		HighestContiguous: rb.highestContiguous,
		ReadCursor:        rb.readCursor,
	}
}

// IsComplete returns true if all expected chunks have been received.
func (rb *ReceiveBuffer) IsComplete() bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.highestContiguous == int64(rb.totalChunks)-1
}

// Close marks the buffer as closed. Further Insert calls will return ErrBufferClosed.
func (rb *ReceiveBuffer) Close() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.closed = true
}

// BufferStats holds diagnostic counters for the receive buffer.
type BufferStats struct {
	TotalChunks       uint64
	PacketsReceived   uint64
	Duplicates        uint64
	HighestContiguous int64
	ReadCursor        uint64
}
