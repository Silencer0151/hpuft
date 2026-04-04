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

	// baseSeqNum is the starting sequence number for resumed transfers.
	// For fresh transfers this is 0. For resumed transfers, sequences
	// below baseSeqNum are considered already received and are rejected.
	baseSeqNum uint64

	// highestContiguous is the largest absolute seqNum N such that all
	// slots baseSeqNum..N are present. Starts at baseSeqNum-1 (nothing
	// contiguous yet). Stored as int64 to allow -1 sentinel.
	highestContiguous int64

	// highestReceived is the largest SequenceNum we've seen inserted.
	highestReceived uint64
	hasAnyPacket    bool

	// readCursor tracks how far the disk writer has consumed (relative index).
	// Everything in [readCursor, highestContiguous-baseSeqNum] is available to read.
	readCursor uint64

	// originalTotalChunks is the total chunks for the whole file, used by
	// IsComplete and Stats. For fresh transfers this equals totalChunks.
	// For resumed transfers, totalChunks is the remaining chunk count.
	originalTotalChunks uint64

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
		data:                make([]byte, totalChunks*uint64(chunkSize)),
		present:             make([]bool, totalChunks),
		chunkSize:           chunkSize,
		totalChunks:         totalChunks,
		originalTotalChunks: totalChunks,
		highestContiguous:   -1,
		readCursor:          0,
	}
}

// NewReceiveBufferWithOffset creates a buffer for resuming a transfer.
// Only sequences from baseSeqNum onward are allocated. Sequences below
// baseSeqNum are considered already received and will be rejected by Insert.
func NewReceiveBufferWithOffset(fileSize uint64, chunkSize int, baseSeqNum uint64) *ReceiveBuffer {
	if chunkSize <= 0 {
		chunkSize = protocol.MaxPayload
	}

	originalTotal := fileSize / uint64(chunkSize)
	if fileSize%uint64(chunkSize) != 0 {
		originalTotal++
	}

	remaining := originalTotal - baseSeqNum
	if baseSeqNum >= originalTotal {
		remaining = 0
	}

	return &ReceiveBuffer{
		data:                make([]byte, remaining*uint64(chunkSize)),
		present:             make([]bool, remaining),
		chunkSize:           chunkSize,
		totalChunks:         remaining,
		originalTotalChunks: originalTotal,
		baseSeqNum:          baseSeqNum,
		highestContiguous:   int64(baseSeqNum) - 1, // all seqs before base are "present"
		readCursor:          0,
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
	if seqNum < rb.baseSeqNum {
		rb.duplicates++ // pre-resume sequence, treat as dup
		return false, nil
	}
	slot := seqNum - rb.baseSeqNum
	if slot >= rb.totalChunks {
		return false, ErrSequenceOutOfRange
	}
	if len(payload) > rb.chunkSize {
		return false, ErrPayloadTooLarge
	}

	if rb.present[slot] {
		rb.duplicates++
		return false, nil
	}

	// Place payload at exact offset
	offset := slot * uint64(rb.chunkSize)
	copy(rb.data[offset:offset+uint64(len(payload))], payload)

	// Zero-pad if payload is shorter than chunkSize (final chunk of file)
	if len(payload) < rb.chunkSize {
		end := offset + uint64(rb.chunkSize)
		for i := offset + uint64(len(payload)); i < end; i++ {
			rb.data[i] = 0
		}
	}

	rb.present[slot] = true
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
	// Convert absolute highestContiguous to a slot index.
	nextAbsolute := uint64(rb.highestContiguous + 1)
	if nextAbsolute < rb.baseSeqNum {
		nextAbsolute = rb.baseSeqNum
	}
	nextSlot := nextAbsolute - rb.baseSeqNum
	for nextSlot < rb.totalChunks && rb.present[nextSlot] {
		nextSlot++
	}
	rb.highestContiguous = int64(nextSlot+rb.baseSeqNum) - 1
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

	if rb.highestContiguous < int64(rb.baseSeqNum) || rb.readCursor > uint64(rb.highestContiguous)-rb.baseSeqNum {
		return nil
	}

	// readCursor and endSlot are slot-relative (0-based within our allocation).
	endSlot := uint64(rb.highestContiguous) - rb.baseSeqNum
	startOffset := rb.readCursor * uint64(rb.chunkSize)
	endOffset := (endSlot + 1) * uint64(rb.chunkSize)

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
	if seqNum < rb.baseSeqNum {
		return true // pre-resume sequences are considered present
	}
	slot := seqNum - rb.baseSeqNum
	if slot >= rb.totalChunks {
		return false
	}
	return rb.present[slot]
}

// MissingInRange returns all absolute sequence numbers in [start, end)
// that have not been received. Used by the heartbeat generator to build
// NACK arrays after FEC recovery has been attempted.
func (rb *ReceiveBuffer) MissingInRange(start, end uint64) []uint64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// Clamp to the range we actually manage.
	if start < rb.baseSeqNum {
		start = rb.baseSeqNum
	}
	maxAbsolute := rb.baseSeqNum + rb.totalChunks
	if end > maxAbsolute {
		end = maxAbsolute
	}

	var missing []uint64
	for i := start; i < end; i++ {
		slot := i - rb.baseSeqNum
		if !rb.present[slot] {
			missing = append(missing, i)
		}
	}
	return missing
}

// Stats returns buffer statistics. TotalChunks is the original total for the
// whole file (not the remaining count for resumed transfers).
func (rb *ReceiveBuffer) Stats() BufferStats {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return BufferStats{
		TotalChunks:       rb.originalTotalChunks,
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
	return rb.highestContiguous == int64(rb.originalTotalChunks)-1
}

// BaseSeqNum returns the starting sequence offset (0 for fresh transfers).
func (rb *ReceiveBuffer) BaseSeqNum() uint64 {
	return rb.baseSeqNum
}

// ExportBitset returns a packed bit array representing which absolute sequence
// numbers have been received, covering [0, totalChunks). Used by the checkpoint
// writer to serialize the receive state per the v5.2 sidecar format.
func (rb *ReceiveBuffer) ExportBitset(totalChunks uint64) []byte {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	bits := make([]byte, (totalChunks+7)/8)
	// All sequences below baseSeqNum are considered received (already on disk).
	for i := uint64(0); i < rb.baseSeqNum && i < totalChunks; i++ {
		bits[i/8] |= 1 << (i % 8)
	}
	// Sequences in our allocation.
	for slot := uint64(0); slot < rb.totalChunks; slot++ {
		absSeq := slot + rb.baseSeqNum
		if absSeq >= totalChunks {
			break
		}
		if rb.present[slot] {
			bits[absSeq/8] |= 1 << (absSeq % 8)
		}
	}
	return bits
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
