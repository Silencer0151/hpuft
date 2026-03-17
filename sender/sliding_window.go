package sender

import "sync"

// windowSize is the maximum number of unacknowledged packets the sender will
// keep in flight before blocking the main send loop. At 1368 bytes per payload
// this caps memory usage at ~68 MB while still allowing a full send window on
// any realistic RTT/bandwidth product.
const windowSize uint64 = 50_000

// unsetHC is the sentinel value for SlidingWindow.hc before any Advance call.
const unsetHC = ^uint64(0) // math.MaxUint64

// SlidingWindow is a fixed-capacity ring-buffer cache of sent payload chunks.
// It replaces the unbounded map[uint64][]byte previously used for NACK
// retransmission. As the receiver's HighestContiguous frontier advances, old
// slots are evicted, bounding peak memory regardless of loss or stall.
//
// Concurrency: Store and Advance may be called from different goroutines
// (main send loop vs. heartbeat listener). All public methods are safe for
// concurrent use.
type SlidingWindow struct {
	mu    sync.RWMutex
	slots [][]byte
	size  uint64
	hc    uint64 // highest contiguous seq evicted (inclusive); unsetHC if never advanced
}

// NewSlidingWindow allocates a ring buffer that can hold size unacknowledged
// payload chunks simultaneously.
func NewSlidingWindow(size uint64) *SlidingWindow {
	return &SlidingWindow{
		slots: make([][]byte, size),
		size:  size,
		hc:    unsetHC,
	}
}

// Store saves a copy of data at position seq. It is called from the main send
// loop immediately after transmitting a new packet.
func (sw *SlidingWindow) Store(seq uint64, data []byte) {
	buf := make([]byte, len(data))
	copy(buf, data)
	sw.mu.Lock()
	sw.slots[seq%sw.size] = buf
	sw.mu.Unlock()
}

// Load retrieves the stored payload for seq. Returns nil, false if the slot
// has been evicted (seq <= HighestContiguous) or was never stored.
func (sw *SlidingWindow) Load(seq uint64) ([]byte, bool) {
	sw.mu.RLock()
	defer sw.mu.RUnlock()
	if sw.hc != unsetHC && seq <= sw.hc {
		return nil, false
	}
	chunk := sw.slots[seq%sw.size]
	if chunk == nil {
		return nil, false
	}
	return chunk, true
}

// Advance evicts all slots with sequence number <= highestContiguous. It is
// called from the heartbeat listener goroutine whenever HighestContiguous
// increases. Mirrors the existing guard: calls with highestContiguous == 0 are
// no-ops to preserve seq-0 retransmit availability until HC genuinely advances.
func (sw *SlidingWindow) Advance(highestContiguous uint64) {
	if highestContiguous == 0 {
		return
	}
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if sw.hc != unsetHC && highestContiguous <= sw.hc {
		return // already evicted up to or past this point
	}
	var start uint64
	if sw.hc == unsetHC {
		start = 0 // first advance: evict from seq 0
	} else {
		start = sw.hc + 1
	}
	for seq := start; seq <= highestContiguous; seq++ {
		sw.slots[seq%sw.size] = nil
	}
	sw.hc = highestContiguous
}

// IsFull returns true when adding nextSeq would exceed the window capacity.
// The main send loop calls this to apply backpressure: block until the receiver
// acknowledges enough packets to open a slot.
//
// When hc == unsetHC (math.MaxUint64), the expression hc+size+1 wraps to size
// via uint64 overflow, giving the correct limit of [0, size).
func (sw *SlidingWindow) IsFull(nextSeq uint64) bool {
	sw.mu.RLock()
	full := nextSeq >= sw.hc+sw.size+1
	sw.mu.RUnlock()
	return full
}
