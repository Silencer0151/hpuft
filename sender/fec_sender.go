package sender

import (
	"hpuft/protocol"
	"log"
	"sync"
)

// shardPool recycles MaxPayload-sized byte slices for FEC shard padding.
// AddShard does make([]byte, MaxPayload) per data packet — at 35 MB/s that's
// ~25K allocs/sec (~470 MB heap churn for a 237 MB file). The pool drops
// heap allocations for the FEC encode path to near zero.
var shardPool = sync.Pool{
	New: func() any {
		b := make([]byte, protocol.MaxPayload)
		return &b
	},
}

// rsEncoderCache stores RSEncoder instances keyed by (dataShards<<16 | parityShards).
//
// Building a Vandermonde matrix and inverting it is O(k³) GF(2^8) operations.
// For k=100 that is ~2 million GF ops, measured at ~4 ms per call. With 1720
// FEC blocks in a 237 MB transfer, constructing a fresh encoder each time
// costs ~6.9 seconds — roughly equal to the entire transfer time at 35 MB/s.
//
// The matrix is deterministic for a given (k, m) pair and Encode() only reads
// from parityMatrix (no mutation), so the cached encoder is safe for concurrent
// use across goroutines.
var rsEncoderCache sync.Map // key: uint32 → *protocol.RSEncoder

// cachedRSEncoder returns a cached RSEncoder for the given (k, m) pair,
// building and storing one on the first call.
func cachedRSEncoder(dataShards, parityShards int) (*protocol.RSEncoder, error) {
	key := uint32(dataShards)<<16 | uint32(parityShards)
	if v, ok := rsEncoderCache.Load(key); ok {
		return v.(*protocol.RSEncoder), nil
	}
	enc, err := protocol.NewRSEncoder(dataShards, parityShards)
	if err != nil {
		return nil, err
	}
	// LoadOrStore: if two goroutines race, one encoder is discarded (both correct).
	actual, _ := rsEncoderCache.LoadOrStore(key, enc)
	return actual.(*protocol.RSEncoder), nil
}

// BlockEncoder accumulates data payloads per block group and generates
// Reed-Solomon parity packets when a block is complete.
type BlockEncoder struct {
	mu sync.Mutex

	fecCfg    protocol.FECConfig
	blockSize int

	// Current parity ratio, updated from heartbeat loss metrics
	currentLossBP uint16

	// Accumulated data shards for the current block
	currentBlock uint64
	shards       [][]byte
	shardLengths []int // actual length of each shard (before padding)
}

// NewBlockEncoder creates a FEC block encoder.
func NewBlockEncoder(fecCfg protocol.FECConfig) *BlockEncoder {
	return &BlockEncoder{
		fecCfg:    fecCfg,
		blockSize: fecCfg.BlockSize,
		shards:    make([][]byte, 0, fecCfg.BlockSize),
	}
}

// UpdateLossRate sets the current observed loss rate for adaptive parity.
func (be *BlockEncoder) UpdateLossRate(lossBP uint16) {
	be.mu.Lock()
	defer be.mu.Unlock()
	be.currentLossBP = lossBP
}

// AddShard adds a data payload to the current block. If this completes
// the block (reaches BlockSize), it generates parity and returns them.
// Returns nil if the block is not yet full.
func (be *BlockEncoder) AddShard(blockGroup uint64, payload []byte) *ParityResult {
	be.mu.Lock()
	defer be.mu.Unlock()

	if blockGroup != be.currentBlock && len(be.shards) > 0 {
		log.Printf("[fec-encoder] WARNING: block %d had %d shards when block %d started (auto-flushing)",
			be.currentBlock, len(be.shards), blockGroup)
		be.returnShards()
	}
	be.currentBlock = blockGroup

	// Get a pooled buffer and copy the payload into it.
	// Pad to MaxPayload (RS needs equal-length shards); the pool recycles
	// the buffer after encodeBlock() returns.
	bp := shardPool.Get().(*[]byte)
	padded := (*bp)[:protocol.MaxPayload]
	copy(padded, payload)
	clear(padded[len(payload):]) // zero-pad remainder (Go 1.21+)
	be.shards = append(be.shards, padded)
	be.shardLengths = append(be.shardLengths, len(payload))

	if len(be.shards) >= be.blockSize {
		result := be.encodeBlock()
		be.returnShards()
		be.shards = be.shards[:0]
		be.shardLengths = be.shardLengths[:0]
		return result
	}

	return nil
}

// FlushTail generates parity for the final (possibly partial) block.
// Must be called after the last data packet. Returns nil if no shards
// are buffered.
func (be *BlockEncoder) FlushTail() *ParityResult {
	be.mu.Lock()
	defer be.mu.Unlock()

	if len(be.shards) == 0 {
		return nil
	}

	result := be.encodeBlock()
	be.returnShards()
	be.shards = be.shards[:0]
	be.shardLengths = be.shardLengths[:0]
	return result
}

// returnShards returns pooled shard buffers back to the pool.
// Must be called with mu held, after encodeBlock() has consumed the shards.
func (be *BlockEncoder) returnShards() {
	for i, s := range be.shards {
		bp := &s
		shardPool.Put(bp)
		be.shards[i] = nil // clear pointer to avoid retaining
	}
}

// encodeBlock runs RS encoding on the accumulated shards using a cached encoder.
// Must be called with mu held. Does NOT return shards to pool (caller handles that).
func (be *BlockEncoder) encodeBlock() *ParityResult {
	dataCount := len(be.shards)
	parityCount := be.fecCfg.ParityCount(dataCount, be.currentLossBP)

	if dataCount == 0 {
		return nil
	}

	// Use cached encoder — avoids rebuilding the Vandermonde matrix every block.
	enc, err := cachedRSEncoder(dataCount, parityCount)
	if err != nil {
		log.Printf("[fec-encoder] RS encoder creation failed (k=%d, m=%d): %v",
			dataCount, parityCount, err)
		return nil
	}

	parityShards, err := enc.Encode(be.shards)
	if err != nil {
		log.Printf("[fec-encoder] RS encode failed: %v", err)
		return nil
	}

	// Build parity payloads with metadata header:
	//   [0:2] uint16 dataCount
	//   [2:4] uint16 parityCount
	//   [4:]  parity shard data
	payloads := make([][]byte, parityCount)
	for i, shard := range parityShards {
		payload := make([]byte, 4+len(shard))
		payload[0] = byte(dataCount >> 8)
		payload[1] = byte(dataCount)
		payload[2] = byte(parityCount >> 8)
		payload[3] = byte(parityCount)
		copy(payload[4:], shard)
		payloads[i] = payload
	}

	return &ParityResult{
		BlockGroup:  be.currentBlock,
		DataCount:   dataCount,
		ParityCount: parityCount,
		Payloads:    payloads,
	}
}

// ParityResult holds generated parity packets for a completed block.
type ParityResult struct {
	BlockGroup  uint64
	DataCount   int
	ParityCount int
	Payloads    [][]byte // each payload includes 4-byte metadata header + parity shard
}
