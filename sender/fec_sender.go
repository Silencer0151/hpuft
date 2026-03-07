package sender

import (
	"hpuft/protocol"
	"log"
	"sync"
)

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
//
// The returned ParityResult contains ready-to-send parity payloads with
// embedded metadata (dataCount, parityCount) in the first 4 bytes.
func (be *BlockEncoder) AddShard(blockGroup uint64, payload []byte) *ParityResult {
	be.mu.Lock()
	defer be.mu.Unlock()

	// If we've moved to a new block group, the old one was a tail block
	// that should have been flushed with FlushTail
	if blockGroup != be.currentBlock && len(be.shards) > 0 {
		log.Printf("[fec-encoder] WARNING: block %d had %d shards when block %d started (auto-flushing)",
			be.currentBlock, len(be.shards), blockGroup)
		be.shards = be.shards[:0]
	}
	be.currentBlock = blockGroup

	// Pad to MaxPayload for encoding (RS needs equal-length shards)
	padded := make([]byte, protocol.MaxPayload)
	copy(padded, payload)
	be.shards = append(be.shards, padded)
	be.shardLengths = append(be.shardLengths, len(payload))

	if len(be.shards) >= be.blockSize {
		result := be.encodeBlock()
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
	be.shards = be.shards[:0]
	be.shardLengths = be.shardLengths[:0]
	return result
}

// encodeBlock runs RS encoding on the accumulated shards.
// Must be called with mu held.
func (be *BlockEncoder) encodeBlock() *ParityResult {
	dataCount := len(be.shards)
	parityCount := be.fecCfg.ParityCount(dataCount, be.currentLossBP)

	if dataCount == 0 {
		return nil
	}

	enc, err := protocol.NewRSEncoder(dataCount, parityCount)
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
