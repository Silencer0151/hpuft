package receiver

import (
	"hpuft/protocol"
	"log"
	"sync"
)

// BlockDecoder tracks received data and parity packets per block group
// and attempts Reed-Solomon reconstruction when enough packets arrive.
type BlockDecoder struct {
	mu sync.Mutex

	blockSize int
	blocks    map[uint64]*blockState
	buf       *ReceiveBuffer

	// Stats
	blocksRecovered int
	shardsRecovered int
	blocksComplete  int
	blocksFailed    int
}

// blockState tracks the data and parity shards for a single block group.
type blockState struct {
	blockGroup  uint64
	dataCount   int // expected data shards (set from parity metadata or inferred)
	parityCount int // expected parity shards (set from parity metadata)

	// dataPresent[i] = true if data shard i (relative to block start) was received
	dataPresent map[int]bool
	// dataShards[i] = padded payload for data shard i
	dataShards map[int][]byte

	// parityShards[i] = raw parity shard data (without metadata header)
	parityPresent map[int]bool
	parityShards  map[int][]byte

	// Whether we've already attempted/completed reconstruction for this block
	reconstructed bool
}

// NewBlockDecoder creates a FEC block decoder.
func NewBlockDecoder(blockSize int, buf *ReceiveBuffer) *BlockDecoder {
	return &BlockDecoder{
		blockSize: blockSize,
		blocks:    make(map[uint64]*blockState),
		buf:       buf,
	}
}

// RecordData records a received data packet for FEC tracking.
// The actual insertion into the receive buffer is done by the caller.
func (bd *BlockDecoder) RecordData(blockGroup uint64, seqNum uint64, payload []byte) {
	bd.mu.Lock()
	defer bd.mu.Unlock()

	bs := bd.getOrCreateBlock(blockGroup)
	if bs.reconstructed {
		return
	}

	// Index within the block
	blockStart := blockGroup * uint64(bd.blockSize)
	idx := int(seqNum - blockStart)

	if bs.dataPresent[idx] {
		return // duplicate
	}

	// Pad to MaxPayload for RS compatibility
	padded := make([]byte, protocol.MaxPayload)
	copy(padded, payload)

	bs.dataPresent[idx] = true
	bs.dataShards[idx] = padded
}

// RecordParity records a received parity packet.
// parityIdx is the parity index within the block (from SequenceNum).
// payload is the raw parity packet payload including the 4-byte metadata header.
func (bd *BlockDecoder) RecordParity(blockGroup uint64, parityIdx int, payload []byte) {
	bd.mu.Lock()
	defer bd.mu.Unlock()

	bs := bd.getOrCreateBlock(blockGroup)
	if bs.reconstructed {
		return
	}

	if len(payload) < 4 {
		return // malformed
	}

	// Parse metadata from payload header
	dataCount := int(payload[0])<<8 | int(payload[1])
	parityCount := int(payload[2])<<8 | int(payload[3])
	shardData := payload[4:]

	// Update block metadata (first parity packet sets it, subsequent ones should match)
	if bs.dataCount == 0 {
		bs.dataCount = dataCount
		bs.parityCount = parityCount
	}

	if bs.parityPresent[parityIdx] {
		return // duplicate
	}

	// Copy shard data (pad to MaxPayload if needed)
	padded := make([]byte, protocol.MaxPayload)
	copy(padded, shardData)

	bs.parityPresent[parityIdx] = true
	bs.parityShards[parityIdx] = padded
}

// TryReconstruct attempts FEC reconstruction for the given block group.
// Returns a list of (seqNum, payload) pairs for any recovered data shards.
// These should be inserted into the receive buffer by the caller.
func (bd *BlockDecoder) TryReconstruct(blockGroup uint64) []RecoveredShard {
	bd.mu.Lock()
	defer bd.mu.Unlock()

	bs, ok := bd.blocks[blockGroup]
	if !ok || bs.reconstructed {
		return nil
	}

	// Need metadata from at least one parity packet to know the parameters
	if bs.dataCount == 0 || bs.parityCount == 0 {
		return nil
	}

	dataPresent := len(bs.dataPresent)
	parityPresent := len(bs.parityPresent)
	totalPresent := dataPresent + parityPresent

	// Need at least dataCount shards total to reconstruct
	if totalPresent < bs.dataCount {
		return nil
	}

	// If all data is already present, nothing to recover
	if dataPresent == bs.dataCount {
		bs.reconstructed = true
		bd.blocksComplete++
		return nil
	}

	// Build the shard array for RS reconstruction
	// Indices 0..dataCount-1 are data, dataCount..dataCount+parityCount-1 are parity
	totalShards := bs.dataCount + bs.parityCount
	shards := make([][]byte, totalShards)

	// Place data shards
	for i := 0; i < bs.dataCount; i++ {
		if bs.dataPresent[i] {
			shards[i] = bs.dataShards[i]
		}
		// nil = missing
	}

	// Place parity shards
	for i := 0; i < bs.parityCount; i++ {
		if bs.parityPresent[i] {
			shards[bs.dataCount+i] = bs.parityShards[i]
		}
	}

	// Create RS encoder with matching parameters
	enc, err := protocol.NewRSEncoder(bs.dataCount, bs.parityCount)
	if err != nil {
		log.Printf("[fec-decoder] RS encoder creation failed (k=%d, m=%d): %v",
			bs.dataCount, bs.parityCount, err)
		bd.blocksFailed++
		return nil
	}

	// Attempt reconstruction
	if err := enc.Reconstruct(shards); err != nil {
		log.Printf("[fec-decoder] block %d reconstruction failed: %v", blockGroup, err)
		bd.blocksFailed++
		return nil
	}

	// Collect recovered data shards
	blockStart := blockGroup * uint64(bd.blockSize)
	var recovered []RecoveredShard

	for i := 0; i < bs.dataCount; i++ {
		if !bs.dataPresent[i] {
			seqNum := blockStart + uint64(i)
			recovered = append(recovered, RecoveredShard{
				SeqNum:  seqNum,
				Payload: shards[i], // this is MaxPayload-sized (padded)
			})
		}
	}

	bs.reconstructed = true
	bd.blocksRecovered++
	bd.shardsRecovered += len(recovered)

	log.Printf("[fec-decoder] block %d: recovered %d/%d missing shards",
		blockGroup, len(recovered), bs.dataCount-dataPresent)

	return recovered
}

// UnrecoverableInBlock returns sequence numbers within the block that are
// still missing after reconstruction was attempted (or if not enough parity
// is available). Used by the heartbeat to generate NACKs.
func (bd *BlockDecoder) UnrecoverableInBlock(blockGroup uint64) []uint64 {
	bd.mu.Lock()
	defer bd.mu.Unlock()

	bs, ok := bd.blocks[blockGroup]
	if !ok {
		return nil
	}

	// If already reconstructed successfully, nothing is unrecoverable
	if bs.reconstructed {
		return nil
	}

	// If we don't have metadata yet, we can't determine what's missing
	if bs.dataCount == 0 {
		return nil
	}

	blockStart := blockGroup * uint64(bd.blockSize)
	var missing []uint64
	for i := 0; i < bs.dataCount; i++ {
		if !bs.dataPresent[i] {
			missing = append(missing, blockStart+uint64(i))
		}
	}
	return missing
}

// Stats returns FEC decoder statistics.
func (bd *BlockDecoder) Stats() BlockDecoderStats {
	bd.mu.Lock()
	defer bd.mu.Unlock()
	return BlockDecoderStats{
		BlocksRecovered: bd.blocksRecovered,
		ShardsRecovered: bd.shardsRecovered,
		BlocksComplete:  bd.blocksComplete,
		BlocksFailed:    bd.blocksFailed,
		ActiveBlocks:    len(bd.blocks),
	}
}

// CleanupBefore removes tracking state for all block groups before the given one.
// Called periodically to free memory as blocks are completed.
func (bd *BlockDecoder) CleanupBefore(blockGroup uint64) {
	bd.mu.Lock()
	defer bd.mu.Unlock()
	for bg := range bd.blocks {
		if bg < blockGroup {
			delete(bd.blocks, bg)
		}
	}
}

func (bd *BlockDecoder) getOrCreateBlock(blockGroup uint64) *blockState {
	bs, ok := bd.blocks[blockGroup]
	if !ok {
		bs = &blockState{
			blockGroup:    blockGroup,
			dataPresent:   make(map[int]bool),
			dataShards:    make(map[int][]byte),
			parityPresent: make(map[int]bool),
			parityShards:  make(map[int][]byte),
		}
		bd.blocks[blockGroup] = bs
	}
	return bs
}

// RecoveredShard holds a reconstructed data payload and its sequence number.
type RecoveredShard struct {
	SeqNum  uint64
	Payload []byte // MaxPayload-sized (padded)
}

// BlockDecoderStats holds FEC decoder statistics.
type BlockDecoderStats struct {
	BlocksRecovered int
	ShardsRecovered int
	BlocksComplete  int
	BlocksFailed    int
	ActiveBlocks    int
}
