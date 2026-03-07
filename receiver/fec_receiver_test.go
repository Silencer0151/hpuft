package receiver_test

import (
	"bytes"
	"crypto/rand"
	"hpuft/protocol"
	"hpuft/receiver"
	"hpuft/sender"
	"testing"
)

func TestFECEndToEndNoLoss(t *testing.T) {
	blockSize := 10
	chunkSize := 100
	fileSize := uint64(blockSize * chunkSize)

	fileData := make([]byte, fileSize)
	rand.Read(fileData)

	fecCfg := protocol.FECConfig{BlockSize: blockSize, InitialParityPct: 0.05, TailMinParity: 2}
	enc := sender.NewBlockEncoder(fecCfg)

	var parityResult *sender.ParityResult
	for i := 0; i < blockSize; i++ {
		chunk := fileData[i*chunkSize : (i+1)*chunkSize]
		pr := enc.AddShard(0, chunk)
		if pr != nil {
			parityResult = pr
		}
	}

	if parityResult == nil {
		t.Fatal("no parity generated for full block")
	}

	rb := receiver.NewReceiveBuffer(fileSize, chunkSize)
	decoder := receiver.NewBlockDecoder(blockSize, rb)

	for i := 0; i < blockSize; i++ {
		chunk := fileData[i*chunkSize : (i+1)*chunkSize]
		rb.Insert(uint64(i), chunk)
		decoder.RecordData(0, uint64(i), chunk)
	}

	for i, payload := range parityResult.Payloads {
		decoder.RecordParity(0, i, payload)
	}

	recovered := decoder.TryReconstruct(0)
	if len(recovered) != 0 {
		t.Fatalf("expected 0 recovered (all present), got %d", len(recovered))
	}

	stats := decoder.Stats()
	if stats.BlocksComplete != 1 {
		t.Fatalf("BlocksComplete = %d, want 1", stats.BlocksComplete)
	}
}

func TestFECRecoverySingleLoss(t *testing.T) {
	blockSize := 10
	chunkSize := 100
	fileSize := uint64(blockSize * chunkSize)

	fileData := make([]byte, fileSize)
	rand.Read(fileData)

	fecCfg := protocol.FECConfig{BlockSize: blockSize, InitialParityPct: 0.05, TailMinParity: 2}
	enc := sender.NewBlockEncoder(fecCfg)

	var parityResult *sender.ParityResult
	for i := 0; i < blockSize; i++ {
		pr := enc.AddShard(0, fileData[i*chunkSize:(i+1)*chunkSize])
		if pr != nil {
			parityResult = pr
		}
	}

	dropIdx := 3
	rb := receiver.NewReceiveBuffer(fileSize, chunkSize)
	decoder := receiver.NewBlockDecoder(blockSize, rb)

	for i := 0; i < blockSize; i++ {
		if i == dropIdx {
			continue
		}
		chunk := fileData[i*chunkSize : (i+1)*chunkSize]
		rb.Insert(uint64(i), chunk)
		decoder.RecordData(0, uint64(i), chunk)
	}

	for i, payload := range parityResult.Payloads {
		decoder.RecordParity(0, i, payload)
	}

	recovered := decoder.TryReconstruct(0)
	if len(recovered) != 1 {
		t.Fatalf("expected 1 recovered, got %d", len(recovered))
	}

	if recovered[0].SeqNum != uint64(dropIdx) {
		t.Fatalf("recovered seq = %d, want %d", recovered[0].SeqNum, dropIdx)
	}

	expected := fileData[dropIdx*chunkSize : (dropIdx+1)*chunkSize]
	if !bytes.Equal(recovered[0].Payload[:chunkSize], expected) {
		t.Fatal("recovered data mismatch")
	}

	stats := decoder.Stats()
	if stats.BlocksRecovered != 1 || stats.ShardsRecovered != 1 {
		t.Fatalf("FEC stats: recovered=%d shards=%d, want 1/1",
			stats.BlocksRecovered, stats.ShardsRecovered)
	}
}

func TestFECRecoveryMaxLoss(t *testing.T) {
	blockSize := 10
	chunkSize := 100
	fileSize := uint64(blockSize * chunkSize)

	fileData := make([]byte, fileSize)
	rand.Read(fileData)

	fecCfg := protocol.FECConfig{BlockSize: blockSize, InitialParityPct: 0.05, TailMinParity: 2}
	enc := sender.NewBlockEncoder(fecCfg)

	var parityResult *sender.ParityResult
	for i := 0; i < blockSize; i++ {
		pr := enc.AddShard(0, fileData[i*chunkSize:(i+1)*chunkSize])
		if pr != nil {
			parityResult = pr
		}
	}

	parityCount := parityResult.ParityCount
	t.Logf("parity count = %d for %d data shards", parityCount, blockSize)

	rb := receiver.NewReceiveBuffer(fileSize, chunkSize)
	decoder := receiver.NewBlockDecoder(blockSize, rb)

	// Drop exactly parityCount data shards
	for i := 0; i < blockSize; i++ {
		if i < parityCount {
			continue
		}
		chunk := fileData[i*chunkSize : (i+1)*chunkSize]
		rb.Insert(uint64(i), chunk)
		decoder.RecordData(0, uint64(i), chunk)
	}

	for i, payload := range parityResult.Payloads {
		decoder.RecordParity(0, i, payload)
	}

	recovered := decoder.TryReconstruct(0)
	if len(recovered) != parityCount {
		t.Fatalf("expected %d recovered, got %d", parityCount, len(recovered))
	}

	for _, rs := range recovered {
		idx := int(rs.SeqNum)
		expected := fileData[idx*chunkSize : (idx+1)*chunkSize]
		if !bytes.Equal(rs.Payload[:chunkSize], expected) {
			t.Fatalf("shard %d data mismatch", idx)
		}
	}
}

func TestFECTooManyLost(t *testing.T) {
	blockSize := 10
	chunkSize := 100
	fileSize := uint64(blockSize * chunkSize)

	fileData := make([]byte, fileSize)
	rand.Read(fileData)

	fecCfg := protocol.FECConfig{BlockSize: blockSize, InitialParityPct: 0.05, TailMinParity: 2}
	enc := sender.NewBlockEncoder(fecCfg)

	var parityResult *sender.ParityResult
	for i := 0; i < blockSize; i++ {
		pr := enc.AddShard(0, fileData[i*chunkSize:(i+1)*chunkSize])
		if pr != nil {
			parityResult = pr
		}
	}

	parityCount := parityResult.ParityCount

	rb := receiver.NewReceiveBuffer(fileSize, chunkSize)
	decoder := receiver.NewBlockDecoder(blockSize, rb)

	// Drop parityCount+1 — too many
	for i := 0; i < blockSize; i++ {
		if i <= parityCount {
			continue
		}
		chunk := fileData[i*chunkSize : (i+1)*chunkSize]
		rb.Insert(uint64(i), chunk)
		decoder.RecordData(0, uint64(i), chunk)
	}

	for i, payload := range parityResult.Payloads {
		decoder.RecordParity(0, i, payload)
	}

	recovered := decoder.TryReconstruct(0)
	if recovered != nil {
		t.Fatalf("expected nil (too many lost), got %d recovered", len(recovered))
	}
}

func TestFECTailBlock(t *testing.T) {
	blockSize := 10
	chunkSize := 100
	totalChunks := 13
	fileSize := uint64(totalChunks * chunkSize)

	fileData := make([]byte, fileSize)
	rand.Read(fileData)

	fecCfg := protocol.FECConfig{BlockSize: blockSize, InitialParityPct: 0.05, TailMinParity: 2}
	enc := sender.NewBlockEncoder(fecCfg)

	var block0Parity, block1Parity *sender.ParityResult
	for i := 0; i < totalChunks; i++ {
		blockGroup := uint64(i / blockSize)
		chunk := fileData[i*chunkSize : (i+1)*chunkSize]
		pr := enc.AddShard(blockGroup, chunk)
		if pr != nil {
			if blockGroup == 0 {
				block0Parity = pr
			} else {
				block1Parity = pr
			}
		}
	}

	tailParity := enc.FlushTail()
	if block1Parity == nil && tailParity != nil {
		block1Parity = tailParity
	}

	if block1Parity == nil {
		t.Fatal("no parity for tail block")
	}

	if block1Parity.DataCount != 3 {
		t.Fatalf("tail DataCount = %d, want 3", block1Parity.DataCount)
	}
	if block1Parity.ParityCount < 2 {
		t.Fatalf("tail ParityCount = %d, want >= 2", block1Parity.ParityCount)
	}

	// Drop seq 11 from tail block
	rb := receiver.NewReceiveBuffer(fileSize, chunkSize)
	decoder := receiver.NewBlockDecoder(blockSize, rb)

	for i := 0; i < blockSize; i++ {
		chunk := fileData[i*chunkSize : (i+1)*chunkSize]
		rb.Insert(uint64(i), chunk)
		decoder.RecordData(0, uint64(i), chunk)
	}
	if block0Parity != nil {
		for i, p := range block0Parity.Payloads {
			decoder.RecordParity(0, i, p)
		}
	}

	for i := blockSize; i < totalChunks; i++ {
		if i == 11 {
			continue
		}
		chunk := fileData[i*chunkSize : (i+1)*chunkSize]
		rb.Insert(uint64(i), chunk)
		decoder.RecordData(1, uint64(i), chunk)
	}

	for i, p := range block1Parity.Payloads {
		decoder.RecordParity(1, i, p)
	}

	recovered := decoder.TryReconstruct(1)
	if len(recovered) != 1 {
		t.Fatalf("expected 1 recovered from tail, got %d", len(recovered))
	}

	if recovered[0].SeqNum != 11 {
		t.Fatalf("recovered seq = %d, want 11", recovered[0].SeqNum)
	}

	expected := fileData[11*chunkSize : 12*chunkSize]
	if !bytes.Equal(recovered[0].Payload[:chunkSize], expected) {
		t.Fatal("tail block recovery data mismatch")
	}
}

func TestFECUnrecoverableReporting(t *testing.T) {
	blockSize := 5
	chunkSize := 100
	fileSize := uint64(blockSize * chunkSize)

	fileData := make([]byte, fileSize)
	rand.Read(fileData)

	rb := receiver.NewReceiveBuffer(fileSize, chunkSize)
	decoder := receiver.NewBlockDecoder(blockSize, rb)

	rb.Insert(0, fileData[0:100])
	decoder.RecordData(0, 0, fileData[0:100])
	rb.Insert(3, fileData[300:400])
	decoder.RecordData(0, 3, fileData[300:400])

	// Without parity metadata, can't determine what's missing
	unrec := decoder.UnrecoverableInBlock(0)
	if unrec != nil {
		t.Fatalf("expected nil without metadata, got %v", unrec)
	}

	// Generate and provide parity so metadata is available
	fecCfg := protocol.FECConfig{BlockSize: blockSize, TailMinParity: 2}
	enc := sender.NewBlockEncoder(fecCfg)
	var parity *sender.ParityResult
	for i := 0; i < blockSize; i++ {
		pr := enc.AddShard(0, fileData[i*chunkSize:(i+1)*chunkSize])
		if pr != nil {
			parity = pr
		}
	}
	if parity == nil {
		parity = enc.FlushTail()
	}
	if parity != nil {
		for i, p := range parity.Payloads {
			decoder.RecordParity(0, i, p)
		}
	}

	unrec = decoder.UnrecoverableInBlock(0)
	if len(unrec) != 3 {
		t.Fatalf("expected 3 unrecoverable, got %d: %v", len(unrec), unrec)
	}
}
