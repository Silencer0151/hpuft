package receiver

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

// v5.2 checkpoint sidecar format (.hpudp-ckpt), all little-endian:
//
//	[0:4]   Magic:             0x48505543 ("HPUC")
//	[4:8]   Version:           1
//	[8:16]  file_size          (uint64)
//	[16:24] file_hash          (uint64, xxHash64)
//	[24:32] total_chunks       (uint64)
//	[32:40] highest_contiguous (uint64)
//	[40:]   recv_bits          (ceil(total_chunks/8) bytes)

const (
	checkpointMagic   = uint32(0x48505543) // "HPUC"
	checkpointVersion = uint32(1)
	checkpointFixedSize = 40 // bytes before recv_bits
)

// CheckpointData holds the state needed to resume an interrupted transfer.
type CheckpointData struct {
	FileSize          uint64
	FileHash          uint64 // xxHash64 of the complete source file
	TotalChunks       uint64
	HighestContiguous uint64
	RecvBits          []byte // ceil(TotalChunks/8) bytes
}

// CheckpointPath returns the sidecar path for a given output file.
func CheckpointPath(outputPath string) string {
	return outputPath + ".hpudp-ckpt"
}

// WriteCheckpoint atomically writes a v5.2 checkpoint sidecar file.
func WriteCheckpoint(path string, data *CheckpointData) error {
	recvBitsLen := (data.TotalChunks + 7) / 8
	buf := make([]byte, checkpointFixedSize+recvBitsLen)

	binary.LittleEndian.PutUint32(buf[0:4], checkpointMagic)
	binary.LittleEndian.PutUint32(buf[4:8], checkpointVersion)
	binary.LittleEndian.PutUint64(buf[8:16], data.FileSize)
	binary.LittleEndian.PutUint64(buf[16:24], data.FileHash)
	binary.LittleEndian.PutUint64(buf[24:32], data.TotalChunks)
	binary.LittleEndian.PutUint64(buf[32:40], data.HighestContiguous)
	if len(data.RecvBits) > 0 {
		copy(buf[40:], data.RecvBits)
	}

	// Atomic write: write to .tmp, rename over target.
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, buf, 0644); err != nil {
		return fmt.Errorf("write checkpoint tmp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename checkpoint: %w", err)
	}
	return nil
}

// ReadCheckpoint reads a v5.2 checkpoint sidecar file.
func ReadCheckpoint(path string) (*CheckpointData, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(buf) < checkpointFixedSize {
		return nil, fmt.Errorf("checkpoint file too short: %d bytes", len(buf))
	}

	magic := binary.LittleEndian.Uint32(buf[0:4])
	if magic != checkpointMagic {
		return nil, fmt.Errorf("checkpoint bad magic: 0x%08X", magic)
	}
	version := binary.LittleEndian.Uint32(buf[4:8])
	if version != checkpointVersion {
		return nil, fmt.Errorf("checkpoint unsupported version: %d", version)
	}

	data := &CheckpointData{
		FileSize:          binary.LittleEndian.Uint64(buf[8:16]),
		FileHash:          binary.LittleEndian.Uint64(buf[16:24]),
		TotalChunks:       binary.LittleEndian.Uint64(buf[24:32]),
		HighestContiguous: binary.LittleEndian.Uint64(buf[32:40]),
	}

	recvBitsLen := (data.TotalChunks + 7) / 8
	if uint64(len(buf)) >= checkpointFixedSize+recvBitsLen {
		data.RecvBits = make([]byte, recvBitsLen)
		copy(data.RecvBits, buf[checkpointFixedSize:checkpointFixedSize+recvBitsLen])
	}

	return data, nil
}

// DeleteCheckpoint removes the checkpoint sidecar file if it exists.
func DeleteCheckpoint(outputPath string) {
	os.Remove(CheckpointPath(outputPath))
	// Also clean up old v5.1 sidecar if present.
	os.Remove(outputPath + ".hpuft-resume")
}

// FindCheckpoint looks for a v5.2 checkpoint matching the given file
// parameters. Returns nil if no matching sidecar is found.
func FindCheckpoint(outputDir, fileName string, fileSize, fileHash uint64) (*CheckpointData, string) {
	tmpPath := filepath.Join(outputDir, filepath.Base(fileName))
	cpPath := CheckpointPath(tmpPath)

	cp, err := ReadCheckpoint(cpPath)
	if err != nil {
		return nil, ""
	}

	// Validate checkpoint matches the requested transfer.
	if cp.FileSize != fileSize || cp.FileHash != fileHash {
		return nil, ""
	}

	// Verify the .tmp file still exists.
	if _, err := os.Stat(tmpPath); err != nil {
		return nil, ""
	}

	return cp, tmpPath
}
