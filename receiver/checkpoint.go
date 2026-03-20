package receiver

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

// CheckpointData holds the state needed to resume an interrupted transfer.
type CheckpointData struct {
	HighestContiguous uint64 // sequence number of last contiguous chunk
	PartialHash       uint64 // xxHash64 of bytes 0..(HighestContiguous+1)*chunkSize
	FileSize          uint64 // total expected file size
	FullHash          uint64 // xxHash64 of complete source file
	ChunkSize         uint32 // payload size per chunk
	FileName          string // original file name
}

// Binary sidecar format:
//   [0:8]   HighestContiguous
//   [8:16]  PartialHash
//   [16:24] FileSize
//   [24:32] FullHash
//   [32:36] ChunkSize
//   [36:]   FileName (null-terminated)
const checkpointFixedSize = 36

// WriteCheckpoint atomically writes a checkpoint sidecar file.
func WriteCheckpoint(path string, data *CheckpointData) error {
	nameBytes := []byte(data.FileName)
	buf := make([]byte, checkpointFixedSize+len(nameBytes)+1)

	binary.BigEndian.PutUint64(buf[0:8], data.HighestContiguous)
	binary.BigEndian.PutUint64(buf[8:16], data.PartialHash)
	binary.BigEndian.PutUint64(buf[16:24], data.FileSize)
	binary.BigEndian.PutUint64(buf[24:32], data.FullHash)
	binary.BigEndian.PutUint32(buf[32:36], data.ChunkSize)
	copy(buf[36:], nameBytes)
	buf[len(buf)-1] = 0

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

// ReadCheckpoint reads a checkpoint sidecar file.
func ReadCheckpoint(path string) (*CheckpointData, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(buf) < checkpointFixedSize+1 {
		return nil, fmt.Errorf("checkpoint file too short: %d bytes", len(buf))
	}

	data := &CheckpointData{
		HighestContiguous: binary.BigEndian.Uint64(buf[0:8]),
		PartialHash:       binary.BigEndian.Uint64(buf[8:16]),
		FileSize:          binary.BigEndian.Uint64(buf[16:24]),
		FullHash:          binary.BigEndian.Uint64(buf[24:32]),
		ChunkSize:         binary.BigEndian.Uint32(buf[32:36]),
	}

	nameData := buf[36:]
	for i, b := range nameData {
		if b == 0 {
			data.FileName = string(nameData[:i])
			return data, nil
		}
	}
	data.FileName = string(nameData)
	return data, nil
}

// CheckpointPath returns the sidecar path for a given output file.
func CheckpointPath(outputPath string) string {
	return outputPath + ".hpuft-resume"
}

// DeleteCheckpoint removes the checkpoint sidecar file if it exists.
func DeleteCheckpoint(outputPath string) {
	os.Remove(CheckpointPath(outputPath))
}

// FindCheckpoint looks for an existing checkpoint matching the given file
// parameters in the output directory. Returns nil if no matching sidecar found.
func FindCheckpoint(outputDir, fileName string, fileSize, fullHash uint64) (*CheckpointData, string) {
	// The .tmp file and sidecar are named after the base filename.
	tmpPath := filepath.Join(outputDir, filepath.Base(fileName))
	cpPath := CheckpointPath(tmpPath)

	cp, err := ReadCheckpoint(cpPath)
	if err != nil {
		return nil, ""
	}

	// Validate checkpoint matches the requested transfer.
	if cp.FileSize != fileSize || cp.FullHash != fullHash || cp.FileName != filepath.Base(fileName) {
		return nil, ""
	}

	// Verify the .tmp file still exists.
	if _, err := os.Stat(tmpPath); err != nil {
		return nil, ""
	}

	return cp, tmpPath
}
