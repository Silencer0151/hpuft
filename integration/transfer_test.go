package integration

import (
	"crypto/rand"
	"fmt"
	"hpuft/receiver"
	"hpuft/sender"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// freePort returns an available UDP port by binding :0 and releasing it.
func freePort(t *testing.T) int {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := conn.LocalAddr().(*net.UDPAddr).Port
	conn.Close()
	return port
}

// genFile creates a temporary file filled with random bytes.
func genFile(t *testing.T, dir string, size int) string {
	t.Helper()
	path := filepath.Join(dir, "testfile.bin")
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

// TestLoopbackTransfer sends a file over localhost UDP and verifies integrity.
// The receiver's Run() returns an error on hash mismatch, so a nil error
// from both sides plus byte-equal files is a strong end-to-end guarantee.
func TestLoopbackTransfer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping loopback transfer in short mode")
	}

	const fileSize = 256 * 1024 // 256 KB — exercises multiple FEC blocks

	srcDir := t.TempDir()
	dstDir := t.TempDir()

	srcPath := genFile(t, srcDir, fileSize)

	port := freePort(t)
	listenAddr := fmt.Sprintf(":%d", port)
	remoteAddr := fmt.Sprintf("127.0.0.1:%d", port)

	// --- Start receiver ---
	recvCfg := receiver.DefaultConfig()
	recvCfg.ListenAddr = listenAddr
	recvCfg.OutputDir = dstDir
	recvCfg.Debug = true

	recv, err := receiver.New(recvCfg)
	if err != nil {
		t.Fatalf("receiver.New: %v", err)
	}

	recvErr := make(chan error, 1)
	go func() { recvErr <- recv.Run() }()

	// Give receiver time to bind
	time.Sleep(100 * time.Millisecond)

	// --- Start sender ---
	sendCfg := sender.DefaultConfig()
	sendCfg.RemoteAddr = remoteAddr
	sendCfg.FilePath = srcPath
	sendCfg.NoDelay = true // fast as possible for testing
	sendCfg.Debug = true

	snd := sender.New(sendCfg)

	sendErr := make(chan error, 1)
	go func() { sendErr <- snd.Send() }()

	// --- Wait for both with timeout ---
	timeout := time.After(30 * time.Second)

	select {
	case err := <-sendErr:
		if err != nil {
			t.Fatalf("sender error: %v", err)
		}
	case <-timeout:
		t.Fatal("sender timed out after 30s")
	}

	select {
	case err := <-recvErr:
		if err != nil {
			t.Fatalf("receiver error: %v", err)
		}
	case <-timeout:
		t.Fatal("receiver timed out after 30s")
	}

	// --- Verify file integrity ---
	original, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatalf("read source: %v", err)
	}

	dstPath := filepath.Join(dstDir, filepath.Base(srcPath))
	received, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("read received: %v", err)
	}

	if len(original) != len(received) {
		t.Fatalf("size mismatch: sent %d bytes, received %d bytes", len(original), len(received))
	}

	// Byte-level comparison (hash is already verified by receiver.Run)
	for i := range original {
		if original[i] != received[i] {
			t.Fatalf("data mismatch at byte %d: sent 0x%02X, received 0x%02X", i, original[i], received[i])
		}
	}
}

// TestLoopbackLargeFile sends a 1 MB file to exercise more FEC blocks and CC.
func TestLoopbackLargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large loopback transfer in short mode")
	}

	const fileSize = 1024 * 1024 // 1 MB

	srcDir := t.TempDir()
	dstDir := t.TempDir()

	srcPath := genFile(t, srcDir, fileSize)

	port := freePort(t)
	listenAddr := fmt.Sprintf(":%d", port)
	remoteAddr := fmt.Sprintf("127.0.0.1:%d", port)

	recvCfg := receiver.DefaultConfig()
	recvCfg.ListenAddr = listenAddr
	recvCfg.OutputDir = dstDir

	recv, err := receiver.New(recvCfg)
	if err != nil {
		t.Fatalf("receiver.New: %v", err)
	}

	recvErr := make(chan error, 1)
	go func() { recvErr <- recv.Run() }()
	time.Sleep(100 * time.Millisecond)

	sendCfg := sender.DefaultConfig()
	sendCfg.RemoteAddr = remoteAddr
	sendCfg.FilePath = srcPath
	sendCfg.NoDelay = true

	snd := sender.New(sendCfg)
	sendErr := make(chan error, 1)
	go func() { sendErr <- snd.Send() }()

	timeout := time.After(60 * time.Second)

	select {
	case err := <-sendErr:
		if err != nil {
			t.Fatalf("sender error: %v", err)
		}
	case <-timeout:
		t.Fatal("sender timed out")
	}

	select {
	case err := <-recvErr:
		if err != nil {
			t.Fatalf("receiver error: %v", err)
		}
	case <-timeout:
		t.Fatal("receiver timed out")
	}

	srcHash, err := receiver.HashFile(srcPath)
	if err != nil {
		t.Fatalf("hash source: %v", err)
	}

	dstPath := filepath.Join(dstDir, filepath.Base(srcPath))
	dstHash, err := receiver.HashFile(dstPath)
	if err != nil {
		t.Fatalf("hash received: %v", err)
	}

	if srcHash != dstHash {
		t.Fatalf("hash mismatch: src=0x%016X dst=0x%016X", srcHash, dstHash)
	}
}
