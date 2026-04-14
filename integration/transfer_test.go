package integration

import (
	"crypto/rand"
	"fmt"
	"hpuft/protocol"
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

// deriveTestKeys performs an in-process ECDH handshake and returns the shared
// session key and IV, identical on both sides.
func deriveTestKeys(t *testing.T, connID uint32) ([16]byte, [8]byte) {
	t.Helper()
	clientPriv, err := protocol.GenerateEphemeralKey()
	if err != nil {
		t.Fatalf("GenerateEphemeralKey (client): %v", err)
	}
	serverPriv, err := protocol.GenerateEphemeralKey()
	if err != nil {
		t.Fatalf("GenerateEphemeralKey (server): %v", err)
	}
	// ECDH is symmetric: DeriveSessionKey(clientPriv, serverPub) == DeriveSessionKey(serverPriv, clientPub)
	key, ivBase, err := protocol.DeriveSessionKey(clientPriv, serverPriv.PublicKey().Bytes(), connID)
	if err != nil {
		t.Fatalf("DeriveSessionKey: %v", err)
	}
	return key, ivBase
}

// runLoopback wires a sender and receiver over localhost UDP sockets, both
// using pre-derived v6 session keys, and waits for both sides to finish.
// The sender socket is pre-bound so its address is known before Run() is
// called, allowing the receiver to direct heartbeats to the correct port.
func runLoopback(t *testing.T, srcPath string, dstDir string, debug bool) {
	t.Helper()

	const connID = uint32(0xABCD1234)
	key, ivBase := deriveTestKeys(t, connID)

	// Hash source file — receiver needs this for integrity check.
	srcHash, err := receiver.HashFile(srcPath)
	if err != nil {
		t.Fatalf("HashFile: %v", err)
	}
	fi, err := os.Stat(srcPath)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	fileSize := uint64(fi.Size())

	// Bind receiver socket.
	recvConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP (recv): %v", err)
	}
	defer recvConn.Close()
	recvAddr := recvConn.LocalAddr().(*net.UDPAddr)

	// Bind sender socket so we know its address upfront for heartbeat routing.
	senderConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP (sender): %v", err)
	}
	defer senderConn.Close()
	senderAddr := senderConn.LocalAddr().(*net.UDPAddr)

	// Receiver config.
	recvCfg := receiver.DefaultConfig()
	recvCfg.OutputDir = dstDir
	recvCfg.Conn = recvConn
	recvCfg.EncKey = &key
	recvCfg.IVBase = &ivBase
	recvCfg.Debug = debug
	recvCfg.IncomingSession = &receiver.IncomingSession{
		SenderAddr:   senderAddr,
		ConnectionID: connID,
		Meta: receiver.TransferMeta{
			FileName:    filepath.Base(srcPath),
			FileSize:    fileSize,
			FileHash:    srcHash,
			InitialRate: 0,
		},
	}

	recv, err := receiver.New(recvCfg)
	if err != nil {
		t.Fatalf("receiver.New: %v", err)
	}

	// Sender config — MuxConn routes through our pre-bound socket so receiver
	// can address heartbeats correctly; no RecvChan needed (heartbeat goroutine
	// reads from MuxConn directly via conn.Read).
	sendCfg := sender.DefaultConfig()
	sendCfg.FilePath = srcPath
	sendCfg.MuxConn = senderConn
	sendCfg.MuxAddr = recvAddr
	sendCfg.NoDelay = true
	sendCfg.EncKey = &key
	sendCfg.IVBase = &ivBase
	sendCfg.ConnectionID = connID
	sendCfg.Debug = debug

	snd := sender.New(sendCfg)

	recvErr := make(chan error, 1)
	sendErr := make(chan error, 1)

	go func() { recvErr <- recv.Run() }()
	// Small pause so receiver is in its read loop before sender fires.
	time.Sleep(10 * time.Millisecond)
	go func() { sendErr <- snd.Send() }()

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

	// Verify file integrity (hash already checked by receiver.Run; belt+suspenders).
	dstPath := filepath.Join(dstDir, filepath.Base(srcPath))
	dstHash, err := receiver.HashFile(dstPath)
	if err != nil {
		t.Fatalf("HashFile (dst): %v", err)
	}
	if srcHash != dstHash {
		t.Fatalf("hash mismatch: src=0x%016X dst=0x%016X", srcHash, dstHash)
	}
}

// TestLoopbackTransfer sends a 256 KB file over localhost UDP and verifies
// end-to-end integrity using the v6 pre-derived key model.
func TestLoopbackTransfer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping loopback transfer in short mode")
	}

	const fileSize = 256 * 1024 // exercises multiple FEC blocks

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	srcPath := genFile(t, srcDir, fileSize)

	runLoopback(t, srcPath, dstDir, true)
}

// TestLoopbackLargeFile sends a 1 MB file to exercise more FEC blocks and CC.
func TestLoopbackLargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large loopback transfer in short mode")
	}

	const fileSize = 1024 * 1024

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	srcPath := genFile(t, srcDir, fileSize)

	// Print port info for diagnostics.
	fmt.Printf("TestLoopbackLargeFile: srcPath=%s dstDir=%s\n", srcPath, dstDir)

	runLoopback(t, srcPath, dstDir, false)
}
