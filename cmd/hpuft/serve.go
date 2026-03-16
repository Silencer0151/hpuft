package main

import (
	"flag"
	"fmt"
	"hpuft/protocol"
	"hpuft/receiver"
	"hpuft/sender"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listenAddr := fs.String("listen", ":9001", "address to listen on for PULL_REQ / PUSH_REQ")
	dir := fs.String("dir", ".", "directory of files available to serve")
	fs.Parse(args)

	// Serve uses structured event logging on stderr with timestamps.
	// No progress bar — it runs as a daemon and may have no terminal.
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetOutput(os.Stderr)

	var manifestMu sync.RWMutex
	manifest := buildManifest(*dir)

	addr, err := net.ResolveUDPAddr("udp", *listenAddr)
	if err != nil {
		log.Fatalf("[serve] resolve addr: %v", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("[serve] listen: %v", err)
	}
	defer conn.Close()
	conn.SetReadBuffer(16 * 1024 * 1024)

	// Startup events — always visible.
	fmt.Fprintf(os.Stderr, "[serve] Online. Listening on %s\n", conn.LocalAddr())
	fmt.Fprintf(os.Stderr, "[serve] Manifest loaded: %d authorized files found in %s\n", len(manifest), *dir)

	// busy is 0 when idle, 1 when a transfer is in progress.
	var busy int32
	var busyClient string // address string of the active client, for BUSY log

	rawBuf := make([]byte, protocol.MTUHardCap)

	for {
		n, clientAddr, err := conn.ReadFromUDP(rawBuf)
		if err != nil {
			log.Printf("[serve] read error: %v", err)
			continue
		}

		pkt, err := protocol.UnmarshalPacket(rawBuf[:n])
		if err != nil {
			continue
		}

		switch pkt.Header.Type {
		case protocol.PacketPullReq:
			handlePullReq(conn, clientAddr, &pkt, manifest, &manifestMu, &busy, &busyClient)

		case protocol.PacketPushReq:
			handlePushReq(conn, clientAddr, &pkt, *dir, manifest, &manifestMu, &busy, &busyClient)
		}
	}
}

func handlePullReq(conn *net.UDPConn, clientAddr *net.UDPAddr, pkt *protocol.Packet,
	manifest map[string]string, manifestMu *sync.RWMutex,
	busy *int32, busyClient *string) {

	req, err := protocol.UnmarshalPullReq(pkt.Payload)
	if err != nil || req.FileName == "" {
		log.Printf("[serve] malformed PULL_REQ from %s", clientAddr)
		return
	}

	// --- Busy check ---
	if !atomic.CompareAndSwapInt32(busy, 0, 1) {
		sendReject(conn, clientAddr, pkt.Header.SessionID, protocol.RejectServerBusy)
		log.Printf("[serve] REJECTED %s: SERVER_BUSY (Transferring to %s)", clientAddr, *busyClient)
		return
	}
	*busyClient = clientAddr.String()

	// --- Manifest check (allowlist) ---
	manifestMu.RLock()
	filePath, ok := manifest[req.FileName]
	manifestMu.RUnlock()
	if !ok {
		sendReject(conn, clientAddr, pkt.Header.SessionID, protocol.RejectFileNotFound)
		atomic.StoreInt32(busy, 0)
		log.Printf("[serve] REJECTED %s: PULL_REQ for %q (Not in manifest)", clientAddr, req.FileName)
		return
	}

	log.Printf("[serve] ACCEPTED %s: PULL_REQ for %q", clientAddr, req.FileName)

	go func(filePath, fileName, clientAddrStr string, sessionID uint32) {
		defer atomic.StoreInt32(busy, 0)

		cfg := sender.DefaultConfig()
		cfg.FilePath = filePath
		cfg.RemoteAddr = clientAddrStr
		cfg.SessionID = sessionID
		cfg.Quiet = true // suppress all [sender] internal logs; serve handles its own events

		start := time.Now()
		s := sender.New(cfg)
		if err := s.Send(); err != nil {
			log.Printf("[serve] TRANSFER FAILED: %q to %s — %v", fileName, clientAddrStr, err)
			return
		}
		elapsed := time.Since(start)
		p := s.Progress()
		mbps := float64(p.TotalBytes) / elapsed.Seconds() / 1e6
		log.Printf("[serve] TRANSFER COMPLETE: %q to %s in %s (%.1f MB/s)",
			fileName, clientAddrStr, elapsed.Round(time.Millisecond), mbps)
	}(filePath, req.FileName, clientAddr.String(), pkt.Header.SessionID)
}

func handlePushReq(conn *net.UDPConn, clientAddr *net.UDPAddr, pkt *protocol.Packet,
	dir string, manifest map[string]string, manifestMu *sync.RWMutex,
	busy *int32, busyClient *string) {

	req, err := protocol.UnmarshalPushReq(pkt.Payload)
	if err != nil || req.FileName == "" {
		log.Printf("[serve] malformed PUSH_REQ from %s", clientAddr)
		return
	}

	// --- Base-name sanitization ---
	safeName := filepath.Base(req.FileName)
	if safeName == "." || safeName == "/" {
		sendReject(conn, clientAddr, pkt.Header.SessionID, protocol.RejectFileNotFound)
		log.Printf("[serve] REJECTED %s: PUSH_REQ for %q (invalid filename)", clientAddr, req.FileName)
		return
	}

	// --- Busy check ---
	if !atomic.CompareAndSwapInt32(busy, 0, 1) {
		sendReject(conn, clientAddr, pkt.Header.SessionID, protocol.RejectServerBusy)
		log.Printf("[serve] REJECTED %s: SERVER_BUSY (Transferring to %s)", clientAddr, *busyClient)
		return
	}
	*busyClient = clientAddr.String()

	// --- No-overwrite check ---
	finalPath := filepath.Join(dir, safeName)
	if _, err := os.Stat(finalPath); err == nil {
		sendReject(conn, clientAddr, pkt.Header.SessionID, protocol.RejectFileExists)
		atomic.StoreInt32(busy, 0)
		log.Printf("[serve] REJECTED %s: PUSH_REQ for %q (file already exists)", clientAddr, safeName)
		return
	}

	// --- Bind ephemeral data port ---
	dataConn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		sendReject(conn, clientAddr, pkt.Header.SessionID, protocol.RejectServerBusy)
		atomic.StoreInt32(busy, 0)
		log.Printf("[serve] PUSH_REQ: failed to bind data port: %v", err)
		return
	}

	port := uint16(dataConn.LocalAddr().(*net.UDPAddr).Port)

	// --- Reply PUSH_ACCEPT ---
	acceptPkt := protocol.Packet{
		Header: protocol.Header{
			Type:      protocol.PacketPushAccept,
			SessionID: pkt.Header.SessionID,
		},
		Payload: protocol.MarshalPushAccept(&protocol.PushAcceptPayload{Port: port}),
	}
	raw, err := protocol.MarshalPacket(&acceptPkt)
	if err != nil {
		dataConn.Close()
		atomic.StoreInt32(busy, 0)
		return
	}
	conn.WriteToUDP(raw, clientAddr)

	log.Printf("[serve] ACCEPTED %s: PUSH_REQ for %q (data port %d)", clientAddr, safeName, port)

	go func(safeName, finalPath string, sessionID uint32) {
		defer atomic.StoreInt32(busy, 0)
		defer dataConn.Close()

		tmpPath := finalPath + ".tmp"

		cfg := receiver.DefaultConfig()
		cfg.Conn = dataConn
		cfg.OutputPath = tmpPath

		r, err := receiver.New(cfg)
		if err != nil {
			log.Printf("[serve] PUSH init error for %q: %v", safeName, err)
			return
		}

		start := time.Now()
		if err := r.Run(); err != nil {
			log.Printf("[serve] PUSH FAILED: %q from %s — %v", safeName, clientAddr, err)
			os.Remove(tmpPath)
			return
		}
		elapsed := time.Since(start)

		// Promote .tmp → final
		if err := os.Rename(tmpPath, finalPath); err != nil {
			log.Printf("[serve] PUSH promote failed for %q: %v", safeName, err)
			os.Remove(tmpPath)
			return
		}

		// Add to manifest
		absPath, _ := filepath.Abs(finalPath)
		manifestMu.Lock()
		manifest[safeName] = absPath
		manifestMu.Unlock()

		p := r.Progress()
		mbps := float64(p.TotalBytes) / elapsed.Seconds() / 1e6
		log.Printf("[serve] PUSH COMPLETE: %q from %s in %s (%.1f MB/s) — added to manifest",
			safeName, clientAddr, elapsed.Round(time.Millisecond), mbps)
	}(safeName, finalPath, pkt.Header.SessionID)
}

// buildManifest scans dir (non-recursively) and returns an allowlist map of
// filename → absolute path. Only regular files are included; directories and
// symlinks are skipped. The map is built once at startup so requests for files
// added after launch are not served (intentional security boundary).
func buildManifest(dir string) map[string]string {
	manifest := make(map[string]string)

	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Fatalf("[serve] read dir %q: %v", dir, err)
	}

	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		name := entry.Name()
		abs, err := filepath.Abs(filepath.Join(dir, name))
		if err != nil {
			log.Printf("[serve] skipping %q: %v", name, err)
			continue
		}
		manifest[name] = abs
	}

	return manifest
}

// sendReject sends a SESSION_REJECT packet to addr with the given reason code.
func sendReject(conn *net.UDPConn, addr *net.UDPAddr, sessionID uint32, reason protocol.RejectReason) {
	pkt := protocol.Packet{
		Header: protocol.Header{
			Type:      protocol.PacketSessionReject,
			SessionID: sessionID,
		},
		Payload: []byte{byte(reason)},
	}
	raw, err := protocol.MarshalPacket(&pkt)
	if err != nil {
		return
	}
	conn.WriteToUDP(raw, addr)
}
