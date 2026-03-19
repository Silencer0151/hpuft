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

	fmt.Fprintf(os.Stderr, "[serve] Online. Listening on %s\n", conn.LocalAddr())
	fmt.Fprintf(os.Stderr, "[serve] Manifest loaded: %d authorized files found in %s\n", len(manifest), *dir)

	// busy is 0 when idle, 1 when a transfer is in progress.
	var busy int32
	var busyClient string

	// activeChan is the forwarding channel for the active transfer goroutine.
	// The main loop copies every non-control packet here when busy==1.
	// Protected by activeMu for safe set/clear across goroutines.
	var activeMu sync.Mutex
	var activeChan chan []byte
	var activeChanDrops atomic.Int64

	rawBuf := make([]byte, protocol.MTUHardCap)

	for {
		n, clientAddr, err := conn.ReadFromUDP(rawBuf)
		if err != nil {
			log.Printf("[serve] read error: %v", err)
			continue
		}

		// If a transfer is in progress, new PULL_REQ/PUSH_REQ get a SERVER_BUSY
		// rejection immediately. All other packets are forwarded to the active
		// goroutine for it to decode. Drop on channel full; sender/receiver retransmit.
		if atomic.LoadInt32(&busy) == 1 {
			pkt, err := protocol.UnmarshalPacket(rawBuf[:n])
			if err == nil {
				switch pkt.Header.Type {
				case protocol.PacketPullReq, protocol.PacketPushReq:
					sendReject(conn, clientAddr, pkt.Header.SessionID, protocol.RejectServerBusy)
					log.Printf("[serve] REJECTED %s: SERVER_BUSY (Transferring to %s)", clientAddr, busyClient)
					continue
				}
			}
			raw := make([]byte, n)
			copy(raw, rawBuf[:n])
			activeMu.Lock()
			ch := activeChan
			activeMu.Unlock()
			if ch != nil {
				select {
				case ch <- raw:
				default:
					activeChanDrops.Add(1)
				}
			}
			continue
		}

		pkt, err := protocol.UnmarshalPacket(rawBuf[:n])
		if err != nil {
			continue
		}

		switch pkt.Header.Type {
		case protocol.PacketPullReq:
			handlePullReq(conn, clientAddr, &pkt, manifest, &manifestMu, &busy, &busyClient, &activeMu, &activeChan)
		case protocol.PacketPushReq:
			handlePushReq(conn, clientAddr, &pkt, *dir, manifest, &manifestMu, &busy, &busyClient, &activeMu, &activeChan, &activeChanDrops)
		}
	}
}

func handlePullReq(conn *net.UDPConn, clientAddr *net.UDPAddr, pkt *protocol.Packet,
	manifest map[string]string, manifestMu *sync.RWMutex,
	busy *int32, busyClient *string,
	activeMu *sync.Mutex, activeChan *chan []byte) {

	req, err := protocol.UnmarshalPullReq(pkt.Payload, pkt.Header.Flags&protocol.FlagEncrypted != 0)
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

	// Create the forwarding channel before the goroutine starts.
	// The main loop won't forward packets until busy==1, which was set above
	// in this same goroutine (the main read loop), so no race.
	ch := make(chan []byte, 256)
	activeMu.Lock()
	*activeChan = ch
	activeMu.Unlock()

	log.Printf("[serve] ACCEPTED %s: PULL_REQ for %q", clientAddr, req.FileName)

	go func(filePath, fileName string, sessionID uint32, clientAddr *net.UDPAddr, peerPubKey []byte, isEncrypted bool) {
		defer func() {
			atomic.StoreInt32(busy, 0)
			activeMu.Lock()
			*activeChan = nil
			activeMu.Unlock()
		}()

		cfg := sender.DefaultConfig()
		cfg.FilePath = filePath
		cfg.RemoteAddr = clientAddr.String() // for logging
		cfg.SessionID = sessionID
		cfg.MuxConn = conn
		cfg.MuxAddr = clientAddr
		cfg.RecvChan = ch
		cfg.Quiet = true
		cfg.Encrypt = isEncrypted
		if isEncrypted && len(peerPubKey) > 0 {
			cfg.PeerPubKey = peerPubKey
		}

		start := time.Now()
		s := sender.New(cfg)
		if err := s.Send(); err != nil {
			log.Printf("[serve] TRANSFER FAILED: %q to %s — %v", fileName, clientAddr, err)
			return
		}
		elapsed := time.Since(start)
		p := s.Progress()
		dataElapsed := elapsed
		repairDur := time.Duration(0)
		if p.RepairStartNs > 0 && p.StartNs > 0 {
			dataElapsed = time.Duration(p.RepairStartNs - p.StartNs)
			repairDur = elapsed - dataElapsed
		}
		mbps := float64(p.TotalBytes) / dataElapsed.Seconds() / 1e6
		if repairDur > 0 {
			log.Printf("[serve] TRANSFER COMPLETE: %q to %s in %s (%.1f MB/s data, +%s repair)",
				fileName, clientAddr, elapsed.Round(time.Millisecond), mbps, repairDur.Round(time.Millisecond))
		} else {
			log.Printf("[serve] TRANSFER COMPLETE: %q to %s in %s (%.1f MB/s)",
				fileName, clientAddr, elapsed.Round(time.Millisecond), mbps)
		}
	}(filePath, req.FileName, pkt.Header.SessionID, clientAddr, req.PubKey[:], req.Encrypted)
}

func handlePushReq(conn *net.UDPConn, clientAddr *net.UDPAddr, pkt *protocol.Packet,
	dir string, manifest map[string]string, manifestMu *sync.RWMutex,
	busy *int32, busyClient *string,
	activeMu *sync.Mutex, activeChan *chan []byte,
	activeChanDrops *atomic.Int64) {

	req, err := protocol.UnmarshalPushReq(pkt.Payload, pkt.Header.Flags&protocol.FlagEncrypted != 0)
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

	// --- Reply PUSH_ACCEPT ---
	// Data flows over the control socket, so the "port" is just the control
	// port the client already knows. The push client uses its existing socket.
	controlPort := uint16(conn.LocalAddr().(*net.UDPAddr).Port)

	// If the push client wants encryption, generate our ephemeral key and
	// derive the session key; include our public key in PUSH_ACCEPT.
	var pushEncKey *[16]byte
	acceptPayload := &protocol.PushAcceptPayload{Port: controlPort}
	acceptHdrFlags := protocol.Flag(0)

	if req.Encrypted {
		ephemPriv, err := protocol.GenerateEphemeralKey()
		if err != nil {
			log.Printf("[serve] generate key for PUSH: %v", err)
			atomic.StoreInt32(busy, 0)
			return
		}
		key, err := protocol.DeriveSessionKey(ephemPriv, req.PubKey[:], pkt.Header.SessionID)
		if err != nil {
			log.Printf("[serve] derive key for PUSH: %v", err)
			atomic.StoreInt32(busy, 0)
			return
		}
		pushEncKey = &key
		copy(acceptPayload.PubKey[:], ephemPriv.PublicKey().Bytes())
		acceptPayload.Encrypted = true
		acceptHdrFlags |= protocol.FlagEncrypted
	}

	acceptPkt := protocol.Packet{
		Header: protocol.Header{
			Type:      protocol.PacketPushAccept,
			SessionID: pkt.Header.SessionID,
			Flags:     acceptHdrFlags,
		},
		Payload: protocol.MarshalPushAccept(acceptPayload),
	}
	raw, err := protocol.MarshalPacket(&acceptPkt)
	if err != nil {
		atomic.StoreInt32(busy, 0)
		return
	}

	ch := make(chan []byte, 256)
	activeMu.Lock()
	*activeChan = ch
	activeMu.Unlock()

	conn.WriteToUDP(raw, clientAddr)

	log.Printf("[serve] ACCEPTED %s: PUSH_REQ for %q", clientAddr, safeName)

	go func(safeName, finalPath string, sessionID uint32, clientAddr *net.UDPAddr, encKey *[16]byte, isEncrypted bool) {
		// Log activeChan drop count every second for diagnostics.
		dropDone := make(chan struct{})
		defer func() {
			close(dropDone)
			atomic.StoreInt32(busy, 0)
			activeMu.Lock()
			*activeChan = nil
			activeMu.Unlock()
		}()

		go func() {
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for {
				select {
				case <-dropDone:
					return
				case <-t.C:
					d := activeChanDrops.Swap(0)
					if d > 0 {
						log.Printf("[serve/diag] activeChan drops: %d/s (cap=%d)", d, cap(ch))
					}
				}
			}
		}()

		tmpPath := finalPath + ".tmp"

		cfg := receiver.DefaultConfig()
		cfg.Conn = conn
		cfg.SenderAddr = clientAddr
		cfg.RecvChan = ch
		cfg.OutputPath = tmpPath
		cfg.Encrypt = isEncrypted
		cfg.EncKey = encKey

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
	}(safeName, finalPath, pkt.Header.SessionID, clientAddr, pushEncKey, req.Encrypted)
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
