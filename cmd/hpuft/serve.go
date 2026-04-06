package main

import (
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"hpuft/protocol"
	"hpuft/receiver"
	"hpuft/sender"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// manifestEntry holds the absolute path and size of a file in the manifest.
type manifestEntry struct {
	path string
	size int64
}

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listenAddr := fs.String("listen", ":9001", "address to listen on for PULL_REQ / PUSH_REQ")
	dir := fs.String("dir", ".", "directory of files available to serve")
	debug := fs.Bool("debug", false, "enable CC/protocol debug logging for transfers")
	masterAddr := fs.String("master", "", "master tracker address (host:port); if set, register this daemon over TCP")
	fs.Parse(args)

	// Serve uses structured event logging on stderr with timestamps.
	// No progress bar — it runs as a daemon and may have no terminal.
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetOutput(os.Stderr)

	var manifestMu sync.RWMutex
	manifest := buildManifest(*dir) // map[name]manifestEntry

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
	conn.SetWriteBuffer(16 * 1024 * 1024)

	fmt.Fprintf(os.Stderr, "[serve] Online. Listening on %s\n", conn.LocalAddr())
	fmt.Fprintf(os.Stderr, "[serve] Manifest loaded: %d authorized files found in %s\n", len(manifest), *dir)

	if *masterAddr != "" {
		udpPort := uint16(conn.LocalAddr().(*net.UDPAddr).Port)
		go runMasterRegistration(*masterAddr, udpPort, &manifestMu, manifest)
	}

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

		// LIST_REQ is always handled immediately, regardless of busy state.
		// Parse speculatively; if it fails or isn't a LIST_REQ, fall through.
		if earlyPkt, err := protocol.UnmarshalPacket(rawBuf[:n]); err == nil {
			if earlyPkt.Header.Type == protocol.PacketListReq {
				handleListReq(conn, clientAddr, &earlyPkt, manifest, &manifestMu)
				continue
			}
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
			handlePullReq(conn, clientAddr, &pkt, manifest, &manifestMu, &busy, &busyClient, &activeMu, &activeChan, *debug)
		case protocol.PacketPushReq:
			handlePushReq(conn, clientAddr, &pkt, *dir, manifest, &manifestMu, &busy, &busyClient, &activeMu, &activeChan, &activeChanDrops, *debug)
		case protocol.PacketListReq:
			handleListReq(conn, clientAddr, &pkt, manifest, &manifestMu)
		}
	}
}

func handlePullReq(conn *net.UDPConn, clientAddr *net.UDPAddr, pkt *protocol.Packet,
	manifest map[string]manifestEntry, manifestMu *sync.RWMutex,
	busy *int32, busyClient *string,
	activeMu *sync.Mutex, activeChan *chan []byte, debug bool) {

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
	entry, ok := manifest[req.FileName]
	manifestMu.RUnlock()
	if !ok {
		sendReject(conn, clientAddr, pkt.Header.SessionID, protocol.RejectFileNotFound)
		atomic.StoreInt32(busy, 0)
		log.Printf("[serve] REJECTED %s: PULL_REQ for %q (Not in manifest)", clientAddr, req.FileName)
		return
	}
	filePath := entry.path

	// Create the forwarding channel before the goroutine starts.
	// The main loop won't forward packets until busy==1, which was set above
	// in this same goroutine (the main read loop), so no race.
	ch := make(chan []byte, 4096)
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
		cfg.Quiet = !debug
		cfg.Debug = debug
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
	dir string, manifest map[string]manifestEntry, manifestMu *sync.RWMutex,
	busy *int32, busyClient *string,
	activeMu *sync.Mutex, activeChan *chan []byte,
	activeChanDrops *atomic.Int64, debug bool) {

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
	// v5.2: single-socket model — no port field in PUSH_ACCEPT.
	// Data flows on the same address:port that received the PUSH_REQ.

	// If the push client wants encryption, generate our ephemeral key and
	// derive the session key; include our public key in PUSH_ACCEPT.
	var pushEncKey *[16]byte
	var pushIVBase *[8]byte
	acceptPayload := &protocol.PushAcceptPayload{}
	acceptHdrFlags := protocol.Flag(0)

	if req.Encrypted {
		ephemPriv, err := protocol.GenerateEphemeralKey()
		if err != nil {
			log.Printf("[serve] generate key for PUSH: %v", err)
			atomic.StoreInt32(busy, 0)
			return
		}
		key, derivedIVBase, err := protocol.DeriveSessionKey(ephemPriv, req.PubKey[:], pkt.Header.SessionID)
		if err != nil {
			log.Printf("[serve] derive key for PUSH: %v", err)
			atomic.StoreInt32(busy, 0)
			return
		}
		pushEncKey = &key
		pushIVBase = &derivedIVBase
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

	ch := make(chan []byte, 4096)
	activeMu.Lock()
	*activeChan = ch
	activeMu.Unlock()

	conn.WriteToUDP(raw, clientAddr)

	log.Printf("[serve] ACCEPTED %s: PUSH_REQ for %q", clientAddr, safeName)

	go func(safeName, finalPath string, sessionID uint32, clientAddr *net.UDPAddr, encKey *[16]byte, ivBase *[8]byte, isEncrypted bool) {
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
		cfg.IVBase = ivBase
		cfg.Debug = debug

		r, err := receiver.New(cfg)
		if err != nil {
			log.Printf("[serve] PUSH init error for %q: %v", safeName, err)
			return
		}

		start := time.Now()
		if err := r.Run(); err != nil {
			log.Printf("[serve] PUSH FAILED: %q from %s — %v", safeName, clientAddr, err)
			// Leave .tmp and sidecar for potential resume (unless hash mismatch,
			// which receiver.Run already cleans up).
			return
		}
		elapsed := time.Since(start)

		// Promote .tmp → final and clean up sidecar.
		receiver.DeleteCheckpoint(tmpPath)
		if err := os.Rename(tmpPath, finalPath); err != nil {
			log.Printf("[serve] PUSH promote failed for %q: %v", safeName, err)
			os.Remove(tmpPath)
			return
		}

		// Add to manifest
		absPath, _ := filepath.Abs(finalPath)
		fi, _ := os.Stat(absPath)
		var addedSize int64
		if fi != nil {
			addedSize = fi.Size()
		}
		manifestMu.Lock()
		manifest[safeName] = manifestEntry{path: absPath, size: addedSize}
		manifestMu.Unlock()

		p := r.Progress()
		mbps := float64(p.TotalBytes) / elapsed.Seconds() / 1e6
		log.Printf("[serve] PUSH COMPLETE: %q from %s in %s (%.1f MB/s) — added to manifest",
			safeName, clientAddr, elapsed.Round(time.Millisecond), mbps)
	}(safeName, finalPath, pkt.Header.SessionID, clientAddr, pushEncKey, pushIVBase, req.Encrypted)
}

func handleListReq(conn *net.UDPConn, clientAddr *net.UDPAddr, pkt *protocol.Packet,
	manifest map[string]manifestEntry, manifestMu *sync.RWMutex) {

	manifestMu.RLock()
	entries := make([]protocol.ListEntry, 0, len(manifest))
	for name, e := range manifest {
		entries = append(entries, protocol.ListEntry{Name: name, Size: uint64(e.size)})
	}
	manifestMu.RUnlock()

	respPkt := protocol.Packet{
		Header: protocol.Header{
			Type:      protocol.PacketListResp,
			SessionID: pkt.Header.SessionID,
		},
		Payload: protocol.MarshalListResp(entries),
	}
	raw, err := protocol.MarshalPacket(&respPkt)
	if err != nil {
		return
	}
	conn.WriteToUDP(raw, clientAddr)
}

// buildManifest scans dir (non-recursively) and returns an allowlist map of
// filename → manifestEntry{path, size}. Only regular files are included;
// directories and symlinks are skipped.
func buildManifest(dir string) map[string]manifestEntry {
	manifest := make(map[string]manifestEntry)

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
		info, err := entry.Info()
		if err != nil {
			log.Printf("[serve] skipping %q: %v", name, err)
			continue
		}
		manifest[name] = manifestEntry{path: abs, size: info.Size()}
	}

	return manifest
}

// runMasterRegistration dials masterAddr over TCP, sends a Burst (0x01) with the
// current manifest state, then keeps the connection alive with Heartbeat (0x00)
// frames every 50 seconds. On any error it reconnects with exponential backoff.
func runMasterRegistration(masterAddr string, udpPort uint16, manifestMu *sync.RWMutex, manifest map[string]manifestEntry) {
	backoff := time.Second
	for {
		conn, err := net.Dial("tcp", masterAddr)
		if err != nil {
			log.Printf("[master] connect %s: %v — retry in %s", masterAddr, err, backoff)
			time.Sleep(backoff)
			if backoff < 64*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
		log.Printf("[master] connected to %s", masterAddr)

		if err := sendMasterBurst(conn, udpPort, manifestMu, manifest); err != nil {
			log.Printf("[master] burst: %v", err)
			conn.Close()
			continue
		}

		ticker := time.NewTicker(50 * time.Second)
		lost := false
		for range ticker.C {
			if err := sendMasterHeartbeat(conn); err != nil {
				log.Printf("[master] heartbeat: %v", err)
				lost = true
				break
			}
		}
		ticker.Stop()
		conn.Close()
		if lost {
			time.Sleep(time.Second)
		}
	}
}

// sendMasterBurst builds and writes a 0x01 Burst frame to conn.
//
// Wire layout:
//
//	[4] total length  — bytes after this field (type + port + count + file entries)
//	[1] type          — 0x01
//	[2] UDP port
//	[4] file count
//	per file:
//	  [32] SHA-256 hash
//	  [8]  size (uint64)
//	  [2]  name length (uint16)
//	  [N]  name bytes
func sendMasterBurst(conn net.Conn, udpPort uint16, manifestMu *sync.RWMutex, manifest map[string]manifestEntry) error {
	type entry struct {
		hash [32]byte
		size uint64
		name string
	}

	manifestMu.RLock()
	entries := make([]entry, 0, len(manifest))
	for name, me := range manifest {
		entries = append(entries, entry{size: uint64(me.size), name: name})
		// capture path for hashing after unlock
		entries[len(entries)-1].hash = sha256File(me.path)
	}
	manifestMu.RUnlock()

	// Compute payload size: port(2) + count(4) + per-file(32+8+2+namelen)
	payloadSize := 2 + 4
	for i := range entries {
		payloadSize += 32 + 8 + 2 + len(entries[i].name)
	}

	// totalLen = type(1) + payloadSize
	totalLen := 1 + payloadSize
	buf := make([]byte, 4+totalLen)
	off := 0

	binary.BigEndian.PutUint32(buf[off:], uint32(totalLen))
	off += 4
	buf[off] = 0x01
	off++
	binary.BigEndian.PutUint16(buf[off:], udpPort)
	off += 2
	binary.BigEndian.PutUint32(buf[off:], uint32(len(entries)))
	off += 4

	for _, e := range entries {
		copy(buf[off:], e.hash[:])
		off += 32
		binary.BigEndian.PutUint64(buf[off:], e.size)
		off += 8
		binary.BigEndian.PutUint16(buf[off:], uint16(len(e.name)))
		off += 2
		copy(buf[off:], e.name)
		off += len(e.name)
	}

	_, err := conn.Write(buf)
	return err
}

// sendMasterHeartbeat writes a 0x00 Heartbeat frame to conn.
//
// Wire layout: [4] length=0  [1] type=0x00
func sendMasterHeartbeat(conn net.Conn) error {
	var frame [5]byte
	// length field stays 0x00000000; type byte is 0x00
	_, err := conn.Write(frame[:])
	return err
}

// sha256File computes the SHA-256 hash of the file at path.
// Returns a zero hash on error (logged by caller).
func sha256File(path string) [32]byte {
	f, err := os.Open(path)
	if err != nil {
		return [32]byte{}
	}
	defer f.Close()
	h := sha256.New()
	io.Copy(h, f)
	var out [32]byte
	h.Sum(out[:0])
	return out
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
