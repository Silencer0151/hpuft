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
	listenAddr := fs.String("listen", ":9001", "address to listen on")
	dir := fs.String("dir", ".", "directory of files to serve")
	debug := fs.Bool("debug", false, "enable CC/protocol debug logging for transfers")
	masterAddr := fs.String("master", "", "master tracker address (host:port)")
	fs.Parse(args)

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
	conn.SetWriteBuffer(16 * 1024 * 1024)

	fmt.Fprintf(os.Stderr, "[serve] Online. Listening on %s\n", conn.LocalAddr())
	fmt.Fprintf(os.Stderr, "[serve] Manifest loaded: %d files found in %s\n", len(manifest), *dir)

	if *masterAddr != "" {
		udpPort := uint16(conn.LocalAddr().(*net.UDPAddr).Port)
		go runMasterRegistration(*masterAddr, udpPort, &manifestMu, manifest)
	}

	conns := newConnTable()

	// Busy model: single-lane — one transfer at a time preserves CC correctness.
	// Multiple connections may coexist idle; only one may be ConnTransferring.
	var busy atomic.Int32
	var busyClient string // written/read only in the main loop goroutine; no extra sync needed

	// Background idle-connection reaper.
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for range t.C {
			conns.reapIdle(time.Now())
		}
	}()

	rawBuf := make([]byte, protocol.MTUHardCap)
	for {
		n, clientAddr, err := conn.ReadFromUDP(rawBuf)
		if err != nil {
			log.Printf("[serve] read error: %v", err)
			continue
		}

		hdr, herr := protocol.UnmarshalHeader(rawBuf[:n])
		if herr != nil {
			continue
		}

		// ── Level 1: connection-establishment packets ──────────────────────
		switch hdr.Type {
		case protocol.PacketHello:
			handleHello(conn, clientAddr, hdr, rawBuf[:n], conns)
			continue
		case protocol.PacketPing:
			if sc := conns.get(clientAddr, hdr.ConnectionID); sc != nil {
				sc.SendPong()
				sc.touch()
			}
			continue
		}

		// ── Level 2: per-connection dispatch ──────────────────────────────
		sc := conns.get(clientAddr, hdr.ConnectionID)
		if sc == nil {
			continue // drop packets from unknown connections
		}
		sc.touch()

		switch hdr.Type {
		case protocol.PacketRequest:
			handleRequest(sc, rawBuf[:n], conn, *dir, manifest, &manifestMu, &busy, &busyClient, *debug)

		case protocol.PacketReject:
			// Client is disconnecting; remove and free state.
			conns.remove(clientAddr, hdr.ConnectionID)
			log.Printf("[serve] DISCONNECTED %s (ID=0x%08x)", clientAddr, hdr.ConnectionID)

		case protocol.PacketData, protocol.PacketParity,
			protocol.PacketHeartbeat, protocol.PacketComplete,
			protocol.PacketAckClose:
			ch := sc.getTransferChan()
			if ch == nil {
				continue
			}
			raw := make([]byte, n)
			copy(raw, rawBuf[:n])
			select {
			case ch <- protocol.RawPacket{Data: raw, Src: clientAddr}:
			default: // drop; sender/receiver will retransmit
			}
		}
	}
}

// ── HELLO handler ─────────────────────────────────────────────────────────────

func handleHello(conn *net.UDPConn, addr *net.UDPAddr, hdr protocol.Header, raw []byte, conns *connTable) {
	// If the ConnectionID is already in use, reject it so the client retries
	// with a new random ID.
	//
	// ConnTransferring → genuine collision; hard reject.
	// ConnIdle         → orphaned entry from a previous session that ended
	//                    without a clean REJECT(ClientDisconnect). Silently
	//                    evict the dead entry and send REJECT(Collision) so
	//                    DialConnection picks a fresh ID. This avoids creating
	//                    a phantom connection the client will never use and
	//                    keeps the connTable clean without log spam.
	if existing := conns.get(addr, hdr.ConnectionID); existing != nil {
		if existing.State() == protocol.ConnTransferring {
			sendReject(conn, addr, hdr.ConnectionID, protocol.ReasonConnectionIDCollision)
			log.Printf("[serve] REJECTED %s: HELLO collision (0x%08x, transfer active)", addr, hdr.ConnectionID)
			return
		}
		// Idle orphan — evict silently and tell the client to try a new ID.
		conns.remove(addr, hdr.ConnectionID)
		sendReject(conn, addr, hdr.ConnectionID, protocol.ReasonConnectionIDCollision)
		return
	}

	// Extract the HELLO payload (everything after the fixed header).
	if len(raw) < protocol.HeaderSize {
		return
	}
	helloPayload := raw[protocol.HeaderSize:]

	c, welcomeRaw, err := protocol.AcceptConnection(conn, addr, hdr.ConnectionID, helloPayload)
	if err != nil {
		log.Printf("[serve] AcceptConnection from %s: %v", addr, err)
		return
	}

	sc := &serverConn{Connection: c}
	sc.touch()
	conns.put(sc)

	if _, err := conn.WriteToUDP(welcomeRaw, addr); err != nil {
		log.Printf("[serve] send WELCOME to %s: %v", addr, err)
		conns.remove(addr, hdr.ConnectionID)
		return
	}
	log.Printf("[serve] CONNECTED %s (ID=0x%08x)", addr, hdr.ConnectionID)
}

// ── REQUEST dispatcher ────────────────────────────────────────────────────────

func handleRequest(sc *serverConn, raw []byte, conn *net.UDPConn, dir string,
	manifest map[string]manifestEntry, manifestMu *sync.RWMutex,
	busy *atomic.Int32, busyClient *string, debug bool) {

	plaintext, err := sc.DecryptRequest(raw)
	if err != nil {
		log.Printf("[serve] decrypt REQUEST from %s: %v", sc.RemoteAddr, err)
		return
	}

	op, body, err := protocol.ParseRequest(plaintext)
	if err != nil {
		hdr, _ := protocol.UnmarshalHeader(raw)
		sendEncryptedResponse(sc, hdr.SequenceNum,
			protocol.BuildResponseError(protocol.ReasonInvalidRequest, err.Error()))
		return
	}

	hdr, _ := protocol.UnmarshalHeader(raw)
	reqID := hdr.SequenceNum

	switch op {
	case protocol.OpPut:
		handleOpPut(sc, reqID, body, conn, dir, manifest, manifestMu, busy, busyClient, debug)
	case protocol.OpGet:
		handleOpGet(sc, reqID, body, conn, manifest, manifestMu, busy, busyClient, debug)
	case protocol.OpList:
		handleOpList(sc, reqID, manifest, manifestMu)
	case protocol.OpDelete:
		handleOpDelete(sc, reqID, body, dir, manifest, manifestMu)
	default:
		sendEncryptedResponse(sc, reqID,
			protocol.BuildResponseError(protocol.ReasonInvalidRequest, fmt.Sprintf("unknown op 0x%02x", op)))
	}
}

// ── OpPut ─────────────────────────────────────────────────────────────────────

func handleOpPut(sc *serverConn, reqID uint64, body []byte, conn *net.UDPConn, dir string,
	manifest map[string]manifestEntry, manifestMu *sync.RWMutex,
	busy *atomic.Int32, busyClient *string, debug bool) {

	// PUT body: FileSize(8) | FileHash(8) | InitialRate(4) | FileName(null-term)
	if len(body) < 21 {
		sendEncryptedResponse(sc, reqID,
			protocol.BuildResponseError(protocol.ReasonInvalidRequest, "PUT body too short"))
		return
	}
	fileSize := binary.BigEndian.Uint64(body[0:8])
	fileHash := binary.BigEndian.Uint64(body[8:16])
	initialRate := binary.BigEndian.Uint32(body[16:20])
	fileName := nullTermStr(body[20:])

	safeName := filepath.Base(fileName)
	if safeName == "." || safeName == ".." || safeName == "" {
		sendEncryptedResponse(sc, reqID,
			protocol.BuildResponseError(protocol.ReasonInvalidRequest, "invalid filename"))
		log.Printf("[serve] REJECTED %s: PUT invalid filename %q", sc.RemoteAddr, fileName)
		return
	}

	// Busy check.
	if !busy.CompareAndSwap(0, 1) {
		sendEncryptedResponse(sc, reqID,
			protocol.BuildResponseError(protocol.ReasonServerBusy, ""))
		log.Printf("[serve] REJECTED %s: SERVER_BUSY (transferring to %s)", sc.RemoteAddr, *busyClient)
		return
	}
	*busyClient = sc.RemoteAddr.String()

	// No-overwrite check.
	finalPath := filepath.Join(dir, safeName)
	if _, err := os.Stat(finalPath); err == nil {
		sendEncryptedResponse(sc, reqID,
			protocol.BuildResponseError(protocol.ReasonFileExists, ""))
		busy.Store(0)
		log.Printf("[serve] REJECTED %s: PUT %q already exists", sc.RemoteAddr, safeName)
		return
	}

	// Accept: send RESPONSE OK, wire up the transfer channel, spawn goroutine.
	sendEncryptedResponse(sc, reqID, protocol.BuildPutResponseOK())

	ch := make(chan protocol.RawPacket, 4096)
	sc.setTransferChan(ch)
	sc.SetState(protocol.ConnTransferring)

	log.Printf("[serve] ACCEPTED %s: PUT %q", sc.RemoteAddr, safeName)

	go func() {
		defer func() {
			sc.setTransferChan(nil)
			sc.SetState(protocol.ConnIdle)
			busy.Store(0)
		}()

		tmpPath := finalPath + ".tmp"

		cfg := receiver.DefaultConfig()
		cfg.Conn = conn
		cfg.SenderAddr = sc.RemoteAddr
		cfg.RecvChan = ch
		cfg.OutputPath = tmpPath
		cfg.EncKey = &sc.SessionKey
		cfg.IVBase = &sc.IVBase
		cfg.Debug = debug
		cfg.IncomingSession = &receiver.IncomingSession{
			SenderAddr:   sc.RemoteAddr,
			ConnectionID: sc.ID,
			Meta: receiver.TransferMeta{
				FileName:    safeName,
				FileSize:    fileSize,
				FileHash:    fileHash,
				InitialRate: initialRate,
			},
		}

		r, err := receiver.New(cfg)
		if err != nil {
			log.Printf("[serve] PUT init error for %q: %v", safeName, err)
			return
		}

		start := time.Now()
		if err := r.Run(); err != nil {
			log.Printf("[serve] PUT FAILED: %q from %s — %v", safeName, sc.RemoteAddr, err)
			return
		}
		elapsed := time.Since(start)

		receiver.DeleteCheckpoint(tmpPath)
		if err := os.Rename(tmpPath, finalPath); err != nil {
			log.Printf("[serve] PUT promote failed for %q: %v", safeName, err)
			os.Remove(tmpPath)
			return
		}

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
		log.Printf("[serve] PUT COMPLETE: %q from %s in %s (%.1f MB/s) — added to manifest",
			safeName, sc.RemoteAddr, elapsed.Round(time.Millisecond), mbps)
	}()
}

// ── OpGet ─────────────────────────────────────────────────────────────────────

func handleOpGet(sc *serverConn, reqID uint64, body []byte, conn *net.UDPConn,
	manifest map[string]manifestEntry, manifestMu *sync.RWMutex,
	busy *atomic.Int32, busyClient *string, debug bool) {

	// GET body: FileName(null-term)
	fileName := nullTermStr(body)
	if fileName == "" {
		sendEncryptedResponse(sc, reqID,
			protocol.BuildResponseError(protocol.ReasonInvalidRequest, "missing filename"))
		return
	}

	// Manifest lookup.
	manifestMu.RLock()
	entry, ok := manifest[fileName]
	manifestMu.RUnlock()
	if !ok {
		sendEncryptedResponse(sc, reqID,
			protocol.BuildResponseError(protocol.ReasonFileNotFound, ""))
		log.Printf("[serve] REJECTED %s: GET %q (not in manifest)", sc.RemoteAddr, fileName)
		return
	}

	// Busy check.
	if !busy.CompareAndSwap(0, 1) {
		sendEncryptedResponse(sc, reqID,
			protocol.BuildResponseError(protocol.ReasonServerBusy, ""))
		log.Printf("[serve] REJECTED %s: SERVER_BUSY (transferring to %s)", sc.RemoteAddr, *busyClient)
		return
	}
	*busyClient = sc.RemoteAddr.String()

	// Hash the file for the response metadata.
	fileHash, err := receiver.HashFile(entry.path)
	if err != nil {
		sendEncryptedResponse(sc, reqID,
			protocol.BuildResponseError(protocol.ReasonFileNotFound, ""))
		busy.Store(0)
		log.Printf("[serve] GET hash failed for %q: %v", fileName, err)
		return
	}

	// Accept: send RESPONSE OK with transfer metadata, wire up channel.
	sendEncryptedResponse(sc, reqID,
		protocol.BuildGetResponseOK(uint64(entry.size), fileHash, 0))

	ch := make(chan protocol.RawPacket, 4096)
	sc.setTransferChan(ch)
	sc.SetState(protocol.ConnTransferring)

	log.Printf("[serve] ACCEPTED %s: GET %q", sc.RemoteAddr, fileName)

	go func(filePath, fileName string) {
		defer func() {
			sc.setTransferChan(nil)
			sc.SetState(protocol.ConnIdle)
			busy.Store(0)
		}()

		cfg := sender.DefaultConfig()
		cfg.FilePath = filePath
		cfg.RemoteAddr = sc.RemoteAddr.String()
		cfg.ConnectionID = sc.ID
		cfg.MuxConn = conn
		cfg.MuxAddr = sc.RemoteAddr
		cfg.RecvChan = ch
		cfg.EncKey = &sc.SessionKey
		cfg.IVBase = &sc.IVBase
		cfg.Quiet = !debug
		cfg.Debug = debug

		start := time.Now()
		s := sender.New(cfg)
		if err := s.Send(); err != nil {
			log.Printf("[serve] GET FAILED: %q to %s — %v", fileName, sc.RemoteAddr, err)
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
			log.Printf("[serve] GET COMPLETE: %q to %s in %s (%.1f MB/s data, +%s repair)",
				fileName, sc.RemoteAddr, elapsed.Round(time.Millisecond), mbps, repairDur.Round(time.Millisecond))
		} else {
			log.Printf("[serve] GET COMPLETE: %q to %s in %s (%.1f MB/s)",
				fileName, sc.RemoteAddr, elapsed.Round(time.Millisecond), mbps)
		}
	}(entry.path, fileName)
}

// ── OpList ────────────────────────────────────────────────────────────────────

func handleOpList(sc *serverConn, reqID uint64,
	manifest map[string]manifestEntry, manifestMu *sync.RWMutex) {

	manifestMu.RLock()
	entries := make([]protocol.ListEntry, 0, len(manifest))
	for name, e := range manifest {
		entries = append(entries, protocol.ListEntry{Name: name, Size: uint64(e.size)})
	}
	manifestMu.RUnlock()

	body := protocol.MarshalListBody(entries)
	sendEncryptedResponse(sc, reqID, protocol.BuildListResponseOK(body))
	log.Printf("[serve] LIST for %s (%d entries)", sc.RemoteAddr, len(entries))
}

// ── OpDelete ──────────────────────────────────────────────────────────────────

func handleOpDelete(sc *serverConn, reqID uint64, body []byte, dir string,
	manifest map[string]manifestEntry, manifestMu *sync.RWMutex) {

	// DELETE body: FileName(null-term)
	fileName := nullTermStr(body)
	safeName := filepath.Base(fileName)
	if safeName == "." || safeName == ".." || safeName == "" {
		sendEncryptedResponse(sc, reqID,
			protocol.BuildResponseError(protocol.ReasonInvalidRequest, "invalid filename"))
		return
	}

	// Must be in manifest (authorization check).
	manifestMu.RLock()
	_, ok := manifest[safeName]
	manifestMu.RUnlock()
	if !ok {
		sendEncryptedResponse(sc, reqID,
			protocol.BuildResponseError(protocol.ReasonFileNotFound, ""))
		log.Printf("[serve] REJECTED %s: DELETE %q (not in manifest)", sc.RemoteAddr, safeName)
		return
	}

	finalPath := filepath.Join(dir, safeName)
	if err := os.Remove(finalPath); err != nil {
		sendEncryptedResponse(sc, reqID,
			protocol.BuildResponseError(protocol.ReasonDeleteDenied, err.Error()))
		log.Printf("[serve] DELETE failed for %q: %v", safeName, err)
		return
	}

	manifestMu.Lock()
	delete(manifest, safeName)
	manifestMu.Unlock()

	sendEncryptedResponse(sc, reqID, protocol.BuildDeleteResponseOK())
	log.Printf("[serve] DELETE COMPLETE: %q by %s", safeName, sc.RemoteAddr)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// sendEncryptedResponse encrypts plaintext as a RESPONSE and writes it to the
// client. It is the counterpart to SendRequest on the client side.
func sendEncryptedResponse(sc *serverConn, reqID uint64, plaintext []byte) {
	raw, err := sc.EncryptResponse(reqID, plaintext)
	if err != nil {
		log.Printf("[serve] encrypt response to %s: %v", sc.RemoteAddr, err)
		return
	}
	if _, err := sc.Conn.WriteToUDP(raw, sc.RemoteAddr); err != nil {
		log.Printf("[serve] write response to %s: %v", sc.RemoteAddr, err)
	}
}

// sendReject sends a plain (unencrypted) REJECT packet. Used during HELLO
// before a session key has been established.
func sendReject(conn *net.UDPConn, addr *net.UDPAddr, connectionID uint32, reason protocol.Reason) {
	pkt := protocol.Packet{
		Header: protocol.Header{
			Type:         protocol.PacketReject,
			ConnectionID: connectionID,
		},
		Payload: []byte{byte(reason)},
	}
	raw, err := protocol.MarshalPacket(&pkt)
	if err != nil {
		return
	}
	conn.WriteToUDP(raw, addr)
}

// nullTermStr reads a null-terminated C-style string from data, returning
// everything before the first zero byte (or all bytes if none found).
func nullTermStr(data []byte) string {
	for i, b := range data {
		if b == 0 {
			return string(data[:i])
		}
	}
	return string(data)
}

// ── Manifest ──────────────────────────────────────────────────────────────────

// buildManifest scans dir non-recursively and returns an allowlist map of
// filename → manifestEntry{path, size}. Only regular files are included.
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

// ── Master tracker registration ───────────────────────────────────────────────

// runMasterRegistration dials masterAddr over TCP, sends a Burst (0x01) with
// the current manifest, then keeps the connection alive with Heartbeat (0x00)
// frames every 50 seconds. Reconnects with exponential backoff on any error.
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
		entries[len(entries)-1].hash = sha256File(me.path)
	}
	manifestMu.RUnlock()

	// Payload size: port(2) + count(4) + per-file(32+8+2+namelen)
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
// Wire layout: [4] length=1  [1] type=0x00
func sendMasterHeartbeat(conn net.Conn) error {
	var frame [5]byte
	binary.BigEndian.PutUint32(frame[0:4], 1)
	frame[4] = 0x00
	_, err := conn.Write(frame[:])
	return err
}

// sha256File computes the SHA-256 hash of the file at path.
// Returns a zero hash on error.
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
