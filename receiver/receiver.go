package receiver

import (
	"crypto/cipher"
	"fmt"
	"hpuft/protocol"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

// TransferMeta holds the file metadata extracted from a PUT request.
// It replaces the old SessionReqPayload in the IncomingSession path.
type TransferMeta struct {
	FileName    string
	FileSize    uint64
	FileHash    uint64
	InitialRate uint32
}

// IncomingSession holds the pre-established connection state handed off from
// the serve daemon (or get command) to the receiver. In v6 this is the only
// entry path — the receiver never waits for a SESSION_REQ on the wire.
type IncomingSession struct {
	SenderAddr   *net.UDPAddr
	ConnectionID uint32
	Meta         TransferMeta
}

// Config holds all receiver configuration.
type Config struct {
	ListenAddr string // e.g., ":9000"
	OutputDir  string // directory to write received files
	Session    protocol.SessionConfig

	// Conn, if non-nil, is used instead of binding a new socket on ListenAddr.
	// The caller retains ownership and is responsible for closing it.
	Conn *net.UDPConn

	// IncomingSession, if non-nil, skips Phase 1 (SESSION_REQ wait).
	// Must be set together with Conn.
	IncomingSession *IncomingSession

	// OutputPath, if non-empty, overrides the default OutputDir+filename path.
	// Used by serve's push handler to write directly to a .tmp staging path.
	OutputPath string

	// SenderAddr, if non-nil, is used as the destination for heartbeats and
	// TRANSFER_COMPLETE in mux mode (when RecvChan is set). In socket mode
	// the sender address is captured from the SESSION_REQ source.
	SenderAddr *net.UDPAddr

	// RecvChan, if non-nil, supplies incoming packets instead of reading from
	// the socket. The serve daemon's control loop forwards packets here so the
	// receiver goroutine never needs its own socket read path.
	// SenderAddr must also be set when RecvChan is set.
	RecvChan <-chan []byte

	// Debug enables verbose protocol logging to stderr.
	// When false noisy internal logs are suppressed and the caller shows a
	// progress bar instead.
	Debug bool

	// EncKey is a pre-derived 16-byte AES-128 session key (mandatory in v6).
	// Set by the CLI after the HELLO/WELCOME handshake via protocol.DialConnection.
	EncKey *[16]byte

	// IVBase is the 8-byte iv_base derived alongside EncKey via HKDF.
	// Must always be set together with EncKey.
	IVBase *[8]byte
}

// ReceiverProgress is a snapshot of live transfer metrics for the progress bar.
type ReceiverProgress struct {
	BytesReceived int64
	TotalBytes    int64
	Rebuilt       int64 // FEC-recovered packets so far
	StartNs       int64
}

// DefaultConfig returns a receiver config with spec defaults.
func DefaultConfig() Config {
	return Config{
		ListenAddr: ":9000",
		OutputDir:  ".",
		Session:    protocol.DefaultSessionConfig(),
	}
}

// Receiver manages a UDP socket and handles incoming file transfers.
type Receiver struct {
	cfg     Config
	conn    *net.UDPConn
	ownConn bool // true if we created the conn (must close on Run exit)

	// Progress atomics — written by Run(), read lock-free by progress bar.
	bytesReceived atomic.Int64
	totalBytes    atomic.Int64
	rebuilt       atomic.Int64
	startNs       atomic.Int64

	// Session info set by Run() for external disconnect signaling.
	senderAddr atomic.Pointer[net.UDPAddr]
	sessionID  atomic.Uint32
}

// Progress returns a live snapshot for the progress bar goroutine.
func (r *Receiver) Progress() ReceiverProgress {
	return ReceiverProgress{
		BytesReceived: r.bytesReceived.Load(),
		TotalBytes:    r.totalBytes.Load(),
		Rebuilt:       r.rebuilt.Load(),
		StartNs:       r.startNs.Load(),
	}
}

// SendDisconnect sends a SESSION_REJECT (CLIENT_DISCONNECT) to the sender.
// Safe to call from a signal handler while Run() is active. No-op if the
// session hasn't been established yet.
func (r *Receiver) SendDisconnect() {
	addr := r.senderAddr.Load()
	sid := r.sessionID.Load()
	if addr == nil || sid == 0 {
		return
	}
	pkt := protocol.Packet{
		Header: protocol.Header{
			Type:      protocol.PacketReject,
			ConnectionID: sid,
		},
		Payload: []byte{byte(protocol.ReasonClientDisconnect)},
	}
	if raw, err := protocol.MarshalPacket(&pkt); err == nil {
		r.conn.WriteToUDP(raw, addr)
	}
}

// New creates a new Receiver. If cfg.Conn is non-nil it is used directly
// (caller owns it); otherwise a new socket is bound on cfg.ListenAddr.
func New(cfg Config) (*Receiver, error) {
	if cfg.Conn != nil {
		return &Receiver{cfg: cfg, conn: cfg.Conn, ownConn: false}, nil
	}

	addr, err := net.ResolveUDPAddr("udp", cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve listen addr: %w", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen udp: %w", err)
	}

	// Set a large read buffer to reduce kernel drops under burst
	conn.SetReadBuffer(16 * 1024 * 1024) // 16 MB

	return &Receiver{cfg: cfg, conn: conn, ownConn: true}, nil
}

// Run listens for and handles a single file transfer, then returns.
// For Phase 1 this handles one session at a time.
func (r *Receiver) Run() error {
	if r.ownConn {
		defer r.conn.Close()
	}

	var dbgLog *log.Logger
	if r.cfg.Debug {
		dbgLog = log.New(os.Stderr, "", log.Ltime|log.Lmicroseconds)
	} else {
		dbgLog = log.New(io.Discard, "", 0)
	}

	// --- v6: EncKey and IVBase are mandatory (pre-derived via HELLO/WELCOME) ---
	if r.cfg.EncKey == nil || r.cfg.IVBase == nil {
		return fmt.Errorf("v6 receiver requires pre-derived EncKey and IVBase")
	}

	// --- Phase 1: Unpack IncomingSession (only entry path in v6) ---
	if r.cfg.IncomingSession == nil {
		return fmt.Errorf("v6 receiver requires IncomingSession to be set")
	}
	rawBuf := make([]byte, protocol.MTUHardCap)
	senderAddr := r.cfg.IncomingSession.SenderAddr
	sessionID := r.cfg.IncomingSession.ConnectionID
	meta := r.cfg.IncomingSession.Meta
	dbgLog.Printf("[receiver] session handed off: connectionID=0x%08X file=%q size=%d",
		sessionID, meta.FileName, meta.FileSize)

	// Publish session info for external disconnect signaling (e.g. Ctrl+C).
	r.senderAddr.Store(senderAddr)
	r.sessionID.Store(sessionID)

	// --- Encryption setup (v6: always pre-derived via HELLO/WELCOME handshake) ---
	aead, err := protocol.NewSessionCipher(*r.cfg.EncKey)
	if err != nil {
		return fmt.Errorf("init session cipher: %w", err)
	}
	ivBase := *r.cfg.IVBase

	// --- Phase 2: Validate and allocate buffer ---
	chunkSize := protocol.MaxPayload

	// Guard against corrupted SESSION_REQ (e.g., from proxy packet loss)
	const maxFileSize = 1 << 40 // 1 TB sanity limit
	if meta.FileSize == 0 || meta.FileSize > maxFileSize {
		return fmt.Errorf("invalid file size in SESSION_REQ: %d bytes (max %d)", meta.FileSize, maxFileSize)
	}
	if len(meta.FileName) == 0 {
		return fmt.Errorf("empty filename in SESSION_REQ")
	}

	outputPath := r.cfg.OutputPath
	if outputPath == "" {
		outputPath = filepath.Join(r.cfg.OutputDir, filepath.Base(meta.FileName))
	}

	// --- Resume detection (v5.2: transparent, receiver-side only) ---
	// Check for a .hpudp-ckpt sidecar matching this file. If found, restore
	// the receive state silently — no wire negotiation. The sender always
	// starts from SESSION_REQ / seq=0; the receiver's sliding window reports
	// HighestContiguous in the first heartbeat so the sender advances past
	// already-received data.
	var resumeSeqNum uint64
	var resumeOffset uint64
	resumed := false

	cp, cpTmpPath := FindCheckpoint(r.cfg.OutputDir, meta.FileName, meta.FileSize, meta.FileHash)
	if r.cfg.OutputPath != "" {
		// serve/push mode: check using the explicit output path
		cp2, err2 := ReadCheckpoint(CheckpointPath(r.cfg.OutputPath))
		if err2 == nil && cp2.FileSize == meta.FileSize && cp2.FileHash == meta.FileHash {
			cp = cp2
			cpTmpPath = r.cfg.OutputPath
		}
	}

	if cp != nil && cpTmpPath != "" {
		// Verify the .tmp file still exists and is large enough.
		tmpInfo, statErr := os.Stat(cpTmpPath)
		if statErr == nil && tmpInfo.Size() > 0 {
			resumeSeqNum = cp.HighestContiguous + 1
			resumeOffset = resumeSeqNum * uint64(chunkSize)
			// Clamp to actual on-disk size in case of truncation.
			if uint64(tmpInfo.Size()) < resumeOffset {
				resumeSeqNum = uint64(tmpInfo.Size()) / uint64(chunkSize)
				resumeOffset = resumeSeqNum * uint64(chunkSize)
			}
			if resumeSeqNum > 0 {
				resumed = true
				outputPath = cpTmpPath
				dbgLog.Printf("[receiver] checkpoint found: resuming from seq=%d offset=%d (%s)",
					resumeSeqNum, resumeOffset, cpTmpPath)
			}
		}
	}

	var recvBuf *ReceiveBuffer
	var writer *DiskWriter
	if resumed {
		recvBuf = NewReceiveBufferWithOffset(meta.FileSize, chunkSize, resumeSeqNum)
		var dwErr error
		writer, dwErr = NewDiskWriterForResume(recvBuf, outputPath, meta.FileSize, chunkSize, resumeOffset)
		if dwErr != nil {
			return fmt.Errorf("create resume disk writer: %w", dwErr)
		}
		dbgLog.Printf("[receiver] RESUMED: buffer from seq=%d (%d remaining chunks), writing to %s",
			resumeSeqNum, recvBuf.Stats().TotalChunks-resumeSeqNum, outputPath)
		// Pre-seed bytesReceived so progress bar starts from the resumed offset.
		r.bytesReceived.Store(int64(resumeOffset))
	} else {
		recvBuf = NewReceiveBuffer(meta.FileSize, chunkSize)
		var dwErr error
		writer, dwErr = NewDiskWriter(recvBuf, outputPath, meta.FileSize, chunkSize)
		if dwErr != nil {
			return fmt.Errorf("create disk writer: %w", dwErr)
		}
		dbgLog.Printf("[receiver] allocated buffer: %d chunks of %d bytes, writing to %s",
			recvBuf.Stats().TotalChunks, chunkSize, outputPath)
	}
	defer recvBuf.Close()
	defer writer.Close()

	// Enable checkpoint writing for future resume.
	writer.SetCheckpointConfig(outputPath, meta.FileHash, filepath.Base(meta.FileName), chunkSize)

	r.totalBytes.Store(int64(meta.FileSize))
	r.startNs.Store(time.Now().UnixNano())

	// --- Phase 3: Start heartbeat generator ---
	hbGen := NewHeartbeatGenerator(r.conn, senderAddr, sessionID, recvBuf, writer)
	hbGen.Start()
	defer hbGen.Stop()

	dbgLog.Printf("[receiver] heartbeat generator started")

	// --- Phase 3b: Create FEC block decoder ---
	fecCfg := protocol.DefaultFECConfig()
	blockDecoder := NewBlockDecoder(fecCfg.BlockSize, recvBuf)
	dbgLog.Printf("[receiver] FEC decoder enabled: block_size=%d", fecCfg.BlockSize)

	// --- Phase 4: Receive DATA and PARITY packets ---
	flushTicker := time.NewTicker(50 * time.Millisecond)
	defer flushTicker.Stop()

	lastPacketTime := time.Now()
	inactivityTimeout := time.Duration(r.cfg.Session.InactivityMultiplier) * 100 * time.Millisecond
	// Floor: at least 5 seconds to allow NACK retransmission cycles under loss
	if inactivityTimeout < 5*time.Second {
		inactivityTimeout = 5 * time.Second
	}

	eofReceived := false

	// Debug progress milestones at 25%, 50%, 75%
	transferStart := time.Now()
	nextMilestone := uint64(1) // 1=25%, 2=50%, 3=75%

	for !recvBuf.IsComplete() {
		var pkt protocol.Packet
		var pktErr error

		if r.cfg.RecvChan != nil {
			select {
			case raw, ok := <-r.cfg.RecvChan:
				if !ok {
					return fmt.Errorf("channel closed during transfer")
				}
				lastPacketTime = time.Now()
				pkt, pktErr = decryptAndParse(raw, aead, ivBase)
			case <-time.After(50 * time.Millisecond):
				if time.Since(lastPacketTime) > inactivityTimeout {
					hbGen.Stop()
					return fmt.Errorf("inactivity timeout: no packets for %v", inactivityTimeout)
				}
				writer.Flush()
				continue
			}
		} else {
			r.conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			n, pktSrcAddr, err := r.conn.ReadFromUDP(rawBuf)
			if err != nil {
				if os.IsTimeout(err) {
					if time.Since(lastPacketTime) > inactivityTimeout {
						hbGen.Stop()
						return fmt.Errorf("inactivity timeout: no packets for %v", inactivityTimeout)
					}
					writer.Flush()
					continue
				}
				return fmt.Errorf("read: %w", err)
			}
			lastPacketTime = time.Now()
			pkt, pktErr = decryptAndParse(rawBuf[:n], aead, ivBase)
			// In pull mode the C server spawns a new sender thread on a fresh
			// ephemeral socket. Update the heartbeat destination to that socket's
			// source address so the C sender actually receives our heartbeats.
			if pktErr == nil && pktSrcAddr != nil &&
				(pkt.Header.Type == protocol.PacketData || pkt.Header.Type == protocol.PacketParity) {
				cur := r.senderAddr.Load()
				if cur == nil || cur.Port != pktSrcAddr.Port || cur.IP.String() != pktSrcAddr.IP.String() {
					r.senderAddr.Store(pktSrcAddr)
					hbGen.UpdatePeerAddr(pktSrcAddr)
					senderAddr = pktSrcAddr
				}
			}
		}

		if pktErr != nil {
			continue
		}

		if pkt.Header.ConnectionID != sessionID {
			continue
		}

		switch pkt.Header.Type {
		case protocol.PacketData:
			recvNs := time.Now().UnixNano() // receiver's local time, used only for calibration dispersion

			isNew, err := recvBuf.Insert(pkt.Header.SequenceNum, pkt.Payload)
			if err != nil {
				dbgLog.Printf("[receiver] insert error seq=%d: %v", pkt.Header.SequenceNum, err)
				continue
			}

			// Track metrics for heartbeat
			if isNew {
				hbGen.RecordPacket(len(pkt.Payload))
				r.bytesReceived.Add(int64(len(pkt.Payload)))

				// Debug progress milestones
				if nextMilestone <= 3 {
					threshold := int64(meta.FileSize) * int64(nextMilestone) / 4
					if r.bytesReceived.Load() >= threshold {
						elapsed := time.Since(transferStart).Seconds()
						pct := nextMilestone * 25
						remain := elapsed/float64(pct)*float64(100-pct)
						dbgLog.Printf("[receiver] %d%% complete (%.1fs elapsed, ~%.0fs remaining)",
							pct, elapsed, remain)
						nextMilestone++
					}
				}
			}

			// Echo the sender's own timestamp back so the sender can compute
			// RTT = now_sender - SenderTimestampNs using only its own clock.
			// This avoids cross-machine clock-skew corruption of the RTT estimate.
			// Calibration dispersion still uses the receiver's local arrival time
			// (recvNs) because dispersion measures inter-packet spacing at the
			// receiver, which is correctly measured by the receiver's own clock.
			if pkt.Header.SenderTimestampNs != 0 {
				hbGen.RecordDataReceiveTime(int64(pkt.Header.SenderTimestampNs))
			}
			if pkt.Header.Flags&protocol.FlagCalibrationBurst != 0 {
				hbGen.RecordCalibrationPacket(recvNs)
			}

			// Record in FEC block decoder and attempt reconstruction
			blockDecoder.RecordData(pkt.Header.BlockGroup, pkt.Header.SequenceNum, pkt.Payload)
			recovered := blockDecoder.TryReconstruct(pkt.Header.BlockGroup)
			for _, rs := range recovered {
				payloadSize := chunkSize
				lastSeq := recvBuf.Stats().TotalChunks - 1
				if rs.SeqNum == lastSeq {
					remainder := int(meta.FileSize % uint64(chunkSize))
					if remainder > 0 {
						payloadSize = remainder
					}
				}
				isNew, err := recvBuf.Insert(rs.SeqNum, rs.Payload[:payloadSize])
				if err == nil && isNew {
					hbGen.RecordPacket(payloadSize)
					r.bytesReceived.Add(int64(payloadSize))
					r.rebuilt.Add(1)
				}
			}

			if pkt.Header.Flags&protocol.FlagEndOfFile != 0 {
				eofReceived = true
			}

		case protocol.PacketParity:
			parityIdx := int(pkt.Header.SequenceNum)
			blockDecoder.RecordParity(pkt.Header.BlockGroup, parityIdx, pkt.Payload)

			// Attempt reconstruction — parity arrival may complete a block
			recovered := blockDecoder.TryReconstruct(pkt.Header.BlockGroup)
			for _, rs := range recovered {
				payloadSize := chunkSize
				lastSeq := recvBuf.Stats().TotalChunks - 1
				if rs.SeqNum == lastSeq {
					remainder := int(meta.FileSize % uint64(chunkSize))
					if remainder > 0 {
						payloadSize = remainder
					}
				}
				isNew, err := recvBuf.Insert(rs.SeqNum, rs.Payload[:payloadSize])
				if err == nil && isNew {
					hbGen.RecordPacket(payloadSize)
					r.bytesReceived.Add(int64(payloadSize))
					r.rebuilt.Add(1)
				}
			}

		case protocol.PacketHello:
			continue // duplicate

		default:
			continue
		}

		// Opportunistic flush
		select {
		case <-flushTicker.C:
			writer.Flush()
		default:
		}
	}

	_ = eofReceived

	// --- Phase 5: Stop heartbeat and finalize ---
	// Send one final heartbeat before stopping so the peer always sees at
	// least hb_count=1. For small files (<= 1 chunk) the receive loop may
	// complete before the first 100ms periodic tick fires.
	hbGen.SendFinal()
	hbGen.Stop()

	dbgLog.Printf("[receiver] all %d chunks received, finalizing...", recvBuf.Stats().TotalChunks)

	computedHash, err := writer.Finalize()
	if err != nil {
		return fmt.Errorf("finalize: %w", err)
	}

	stats := recvBuf.Stats()
	fecStats := blockDecoder.Stats()
	dbgLog.Printf("[receiver] transfer stats: received=%d duplicates=%d | FEC: blocks_recovered=%d shards_recovered=%d",
		stats.PacketsReceived, stats.Duplicates, fecStats.BlocksRecovered, fecStats.ShardsRecovered)

	if computedHash != meta.FileHash {
		// Re-hash the file from disk to determine if corruption is in the
		// DiskWriter's incremental hash or in the actual received data.
		diskDiag := ""
		if diskHash, err := HashFile(outputPath); err == nil {
			if diskHash == computedHash {
				diskDiag = fmt.Sprintf(" | disk re-hash matches writer (data corruption in transfer)")
			} else if diskHash == meta.FileHash {
				diskDiag = fmt.Sprintf(" | disk OK but writer hash wrong (DiskWriter bug)")
			} else {
				diskDiag = fmt.Sprintf(" | disk=0x%016X (neither match)", diskHash)
			}
		}

		rejectPkt := protocol.Packet{
			Header: protocol.Header{
				Type:      protocol.PacketReject,
				ConnectionID: sessionID,
			},
			Payload: []byte{byte(protocol.ReasonHashMismatch)},
		}
		raw, _ := protocol.MarshalPacket(&rejectPkt)
		r.conn.WriteToUDP(raw, senderAddr)

		os.Remove(outputPath)
		DeleteCheckpoint(outputPath)
		return fmt.Errorf("hash mismatch: computed 0x%016X, expected 0x%016X | received=%d dups=%d written=%d%s",
			computedHash, meta.FileHash,
			stats.PacketsReceived, stats.Duplicates, writer.BytesWritten(), diskDiag)
	}

	dbgLog.Printf("[receiver] hash verified: 0x%016X", computedHash)

	// Clean up checkpoint sidecar on success.
	DeleteCheckpoint(outputPath)

	// --- Phase 6: Graceful Teardown ---
	completePkt := protocol.Packet{
		Header: protocol.Header{
			Type:      protocol.PacketComplete,
			ConnectionID: sessionID,
		},
	}
	completeRaw, _ := protocol.MarshalPacket(&completePkt)

	ackReceived := false
teardownLoop:
	for attempt := 0; attempt <= r.cfg.Session.ReceiverTeardownRetries; attempt++ {
		if attempt > 0 {
			dbgLog.Printf("[receiver] retransmitting TRANSFER_COMPLETE (attempt %d/%d)",
				attempt, r.cfg.Session.ReceiverTeardownRetries)
		}

		r.conn.WriteToUDP(completeRaw, senderAddr)

		if r.cfg.RecvChan != nil {
			select {
			case raw, ok := <-r.cfg.RecvChan:
				if !ok {
					break teardownLoop
				}
				pkt, err := protocol.UnmarshalPacket(raw)
				if err == nil && pkt.Header.Type == protocol.PacketAckClose && pkt.Header.ConnectionID == sessionID {
					ackReceived = true
					break teardownLoop
				}
			case <-time.After(1 * time.Second):
			}
		} else {
			r.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
			n, _, err := r.conn.ReadFromUDP(rawBuf)
			if err != nil {
				if os.IsTimeout(err) {
					continue
				}
				return fmt.Errorf("read during teardown: %w", err)
			}

			pkt, err := protocol.UnmarshalPacket(rawBuf[:n])
			if err != nil {
				continue
			}

			if pkt.Header.Type == protocol.PacketAckClose && pkt.Header.ConnectionID == sessionID {
				ackReceived = true
				break teardownLoop
			}
		}
	}

	if ackReceived {
		dbgLog.Printf("[receiver] ACK_CLOSE received, entering linger state")
	} else {
		dbgLog.Printf("[receiver] no ACK_CLOSE after retries, proceeding with unilateral teardown (transfer verified)")
	}

	time.Sleep(r.cfg.Session.LingerDuration)

	dbgLog.Printf("[receiver] transfer complete: %s (%d bytes)", outputPath, meta.FileSize)
	return nil
}

// decryptAndParse parses a raw DATA or PARITY packet, decrypting its payload.
// v6 is always encrypted; non-data packet types are parsed without decryption.
func decryptAndParse(raw []byte, aead cipher.AEAD, ivBase [8]byte) (protocol.Packet, error) {
	if len(raw) < protocol.HeaderSize {
		return protocol.Packet{}, protocol.ErrPacketTooSmall
	}
	hdr, err := protocol.UnmarshalHeader(raw)
	if err != nil {
		return protocol.Packet{}, err
	}

	if hdr.Type != protocol.PacketData && hdr.Type != protocol.PacketParity {
		return protocol.UnmarshalPacket(raw)
	}

	// Two PayloadLen conventions exist:
	//   Go senders (EncryptPacket): PayloadLen = plaintext + GCMTagSize
	//     → wire len = HeaderSize + PayloadLen (tag is within PayloadLen)
	//   C senders:                  PayloadLen = plaintext only
	//     → wire len = HeaderSize + PayloadLen + GCMTagSize (tag appended after)
	// Auto-detect by checking if the actual packet is GCMTagSize bytes longer
	// than HeaderSize+PayloadLen; if so, the sender is using the C convention.
	needed := protocol.HeaderSize + int(hdr.PayloadLen)
	if len(raw) == needed+protocol.GCMTagSize {
		needed += protocol.GCMTagSize
	}
	if len(raw) < needed {
		return protocol.Packet{}, fmt.Errorf("encrypted packet too short: %d < %d", len(raw), needed)
	}
	nonce := protocol.BuildNonce(ivBase, hdr.SequenceNum, false)
	pt, err := protocol.DecryptPacket(aead, raw[:needed], nonce)
	if err != nil {
		return protocol.Packet{}, err
	}
	return protocol.Packet{Header: hdr, Payload: pt}, nil
}
