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

// IncomingSession holds a pre-negotiated session for the 'get' command.
// When set on Config, Run() skips Phase 1 (waiting for SESSION_REQ) and
// starts immediately from the already-parsed session data. This allows
// the get command to punch a NAT hole via PULL_REQ, receive the SESSION_REQ
// on its own socket, and hand everything to the receiver without rebinding.
type IncomingSession struct {
	SenderAddr *net.UDPAddr
	SessionID  uint32
	Req        protocol.SessionReqPayload
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

	// Encrypt enables AES-128-GCM decryption (spec §4.5). When true and EncKey
	// is nil, the receiver generates an ephemeral keypair, sends SESSION_ACCEPT
	// carrying its public key, and derives the session key from the sender's
	// public key in SESSION_REQ. When EncKey is non-nil, the pre-derived key is
	// used directly (push or get flows where key exchange happened in the CLI).
	Encrypt bool

	// EncKey, if non-nil, is a pre-derived 16-byte AES-128 session key.
	EncKey *[16]byte
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

	// --- Phase 1: Wait for SESSION_REQ ---
	// Skipped when IncomingSession is set (e.g., 'get' command after PULL_REQ handshake).
	rawBuf := make([]byte, protocol.MTUHardCap)
	var senderAddr *net.UDPAddr
	var sessionID uint32
	var reqPayload protocol.SessionReqPayload

	if r.cfg.IncomingSession != nil {
		senderAddr = r.cfg.IncomingSession.SenderAddr
		sessionID = r.cfg.IncomingSession.SessionID
		reqPayload = r.cfg.IncomingSession.Req
		dbgLog.Printf("[receiver] session handed off from get: sessionID=0x%08X file=%q size=%d",
			sessionID, reqPayload.FileName, reqPayload.FileSize)
	} else if r.cfg.RecvChan != nil {
		// Mux mode: read SESSION_REQ forwarded from the serve control loop.
		// SenderAddr is pre-set from the PUSH_REQ clientAddr captured by serve.
		senderAddr = r.cfg.SenderAddr
		timeout := time.NewTimer(15 * time.Second)
		defer timeout.Stop()
		waitDone := false
		for !waitDone {
			select {
			case raw, ok := <-r.cfg.RecvChan:
				if !ok {
					return fmt.Errorf("channel closed waiting for SESSION_REQ")
				}
				pkt, err := protocol.UnmarshalPacket(raw)
				if err != nil || pkt.Header.Type != protocol.PacketSessionReq {
					continue
				}
				encryptedReq := pkt.Header.Flags&protocol.FlagEncrypted != 0
				req, err := protocol.UnmarshalSessionReq(pkt.Payload, encryptedReq)
				if err != nil {
					continue
				}
				sessionID = pkt.Header.SessionID
				reqPayload = req
				waitDone = true
			case <-timeout.C:
				return fmt.Errorf("timeout waiting for SESSION_REQ")
			}
		}
		dbgLog.Printf("[receiver] SESSION_REQ via channel: sessionID=0x%08X file=%q size=%d",
			sessionID, reqPayload.FileName, reqPayload.FileSize)
	} else {
		dbgLog.Printf("[receiver] listening on %s", r.conn.LocalAddr())

		for {
			n, addr, err := r.conn.ReadFromUDP(rawBuf)
			if err != nil {
				return fmt.Errorf("read: %w", err)
			}

			pkt, err := protocol.UnmarshalPacket(rawBuf[:n])
			if err != nil {
				dbgLog.Printf("[receiver] malformed packet from %s: %v", addr, err)
				continue
			}

			if pkt.Header.Type != protocol.PacketSessionReq {
				continue
			}

			encryptedReq2 := pkt.Header.Flags&protocol.FlagEncrypted != 0
			req, err := protocol.UnmarshalSessionReq(pkt.Payload, encryptedReq2)
			if err != nil {
				dbgLog.Printf("[receiver] malformed SESSION_REQ: %v", err)
				continue
			}

			senderAddr = addr
			sessionID = pkt.Header.SessionID
			reqPayload = req

			dbgLog.Printf("[receiver] SESSION_REQ from %s: sessionID=0x%08X file=%q size=%d checksum=0x%016X",
				addr, sessionID, req.FileName, req.FileSize, req.Checksum)
			break
		}
	}

	// --- Encryption setup ---
	var aead cipher.AEAD
	if r.cfg.Encrypt {
		if r.cfg.EncKey != nil {
			// Pre-derived key: push flow (PUSH_REQ/PUSH_ACCEPT) or get flow.
			c, err := protocol.NewSessionCipher(*r.cfg.EncKey)
			if err != nil {
				return fmt.Errorf("init session cipher: %w", err)
			}
			aead = c
		} else if reqPayload.Encrypted {
			// Direct recv mode: sender embedded its PubKey in SESSION_REQ.
			// Generate our ephemeral key, derive session key, send SESSION_ACCEPT.
			ephemPriv, err := protocol.GenerateEphemeralKey()
			if err != nil {
				return fmt.Errorf("generate ephemeral key: %w", err)
			}
			key, err := protocol.DeriveSessionKey(ephemPriv, reqPayload.PubKey[:], sessionID)
			if err != nil {
				return fmt.Errorf("derive session key: %w", err)
			}
			c, err := protocol.NewSessionCipher(key)
			if err != nil {
				return fmt.Errorf("init cipher: %w", err)
			}
			aead = c

			// Send SESSION_ACCEPT with our public key.
			pubKeyBytes := ephemPriv.PublicKey().Bytes()
			var pubKeyArr [32]byte
			copy(pubKeyArr[:], pubKeyBytes)
			acceptPkt := protocol.Packet{
				Header: protocol.Header{
					Type:      protocol.PacketSessionAccept,
					SessionID: sessionID,
					Flags:     protocol.FlagEncrypted,
				},
				Payload: protocol.MarshalSessionAccept(&protocol.SessionAcceptPayload{
					PubKey: pubKeyArr,
				}),
			}
			acceptRaw, err := protocol.MarshalPacket(&acceptPkt)
			if err != nil {
				return fmt.Errorf("marshal SESSION_ACCEPT: %w", err)
			}
			r.conn.WriteToUDP(acceptRaw, senderAddr)
			dbgLog.Printf("[receiver] sent SESSION_ACCEPT, session key derived")
		}
	}

	// --- Phase 2: Validate and allocate buffer ---
	chunkSize := protocol.MaxPayload
	if aead != nil {
		chunkSize = protocol.MaxEncryptedPayload
	}

	// Guard against corrupted SESSION_REQ (e.g., from proxy packet loss)
	const maxFileSize = 1 << 40 // 1 TB sanity limit
	if reqPayload.FileSize == 0 || reqPayload.FileSize > maxFileSize {
		return fmt.Errorf("invalid file size in SESSION_REQ: %d bytes (max %d)", reqPayload.FileSize, maxFileSize)
	}
	if len(reqPayload.FileName) == 0 {
		return fmt.Errorf("empty filename in SESSION_REQ")
	}

	recvBuf := NewReceiveBuffer(reqPayload.FileSize, chunkSize)
	defer recvBuf.Close()

	outputPath := r.cfg.OutputPath
	if outputPath == "" {
		outputPath = filepath.Join(r.cfg.OutputDir, filepath.Base(reqPayload.FileName))
	}
	writer, err := NewDiskWriter(recvBuf, outputPath, reqPayload.FileSize, chunkSize)
	if err != nil {
		return fmt.Errorf("create disk writer: %w", err)
	}
	defer writer.Close()

	dbgLog.Printf("[receiver] allocated buffer: %d chunks of %d bytes, writing to %s",
		recvBuf.Stats().TotalChunks, chunkSize, outputPath)

	r.totalBytes.Store(int64(reqPayload.FileSize))
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
				pkt, pktErr = decryptAndParse(raw, aead, sessionID)
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
			n, _, err := r.conn.ReadFromUDP(rawBuf)
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
			pkt, pktErr = decryptAndParse(rawBuf[:n], aead, sessionID)
		}

		if pktErr != nil {
			continue
		}

		if pkt.Header.SessionID != sessionID {
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
					remainder := int(reqPayload.FileSize % uint64(chunkSize))
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
					remainder := int(reqPayload.FileSize % uint64(chunkSize))
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

		case protocol.PacketSessionReq:
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

	if computedHash != reqPayload.Checksum {
		dbgLog.Printf("[receiver] HASH MISMATCH: computed=0x%016X expected=0x%016X",
			computedHash, reqPayload.Checksum)

		rejectPkt := protocol.Packet{
			Header: protocol.Header{
				Type:      protocol.PacketSessionReject,
				SessionID: sessionID,
			},
			Payload: []byte{byte(protocol.RejectHashMismatch)},
		}
		raw, _ := protocol.MarshalPacket(&rejectPkt)
		r.conn.WriteToUDP(raw, senderAddr)

		os.Remove(outputPath)
		return fmt.Errorf("hash mismatch: computed 0x%016X, expected 0x%016X",
			computedHash, reqPayload.Checksum)
	}

	dbgLog.Printf("[receiver] hash verified: 0x%016X", computedHash)

	// --- Phase 6: Graceful Teardown ---
	completePkt := protocol.Packet{
		Header: protocol.Header{
			Type:      protocol.PacketTransferComplete,
			SessionID: sessionID,
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
				if err == nil && pkt.Header.Type == protocol.PacketACKClose && pkt.Header.SessionID == sessionID {
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

			if pkt.Header.Type == protocol.PacketACKClose && pkt.Header.SessionID == sessionID {
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

	dbgLog.Printf("[receiver] transfer complete: %s (%d bytes)", outputPath, reqPayload.FileSize)
	return nil
}

// decryptAndParse parses a raw packet, decrypting the payload if the
// Encrypted flag is set and aead is non-nil (DATA and PARITY only).
func decryptAndParse(raw []byte, aead cipher.AEAD, sessionID uint32) (protocol.Packet, error) {
	if len(raw) < protocol.HeaderSize {
		return protocol.Packet{}, protocol.ErrPacketTooSmall
	}
	hdr, err := protocol.UnmarshalHeader(raw)
	if err != nil {
		return protocol.Packet{}, err
	}

	isDataOrParity := hdr.Type == protocol.PacketData || hdr.Type == protocol.PacketParity
	isEncrypted := hdr.Flags&protocol.FlagEncrypted != 0

	if isEncrypted && aead != nil && isDataOrParity {
		needed := protocol.HeaderSize + int(hdr.PayloadLen) + protocol.GCMTagSize
		if len(raw) < needed {
			return protocol.Packet{}, fmt.Errorf("encrypted packet too short: %d < %d", len(raw), needed)
		}
		nonce := protocol.BuildNonce(sessionID, hdr.Type, hdr.SequenceNum, hdr.BlockGroup)
		pt, err := protocol.DecryptPacket(aead, raw[:needed], nonce)
		if err != nil {
			return protocol.Packet{}, err
		}
		return protocol.Packet{Header: hdr, Payload: pt}, nil
	}

	return protocol.UnmarshalPacket(raw)
}
