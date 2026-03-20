package sender

import (
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hpuft/protocol"
	"hpuft/receiver" // for HashFile utility
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds all sender configuration.
type Config struct {
	RemoteAddr  string // e.g., "127.0.0.1:9000"
	FilePath    string // path to the file to send
	Session     protocol.SessionConfig
	Calibration protocol.CalibrationConfig
	Congestion  protocol.CongestionConfig

	// InitialRate in bytes/sec. 0 = use calibration mode (default).
	InitialRate uint32

	// SendDelay is a manual inter-packet delay for Phase 1 testing.
	// Overrides congestion control if non-zero.
	SendDelay time.Duration

	// NoDelay sends as fast as possible with no inter-packet pacing.
	// Overrides everything — no congestion control.
	NoDelay bool

	// NoCongestionControl disables adaptive rate control.
	// Packets are sent at a fixed rate without adjustment.
	NoCongestionControl bool

	// SessionID, if non-zero, overrides the randomly generated session ID.
	// Used by the serve command to reuse the ID from the client's PULL_REQ
	// so both sides agree on the session without a separate handshake.
	SessionID uint32

	// Debug enables verbose protocol and CC logging to stderr.
	// When false all noisy internal logs are suppressed; a progress bar is
	// printed to stdout by the caller instead.
	Debug bool

	// Quiet suppresses all sender-internal output (used by serve daemon).
	// The serve command handles its own structured event logging.
	Quiet bool

	// MuxConn, if non-nil, is used for all writes instead of dialling a new
	// socket. The serve daemon passes its control socket here so PULL traffic
	// flows through the single control port. Push clients pass their handshake
	// socket so the source address matches what the server expects.
	// Must be set together with MuxAddr.
	MuxConn *net.UDPConn

	// MuxAddr is the destination for WriteToUDP when MuxConn is set.
	MuxAddr *net.UDPAddr

	// RecvChan, if non-nil, supplies incoming packets instead of reading from
	// the socket. The serve daemon's control loop forwards packets here so the
	// sender goroutine never needs its own socket read path.
	RecvChan <-chan []byte

	// Encrypt enables AES-128-GCM per-packet encryption (spec §4.5).
	// When true and EncKey is nil and PeerPubKey is nil, the sender performs a
	// 1-RTT SESSION_REQ/SESSION_ACCEPT key exchange (direct send/recv mode).
	Encrypt bool

	// EncKey, if non-nil, is a pre-derived 16-byte AES-128 session key. The
	// serve daemon sets this for push transfers after PUSH_REQ/PUSH_ACCEPT exchange.
	// Takes priority over PeerPubKey when both are set.
	EncKey *[16]byte

	// PeerPubKey is the peer's X25519 public key (32 bytes). The serve daemon
	// sets this for pull transfers after receiving PULL_REQ with the client's key.
	// When Encrypt=true, EncKey=nil, and PeerPubKey is non-nil, the sender
	// generates its own ephemeral key, includes it in SESSION_REQ, derives the
	// session key, and starts sending — no SESSION_ACCEPT required.
	PeerPubKey []byte
}

// DefaultConfig returns sender config with spec defaults.
func DefaultConfig() Config {
	return Config{
		RemoteAddr:  "127.0.0.1:9000",
		Session:     protocol.DefaultSessionConfig(),
		Calibration: protocol.DefaultCalibrationConfig(),
		Congestion:  protocol.DefaultCongestionConfig(),
		InitialRate: 0,
		SendDelay:   0,
	}
}

// SenderProgress is a snapshot of live transfer metrics for the progress bar.
type SenderProgress struct {
	BytesSent     int64
	TotalBytes    int64
	NACKsSent     int64   // cumulative retransmitted packets
	RateBPS       float64 // current token-bucket target rate
	StartNs       int64   // Unix ns when Send() started
	RepairStartNs int64   // Unix ns when tail repair began (0 until repair starts)
	InRepair      bool    // true once all packets are sent and teardown is recovering drops

	// CC diagnostics — zero-valued if congestion control is disabled.
	RTT      time.Duration // most recent round-trip time estimate
	CCPhase  int           // 1 = Multiplicative Probe, 2 = Additive Avoidance
	LossRate float64       // most recent loss rate as a percentage (e.g. 0.10 = 0.10%)
}

// Sender manages a file transfer session.
type Sender struct {
	cfg Config

	// Progress atomics — written by Send(), read lock-free by progress bar.
	bytesSent  atomic.Int64
	totalBytes atomic.Int64
	nacksSent     atomic.Int64
	startNs       atomic.Int64
	repairStartNs atomic.Int64 // Unix ns when tail repair began (0 until repair starts)
	inRepair      atomic.Int32 // 1 once all packets sent and teardown is recovering drops

	bucket *TokenBucket // non-nil once Send() creates it
}

// Progress returns a live snapshot for the progress bar goroutine.
func (s *Sender) Progress() SenderProgress {
	var rate float64
	var rtt time.Duration
	var phase int
	var lossRate float64
	if s.bucket != nil {
		rate = s.bucket.Rate()
		rtt = s.bucket.RTTEstimate()
		phase = s.bucket.Phase()
		lossRate = s.bucket.LossRatePercent()
	}
	return SenderProgress{
		BytesSent:     s.bytesSent.Load(),
		TotalBytes:    s.totalBytes.Load(),
		NACKsSent:     s.nacksSent.Load(),
		RateBPS:       rate,
		StartNs:       s.startNs.Load(),
		RepairStartNs: s.repairStartNs.Load(),
		InRepair:      s.inRepair.Load() == 1,
		RTT:           rtt,
		CCPhase:       phase,
		LossRate:      lossRate,
	}
}

// New creates a new Sender.
func New(cfg Config) *Sender {
	return &Sender{cfg: cfg}
}

// Send performs a complete file transfer.
func (s *Sender) Send() error {
	// --- Logger setup ---
	var dbgLog *log.Logger
	if s.cfg.Debug && !s.cfg.Quiet {
		dbgLog = log.New(os.Stderr, "", log.Ltime|log.Lmicroseconds)
	} else {
		dbgLog = log.New(io.Discard, "", 0)
	}

	// --- Step 1: Read file info and compute hash ---
	fileInfo, err := os.Stat(s.cfg.FilePath)
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}
	fileSize := uint64(fileInfo.Size())

	dbgLog.Printf("[sender] hashing file: %s (%d bytes)", s.cfg.FilePath, fileSize)
	checksum, err := receiver.HashFile(s.cfg.FilePath)
	if err != nil {
		return fmt.Errorf("hash file: %w", err)
	}
	dbgLog.Printf("[sender] file hash: 0x%016X", checksum)

	// --- Step 2: Socket setup ---
	// MuxConn path: use the caller's shared socket (serve control port or
	// push client's handshake socket). No dial, no close, no write buffer.
	// Normal path: dial a fresh connected socket to the remote address.
	var conn *net.UDPConn
	recvChan := s.cfg.RecvChan

	if s.cfg.MuxConn != nil {
		conn = s.cfg.MuxConn
	} else {
		remoteAddr, err := net.ResolveUDPAddr("udp", s.cfg.RemoteAddr)
		if err != nil {
			return fmt.Errorf("resolve remote addr: %w", err)
		}
		conn, err = net.DialUDP("udp", nil, remoteAddr)
		if err != nil {
			return fmt.Errorf("dial udp: %w", err)
		}
		defer conn.Close()
		conn.SetWriteBuffer(16 * 1024 * 1024)
	}

	// writeFn sends raw bytes to the correct destination.
	// In mux mode: unconnected socket → WriteToUDP with the stored remote addr.
	// In normal mode: connected socket → Write.
	writeFn := func(raw []byte) {
		if s.cfg.MuxAddr != nil {
			conn.WriteToUDP(raw, s.cfg.MuxAddr)
		} else {
			conn.Write(raw)
		}
	}

	// --- Step 3: Generate SessionID and send SESSION_REQ ---
	sessionID := s.cfg.SessionID
	if sessionID == 0 {
		sessionID = generateSessionID()
	}

	// --- Encryption setup ---
	// Three cases:
	//   1. Not encrypted: aead = nil, proceed as before.
	//   2. Encrypted + EncKey set (push flow): use pre-derived key, no key exchange.
	//   3. Encrypted + PeerPubKey set (serve pull flow): generate ephemeral key,
	//      embed in SESSION_REQ, derive key immediately, no SESSION_ACCEPT needed.
	//   4. Encrypted + neither set (direct send/recv): generate ephemeral key,
	//      embed in SESSION_REQ, wait for SESSION_ACCEPT.
	var aead cipher.AEAD
	if s.cfg.Encrypt {
		if s.cfg.EncKey != nil {
			// Case 2: pre-derived key (push flow).
			c, err := protocol.NewSessionCipher(*s.cfg.EncKey)
			if err != nil {
				return fmt.Errorf("init session cipher: %w", err)
			}
			aead = c
		}
		// Cases 3 and 4 are handled after SESSION_REQ is sent.
	}

	reqPayload := protocol.SessionReqPayload{
		FileSize:    fileSize,
		Checksum:    checksum,
		InitialRate: s.cfg.InitialRate,
		FileName:    filepath.Base(s.cfg.FilePath),
	}
	reqHdrFlags := protocol.Flag(0)

	var ephemPriv *ecdh.PrivateKey
	if s.cfg.Encrypt && s.cfg.EncKey == nil {
		// Cases 3 and 4: embed sender's public key in SESSION_REQ.
		ephemPriv, err = protocol.GenerateEphemeralKey()
		if err != nil {
			return fmt.Errorf("generate ephemeral key: %w", err)
		}
		reqPayload.Encrypted = true
		copy(reqPayload.PubKey[:], ephemPriv.PublicKey().Bytes())
		reqHdrFlags |= protocol.FlagEncrypted
	}

	reqPkt := protocol.Packet{
		Header: protocol.Header{
			Type:      protocol.PacketSessionReq,
			SessionID: sessionID,
			Flags:     reqHdrFlags,
		},
		Payload: protocol.MarshalSessionReq(&reqPayload),
	}

	reqRaw, err := protocol.MarshalPacket(&reqPkt)
	if err != nil {
		return fmt.Errorf("marshal SESSION_REQ: %w", err)
	}

	dbgLog.Printf("[sender] sending SESSION_REQ: sessionID=0x%08X -> %s encrypted=%v",
		sessionID, s.cfg.RemoteAddr, s.cfg.Encrypt)
	writeFn(reqRaw)

	// Case 3: serve pull — derive key immediately from PeerPubKey.
	if s.cfg.Encrypt && s.cfg.EncKey == nil && len(s.cfg.PeerPubKey) > 0 {
		key, err := protocol.DeriveSessionKey(ephemPriv, s.cfg.PeerPubKey, sessionID)
		if err != nil {
			return fmt.Errorf("derive session key (pull): %w", err)
		}
		c, err := protocol.NewSessionCipher(key)
		if err != nil {
			return fmt.Errorf("init cipher (pull): %w", err)
		}
		aead = c
		dbgLog.Printf("[sender] session key derived (pull flow, no SESSION_ACCEPT)")
	}

	// Case 4: direct send/recv — wait for SESSION_ACCEPT.
	if s.cfg.Encrypt && s.cfg.EncKey == nil && len(s.cfg.PeerPubKey) == 0 {
		dbgLog.Printf("[sender] waiting for SESSION_ACCEPT...")
		buf := make([]byte, protocol.MTUHardCap)
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		acceptLoop:
		for {
			n, err := conn.Read(buf)
			if err != nil {
				conn.SetReadDeadline(time.Time{})
				return fmt.Errorf("SESSION_ACCEPT read: %w", err)
			}
			if n < protocol.HeaderSize {
				continue
			}
			hdr, err := protocol.UnmarshalHeader(buf[:n])
			if err != nil || hdr.Type != protocol.PacketSessionAccept || hdr.SessionID != sessionID {
				continue
			}
			payloadEnd := protocol.HeaderSize + int(hdr.PayloadLen)
			if payloadEnd > n {
				continue
			}
			accept, err := protocol.UnmarshalSessionAccept(buf[protocol.HeaderSize:payloadEnd])
			if err != nil {
				continue
			}
			conn.SetReadDeadline(time.Time{})
			key, err := protocol.DeriveSessionKey(ephemPriv, accept.PubKey[:], sessionID)
			if err != nil {
				return fmt.Errorf("derive session key: %w", err)
			}
			c, err := protocol.NewSessionCipher(key)
			if err != nil {
				return fmt.Errorf("init cipher: %w", err)
			}
			aead = c
			dbgLog.Printf("[sender] session key derived (direct, SESSION_ACCEPT received)")
			break acceptLoop
		}
	}

	// --- Step 3b: Resume negotiation ---
	// After SESSION_REQ is sent and encryption is set up, wait briefly for
	// a RESUME_REQ from the receiver. If the receiver has a checkpoint from
	// a previous interrupted transfer, it will send RESUME_REQ instead of
	// proceeding with a fresh transfer.
	chunkSize := protocol.MaxPayload
	if aead != nil {
		chunkSize = protocol.MaxEncryptedPayload
	}
	var resumeSeqNum uint64 // 0 = fresh transfer

	// The negotiation window: listen for RESUME_REQ for up to 1 second.
	// The receiver checks for a checkpoint immediately after SESSION_REQ and
	// sends RESUME_REQ within milliseconds if one exists. A heartbeat arrival
	// means the receiver started a fresh transfer (heartbeat generator launches
	// only after resume check completes).
	{
		negoBuf := make([]byte, protocol.MTUHardCap)
		negoDeadline := time.Now().Add(1 * time.Second)

		negoLoop:
		for time.Now().Before(negoDeadline) {
			var n int
			var readErr error

			if recvChan != nil {
				select {
				case raw, ok := <-recvChan:
					if !ok {
						break negoLoop
					}
					n = copy(negoBuf, raw)
				case <-time.After(100 * time.Millisecond):
					continue
				}
			} else {
				conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
				n, readErr = conn.Read(negoBuf)
				if readErr != nil {
					if os.IsTimeout(readErr) {
						continue
					}
					break negoLoop
				}
			}

			if n < protocol.HeaderSize {
				continue
			}
			hdr, hdrErr := protocol.UnmarshalHeader(negoBuf[:n])
			if hdrErr != nil || hdr.SessionID != sessionID {
				continue
			}

			switch hdr.Type {
			case protocol.PacketResumeReq:
				payloadEnd := protocol.HeaderSize + int(hdr.PayloadLen)
				if payloadEnd > n {
					continue
				}
				rr, rrErr := protocol.UnmarshalResumeReq(negoBuf[protocol.HeaderSize:payloadEnd], false)
				if rrErr != nil {
					continue
				}

				// Validate: file size and full hash must match.
				if rr.FileSize != fileSize || rr.FullHash != checksum {
					dbgLog.Printf("[sender] RESUME_REQ rejected: file mismatch")
					rejectPkt := protocol.Packet{
						Header: protocol.Header{
							Type:      protocol.PacketSessionReject,
							SessionID: sessionID,
						},
						Payload: []byte{byte(protocol.RejectResumeHashMismatch)},
					}
					raw, _ := protocol.MarshalPacket(&rejectPkt)
					writeFn(raw)
					break negoLoop
				}

				// Validate partial hash: hash bytes 0..ResumeOffset from source.
				partialHash, phErr := receiver.HashFileRange(s.cfg.FilePath, int64(rr.ResumeOffset))
				if phErr != nil || partialHash != rr.PartialHash {
					dbgLog.Printf("[sender] RESUME_REQ rejected: partial hash mismatch (computed=0x%016X, received=0x%016X, err=%v)",
						partialHash, rr.PartialHash, phErr)
					rejectPkt := protocol.Packet{
						Header: protocol.Header{
							Type:      protocol.PacketSessionReject,
							SessionID: sessionID,
						},
						Payload: []byte{byte(protocol.RejectResumeHashMismatch)},
					}
					raw, _ := protocol.MarshalPacket(&rejectPkt)
					writeFn(raw)
					break negoLoop
				}

				// Accept the resume.
				resumeSeqNum = rr.ResumeOffset / uint64(chunkSize)
				acceptPayload := protocol.ResumeAcceptPayload{
					ResumeSequenceNum: resumeSeqNum,
				}
				acceptPkt := protocol.Packet{
					Header: protocol.Header{
						Type:      protocol.PacketResumeAccept,
						SessionID: sessionID,
					},
					Payload: protocol.MarshalResumeAccept(&acceptPayload),
				}
				raw, _ := protocol.MarshalPacket(&acceptPkt)
				writeFn(raw)

				dbgLog.Printf("[sender] RESUME_ACCEPT: starting from seq=%d (offset=%d)",
					resumeSeqNum, rr.ResumeOffset)
				break negoLoop

			case protocol.PacketHeartbeat:
				// Receiver started a fresh transfer — no resume.
				break negoLoop

			case protocol.PacketTransferComplete:
				// Edge case: receiver already has the complete file.
				break negoLoop

			default:
				continue
			}
		}

		if recvChan == nil {
			conn.SetReadDeadline(time.Time{})
		}
	}

	// --- Step 4: Open file and prepare send state ---
	file, err := os.Open(s.cfg.FilePath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	// Seek past already-transferred data if resuming.
	if resumeSeqNum > 0 {
		seekOffset := int64(resumeSeqNum) * int64(chunkSize)
		if _, err := file.Seek(seekOffset, io.SeekStart); err != nil {
			return fmt.Errorf("seek to resume offset: %w", err)
		}
		dbgLog.Printf("[sender] file seeked to offset %d for resume", seekOffset)
	}

	totalChunks := fileSize / uint64(chunkSize)
	if fileSize%uint64(chunkSize) != 0 {
		totalChunks++
	}

	// --- Step 5: Determine pacing mode ---
	useCongestionControl := !s.cfg.NoDelay && !s.cfg.NoCongestionControl && s.cfg.SendDelay == 0

	var bucket *TokenBucket
	var calibration *CalibrationState

	if useCongestionControl {
		startRate := StartingRate(s.cfg.Calibration, s.cfg.InitialRate, chunkSize)
		bucket = NewTokenBucket(startRate, s.cfg.Congestion)
		bucket.SetLogger(dbgLog)
		s.bucket = bucket
		calibration = NewCalibrationState(s.cfg.Calibration, s.cfg.InitialRate)
		dbgLog.Printf("[sender] congestion control ENABLED, starting rate=%.2f MB/s", startRate/1e6)
	} else if s.cfg.NoDelay {
		dbgLog.Printf("[sender] congestion control DISABLED (nodelay mode)")
	} else if s.cfg.SendDelay > 0 {
		dbgLog.Printf("[sender] congestion control DISABLED (fixed delay=%v)", s.cfg.SendDelay)
	}

	// --- Step 5b: Create FEC block encoder ---
	fecCfg := protocol.DefaultFECConfig()
	blockEncoder := NewBlockEncoder(fecCfg)
	dbgLog.Printf("[sender] FEC enabled: block_size=%d, initial_parity=%.0f%%",
		fecCfg.BlockSize, fecCfg.InitialParityPct*100)

	// --- Set progress totals ---
	s.totalBytes.Store(int64(fileSize))
	s.startNs.Store(time.Now().UnixNano())
	if resumeSeqNum > 0 {
		s.bytesSent.Store(int64(resumeSeqNum) * int64(chunkSize))
	}

	// --- Step 6: Start heartbeat listener goroutine ---
	var nackMu sync.Mutex
	nackPending := make(map[uint64]struct{})

	sw := NewSlidingWindow(windowSize)

	doneCh := make(chan struct{})

	type teardownMsg struct {
		pktType protocol.PacketType
		payload []byte
	}
	teardownCh := make(chan teardownMsg, 1)

	var hbWg sync.WaitGroup
	hbWg.Add(1)
	go func() {
		defer hbWg.Done()
		hbBuf := make([]byte, protocol.MTUHardCap)
		for {
			var n int

			if recvChan != nil {
				// Mux mode: receive packets from the control loop channel.
				select {
				case <-doneCh:
					return
				case raw, ok := <-recvChan:
					if !ok {
						return
					}
					n = copy(hbBuf, raw)
				case <-time.After(200 * time.Millisecond):
					continue
				}
			} else {
				// Normal mode: read directly from the socket.
				select {
				case <-doneCh:
					return
				default:
				}
				conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
				var err error
				n, err = conn.Read(hbBuf)
				if err != nil {
					if os.IsTimeout(err) {
						continue
					}
					return
				}
			}

			pkt, err := protocol.UnmarshalPacket(hbBuf[:n])
			if err != nil {
				continue
			}
			if pkt.Header.SessionID != sessionID {
				continue
			}

			switch pkt.Header.Type {
			case protocol.PacketHeartbeat:
				hb, err := protocol.UnmarshalHeartbeat(pkt.Payload)
				if err != nil {
					log.Printf("[sender] malformed heartbeat: %v", err)
					continue
				}

				if bucket != nil {
					bucket.OnHeartbeat(&hb)
				}

				if calibration != nil {
					calibration.OnHeartbeat()
				}

				blockEncoder.UpdateLossRate(hb.LossRate)

				if len(hb.NACKs) > 0 {
					nackMu.Lock()
					newCount := 0
					for _, seq := range hb.NACKs {
						if _, exists := nackPending[seq]; !exists {
							nackPending[seq] = struct{}{}
							newCount++
						}
					}
					total := len(nackPending)
					nackMu.Unlock()
					if newCount > 0 {
						dbgLog.Printf("[sender] queued %d new NACKs (%d total pending)", newCount, total)
					}
				}

				sw.Advance(hb.HighestContiguous)

			case protocol.PacketTransferComplete, protocol.PacketSessionReject:
				select {
				case teardownCh <- teardownMsg{pkt.Header.Type, pkt.Payload}:
				default:
				}
				return
			}
		}
	}()

	// --- Step 8: Main send loop ---
	sendBuf := make([]byte, protocol.MTUHardCap)
	readBuf := make([]byte, chunkSize)
	seqNum := resumeSeqNum
	startTime := time.Now()

	// Diagnostic: track time spent stalled on full sliding window.
	var windowStallNs atomic.Int64
	var windowStallCount atomic.Int64
	var pktsSentSinceLog atomic.Int64
	diagTicker := time.NewTicker(time.Second)
	defer diagTicker.Stop()
	go func() {
		for range diagTicker.C {
			stallNs := windowStallNs.Swap(0)
			stallCnt := windowStallCount.Swap(0)
			pkts := pktsSentSinceLog.Swap(0)
			dbgLog.Printf("[diag] window_stall: %dms in %d sleeps | new_pkts_sent: %d | seqNum=%d",
				stallNs/1e6, stallCnt, pkts, seqNum)
		}
	}()

	for seqNum < totalChunks {
		const maxNACKsPerIteration = 3
		nackMu.Lock()
		var nacksToSend []uint64
		for seq := range nackPending {
			nacksToSend = append(nacksToSend, seq)
			delete(nackPending, seq)
			if len(nacksToSend) >= maxNACKsPerIteration {
				break
			}
		}
		nackMu.Unlock()

		for _, nackSeq := range nacksToSend {
			chunk, ok := sw.Load(nackSeq)

			if !ok {
				continue
			}

			hdr := protocol.Header{
				Type:              protocol.PacketData,
				SessionID:         sessionID,
				SequenceNum:       nackSeq,
				BlockGroup:        nackSeq / uint64(protocol.DefaultFECConfig().BlockSize),
				PayloadLen:        uint16(len(chunk)),
				SenderTimestampNs: uint64(time.Now().UnixNano()),
			}
			if nackSeq == totalChunks-1 {
				hdr.Flags = protocol.FlagEndOfFile
			}

			hdrSize, _ := protocol.MarshalHeader(sendBuf, &hdr)
			copy(sendBuf[hdrSize:], chunk)
			totalPktSize := hdrSize + len(chunk)

			if aead != nil {
				hdr.Flags |= protocol.FlagEncrypted
				protocol.MarshalHeader(sendBuf, &hdr)
				nonce := protocol.BuildNonce(sessionID, protocol.PacketData, nackSeq, hdr.BlockGroup)
				writeFn(protocol.EncryptPacket(aead, sendBuf[:totalPktSize], nonce))
			} else {
				writeFn(sendBuf[:totalPktSize])
			}

			if bucket != nil {
				bucket.Pace(hdrSize + len(chunk))
			}
		}

		// --- Backpressure: if window is full, yield and loop back to NACK
		// processing rather than spinning here. This is critical: if a lost
		// packet stalls HighestContiguous at 0, the window fills and HC never
		// advances unless we keep retransmitting the NACKed sequences. A bare
		// sleep here would starve nackPending and cause receiver inactivity.
		if sw.IsFull(seqNum) {
			windowStallNs.Add(int64(time.Millisecond))
			windowStallCount.Add(1)
			time.Sleep(time.Millisecond)
			continue
		}

		// --- Send next data packet ---
		n, err := io.ReadFull(file, readBuf)
		if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			close(doneCh)
			return fmt.Errorf("read file at seq %d: %w", seqNum, err)
		}
		if n == 0 {
			break
		}

		hdr := protocol.Header{
			Type:              protocol.PacketData,
			SessionID:         sessionID,
			SequenceNum:       seqNum,
			BlockGroup:        seqNum / uint64(protocol.DefaultFECConfig().BlockSize),
			PayloadLen:        uint16(n),
			SenderTimestampNs: uint64(time.Now().UnixNano()),
		}

		if seqNum == totalChunks-1 {
			hdr.Flags = protocol.FlagEndOfFile
		}

		if calibration != nil {
			hdr.Flags |= calibration.Flags()
		}

		hdrSize, _ := protocol.MarshalHeader(sendBuf, &hdr)
		copy(sendBuf[hdrSize:], readBuf[:n])
		totalSize := hdrSize + n

		sw.Store(seqNum, readBuf[:n])

		if aead != nil {
			hdr.Flags |= protocol.FlagEncrypted
			protocol.MarshalHeader(sendBuf, &hdr)
			nonce := protocol.BuildNonce(sessionID, protocol.PacketData, seqNum, hdr.BlockGroup)
			writeFn(protocol.EncryptPacket(aead, sendBuf[:totalSize], nonce))
		} else {
			writeFn(sendBuf[:totalSize])
		}

		s.bytesSent.Store(int64(seqNum) * int64(chunkSize))

		if calibration != nil {
			calibration.PacketSent()
		}

		parityResult := blockEncoder.AddShard(hdr.BlockGroup, readBuf[:n])
		if parityResult != nil {
			sendParityPackets(writeFn, sessionID, parityResult, sendBuf, bucket, s.cfg.SendDelay, aead)
		}

		seqNum++
		pktsSentSinceLog.Add(1)

		if calibration != nil && calibration.Pace() {
			// calibration handled the pacing
		} else if bucket != nil {
			bucket.Pace(totalSize)
		} else if s.cfg.SendDelay > 0 {
			time.Sleep(s.cfg.SendDelay)
		}
	}
	s.bytesSent.Store(int64(fileSize))

	// --- FEC: flush tail block parity ---
	tailResult := blockEncoder.FlushTail()
	if tailResult != nil {
		sendParityPackets(writeFn, sessionID, tailResult, sendBuf, bucket, s.cfg.SendDelay, aead)
		dbgLog.Printf("[sender] sent %d tail block parity packets for block %d (%d data shards)",
			tailResult.ParityCount, tailResult.BlockGroup, tailResult.DataCount)
	}

	elapsed := time.Since(startTime)
	bytesPerSec := float64(fileSize) / elapsed.Seconds()

	if bucket != nil {
		stats := bucket.Stats()
		dbgLog.Printf("[sender] all %d packets sent in %v (%.2f MB/s) | CC: +%d =%d -%d",
			seqNum, elapsed.Round(time.Millisecond), bytesPerSec/1e6,
			stats.Increases, stats.Holds, stats.Decreases)
	} else {
		dbgLog.Printf("[sender] all %d packets sent in %v (%.2f MB/s)",
			seqNum, elapsed.Round(time.Millisecond), bytesPerSec/1e6)
	}

	dbgLog.Printf("[sender] waiting for TRANSFER_COMPLETE...")

	// --- Step 9: Wait for TRANSFER_COMPLETE ---
	select {
	case msg := <-teardownCh:
		close(doneCh)
		hbWg.Wait()
		return s.handleTeardown(conn, writeFn, recvChan, sessionID, msg.pktType, msg.payload, dbgLog)
	default:
	}

	close(doneCh)
	// Wait for the heartbeat goroutine to exit before we take exclusive
	// ownership of the socket (or recvChan). Without this wait, both this
	// goroutine and the heartbeat goroutine race on conn.Read — heartbeats
	// with NACKs get stolen and queued into nackPending (which is never
	// drained again), and TRANSFER_COMPLETE can be stolen into teardownCh
	// while the teardown loop below is blocked in conn.Read.
	hbWg.Wait()

	// The goroutine may have forwarded TRANSFER_COMPLETE just before exiting.
	select {
	case msg := <-teardownCh:
		return s.handleTeardown(conn, writeFn, recvChan, sessionID, msg.pktType, msg.payload, dbgLog)
	default:
	}

	nackMu.Lock()
	pendingNACKs := make([]uint64, 0, len(nackPending))
	for seq := range nackPending {
		pendingNACKs = append(pendingNACKs, seq)
	}
	nackPending = make(map[uint64]struct{})
	nackMu.Unlock()

	retransmitNACKs(writeFn, sessionID, pendingNACKs, sendBuf, sw, totalChunks, bucket, aead)

	// nackCooldown tracks the last retransmit time per sequence number.
	// On a 50ms-RTT path the receiver fires one heartbeat per HB interval
	// (25–100ms) and will keep NACKing the same sequence until the
	// retransmission lands. Without a cooldown, every in-flight heartbeat
	// triggers a redundant retransmit, flooding the link and causing a
	// self-reinforcing loss spiral (observed: 59,908 NACKs for ~780 losses).
	// We gate each sequence to at most one retransmit per RTT + 25% margin.
	nackCooldown := make(map[uint64]time.Time)
	nackCooldownRTT := func() time.Duration {
		if bucket != nil {
			if rtt := bucket.RTTEstimate(); rtt > 0 {
				return rtt * 5 / 4 // RTT + 25% margin
			}
		}
		return 200 * time.Millisecond // conservative default before RTT is known
	}

	// Seed the cooldown map so the initial pending-NACK drain is counted.
	now := time.Now()
	for _, seq := range pendingNACKs {
		nackCooldown[seq] = now
	}

	s.repairStartNs.Store(time.Now().UnixNano())
	s.inRepair.Store(1)

	// Teardown read loop — own the socket (or drain the channel).
	probeDeadline := time.NewTimer(s.cfg.Session.SenderProbeTimeout)
	defer probeDeadline.Stop()
	rawBuf := make([]byte, protocol.MTUHardCap)

	for {
		var pkt protocol.Packet
		var parseErr error

		if recvChan != nil {
			select {
			case raw, ok := <-recvChan:
				if !ok {
					return fmt.Errorf("channel closed waiting for TRANSFER_COMPLETE")
				}
				pkt, parseErr = protocol.UnmarshalPacket(raw)
			case <-probeDeadline.C:
				return fmt.Errorf("timeout waiting for TRANSFER_COMPLETE")
			}
		} else {
			conn.SetReadDeadline(time.Now().Add(s.cfg.Session.SenderProbeTimeout))
			n, err := conn.Read(rawBuf)
			if err != nil {
				if os.IsTimeout(err) {
					return fmt.Errorf("timeout waiting for TRANSFER_COMPLETE")
				}
				return fmt.Errorf("read: %w", err)
			}
			pkt, parseErr = protocol.UnmarshalPacket(rawBuf[:n])
		}

		if parseErr != nil {
			continue
		}
		if pkt.Header.SessionID != sessionID {
			continue
		}

		switch pkt.Header.Type {
		case protocol.PacketTransferComplete, protocol.PacketSessionReject:
			return s.handleTeardown(conn, writeFn, recvChan, sessionID, pkt.Header.Type, pkt.Payload, dbgLog)

		case protocol.PacketHeartbeat:
			hb, err := protocol.UnmarshalHeartbeat(pkt.Payload)
			if err != nil {
				continue
			}
			if bucket != nil {
				bucket.OnHeartbeat(&hb)
			}
			nacksToProcess := hb.NACKs

			// Tail-drop deadlock prevention.
			//
			// The receiver's NACK scan window is bounded by HighestReceived —
			// it never NACKs sequences it has never seen. If the very last
			// packets drop, HighestReceived never advances past the drop point,
			// so the receiver sends heartbeats with 0 NACKs. The sender sees
			// 0 NACKs, sends nothing, the receiver hits its inactivity timeout.
			//
			// Solution: when NACKs are empty but HighestContiguous is short of
			// the last sequence, the tail has dropped and we inject those
			// sequences so retransmitNACKs can push them.
			if len(nacksToProcess) == 0 && hb.HighestContiguous < totalChunks-1 {
				startSeq := hb.HighestContiguous + 1
				for seq := startSeq; seq < totalChunks && uint64(len(nacksToProcess)) < 167; seq++ {
					nacksToProcess = append(nacksToProcess, seq)
				}
				dbgLog.Printf("[sender] teardown tail-drop: injecting %d missing packets starting at seq %d",
					len(nacksToProcess), startSeq)
			}

			if len(nacksToProcess) > 0 {
				rtt := nackCooldownRTT()
				now := time.Now()
				var toRetransmit []uint64
				for _, seq := range nacksToProcess {
					if last, ok := nackCooldown[seq]; !ok || now.Sub(last) >= rtt {
						nackCooldown[seq] = now
						toRetransmit = append(toRetransmit, seq)
					}
				}
				if len(toRetransmit) > 0 {
					dbgLog.Printf("[sender] teardown: retransmitting %d/%d NACKed packets (%d on cooldown)",
						len(toRetransmit), len(nacksToProcess), len(nacksToProcess)-len(toRetransmit))
					s.nacksSent.Add(int64(len(toRetransmit)))
					// Micro-burst prevention: send in batches of 10 with a 2ms
					// sleep between batches. At high transfer speeds the token
					// bucket's burst allowance is large enough to fire all 166
					// retransmits simultaneously, flooding the OS socket buffer
					// and the serve daemon's 256-slot receive channel.
					const teardownBatch = 10
					for i := 0; i < len(toRetransmit); i += teardownBatch {
						end := i + teardownBatch
						if end > len(toRetransmit) {
							end = len(toRetransmit)
						}
						retransmitNACKs(writeFn, sessionID, toRetransmit[i:end], sendBuf, sw, totalChunks, bucket, aead)
						time.Sleep(2 * time.Millisecond)
					}
				}
			}
			// Reset deadline — still making progress
			if recvChan != nil {
				probeDeadline.Reset(s.cfg.Session.SenderProbeTimeout)
			} else {
				conn.SetReadDeadline(time.Now().Add(s.cfg.Session.SenderProbeTimeout))
			}
		}
	}
}

// handleTeardown processes TRANSFER_COMPLETE or SESSION_REJECT and manages linger.
func (s *Sender) handleTeardown(
	conn *net.UDPConn,
	writeFn func([]byte),
	recvChan <-chan []byte,
	sessionID uint32,
	pktType protocol.PacketType,
	payload []byte,
	dbgLog *log.Logger,
) error {
	switch pktType {
	case protocol.PacketTransferComplete:
		dbgLog.Printf("[sender] received TRANSFER_COMPLETE")

		ackPkt := protocol.Packet{
			Header: protocol.Header{
				Type:      protocol.PacketACKClose,
				SessionID: sessionID,
			},
		}
		ackRaw, _ := protocol.MarshalPacket(&ackPkt)
		writeFn(ackRaw)

		dbgLog.Printf("[sender] sent ACK_CLOSE, entering linger state")

		lingerTimer := time.NewTimer(s.cfg.Session.LingerDuration)
		defer lingerTimer.Stop()
		rawBuf := make([]byte, protocol.MTUHardCap)

		for {
			var pkt protocol.Packet
			var err error

			if recvChan != nil {
				select {
				case raw, ok := <-recvChan:
					if !ok {
						goto lingerDone
					}
					pkt, err = protocol.UnmarshalPacket(raw)
				case <-lingerTimer.C:
					goto lingerDone
				}
			} else {
				lingerEnd := time.Now().Add(s.cfg.Session.LingerDuration)
				for time.Now().Before(lingerEnd) {
					conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
					n, err := conn.Read(rawBuf)
					if err != nil {
						if os.IsTimeout(err) {
							continue
						}
						break
					}
					p, err := protocol.UnmarshalPacket(rawBuf[:n])
					if err != nil || p.Header.Type != protocol.PacketTransferComplete || p.Header.SessionID != sessionID {
						continue
					}
					writeFn(ackRaw)
				}
				conn.SetReadDeadline(time.Time{})
				goto lingerDone
			}

			if err != nil {
				continue
			}
			if pkt.Header.Type == protocol.PacketTransferComplete &&
				pkt.Header.SessionID == sessionID {
				writeFn(ackRaw)
			}
		}

	lingerDone:
		log.Printf("[sender] linger complete, session finished")
		return nil

	case protocol.PacketSessionReject:
		if len(payload) > 0 {
			return fmt.Errorf("transfer rejected by receiver: %s", protocol.RejectReason(payload[0]))
		}
		return fmt.Errorf("transfer rejected by receiver")

	default:
		return fmt.Errorf("unexpected teardown packet type: %s", pktType)
	}
}

// retransmitNACKs sends cached data packets for the given sequence numbers.
func retransmitNACKs(
	writeFn func([]byte),
	sessionID uint32,
	nacks []uint64,
	sendBuf []byte,
	sw *SlidingWindow,
	totalChunks uint64,
	bucket *TokenBucket,
	aead cipher.AEAD,
) {
	for _, nackSeq := range nacks {
		chunk, ok := sw.Load(nackSeq)
		if !ok {
			continue
		}

		hdr := protocol.Header{
			Type:              protocol.PacketData,
			SessionID:         sessionID,
			SequenceNum:       nackSeq,
			BlockGroup:        nackSeq / uint64(protocol.DefaultFECConfig().BlockSize),
			PayloadLen:        uint16(len(chunk)),
			SenderTimestampNs: uint64(time.Now().UnixNano()),
		}
		if nackSeq == totalChunks-1 {
			hdr.Flags = protocol.FlagEndOfFile
		}

		pktLen := protocol.HeaderSize + len(chunk)
		if bucket != nil {
			bucket.Pace(pktLen)
		}
		hs, _ := protocol.MarshalHeader(sendBuf, &hdr)
		copy(sendBuf[hs:], chunk)
		totalSize := hs + len(chunk)

		if aead != nil {
			hdr.Flags |= protocol.FlagEncrypted
			protocol.MarshalHeader(sendBuf, &hdr)
			nonce := protocol.BuildNonce(sessionID, protocol.PacketData, nackSeq, hdr.BlockGroup)
			writeFn(protocol.EncryptPacket(aead, sendBuf[:totalSize], nonce))
		} else {
			writeFn(sendBuf[:totalSize])
		}
	}
}

// sendParityPackets transmits parity packets for a completed FEC block.
func sendParityPackets(
	writeFn func([]byte),
	sessionID uint32,
	result *ParityResult,
	sendBuf []byte,
	bucket *TokenBucket,
	fixedDelay time.Duration,
	aead cipher.AEAD,
) {
	for i, payload := range result.Payloads {
		hdr := protocol.Header{
			Type:        protocol.PacketParity,
			SessionID:   sessionID,
			SequenceNum: uint64(i),
			BlockGroup:  result.BlockGroup,
			PayloadLen:  uint16(len(payload)),
		}

		hdrSize, _ := protocol.MarshalHeader(sendBuf, &hdr)
		copy(sendBuf[hdrSize:], payload)
		totalSize := hdrSize + len(payload)

		if aead != nil {
			hdr.Flags |= protocol.FlagEncrypted
			protocol.MarshalHeader(sendBuf, &hdr)
			nonce := protocol.BuildNonce(sessionID, protocol.PacketParity, uint64(i), result.BlockGroup)
			writeFn(protocol.EncryptPacket(aead, sendBuf[:totalSize], nonce))
		} else {
			writeFn(sendBuf[:totalSize])
		}

		if bucket != nil {
			bucket.Pace(totalSize)
		} else if fixedDelay > 0 {
			time.Sleep(fixedDelay)
		}
	}
}

// generateSessionID produces a cryptographically random 32-bit session ID.
func generateSessionID() uint32 {
	var b [4]byte
	rand.Read(b[:])
	return binary.BigEndian.Uint32(b[:])
}
