package sender

import (
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
	BytesSent  int64
	TotalBytes int64
	NACKsSent  int64   // cumulative retransmitted packets
	RateBPS    float64 // current token-bucket target rate
	StartNs    int64   // Unix ns when Send() started
}

// Sender manages a file transfer session.
type Sender struct {
	cfg Config

	// Progress atomics — written by Send(), read lock-free by progress bar.
	bytesSent  atomic.Int64
	totalBytes atomic.Int64
	nacksSent  atomic.Int64
	startNs    atomic.Int64

	bucket *TokenBucket // non-nil once Send() creates it
}

// Progress returns a live snapshot for the progress bar goroutine.
func (s *Sender) Progress() SenderProgress {
	var rate float64
	if s.bucket != nil {
		rate = s.bucket.Rate()
	}
	return SenderProgress{
		BytesSent:  s.bytesSent.Load(),
		TotalBytes: s.totalBytes.Load(),
		NACKsSent:  s.nacksSent.Load(),
		RateBPS:    rate,
		StartNs:    s.startNs.Load(),
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

	reqPayload := protocol.SessionReqPayload{
		FileSize:    fileSize,
		Checksum:    checksum,
		InitialRate: s.cfg.InitialRate,
		FileName:    filepath.Base(s.cfg.FilePath),
	}

	reqPkt := protocol.Packet{
		Header: protocol.Header{
			Type:      protocol.PacketSessionReq,
			SessionID: sessionID,
		},
		Payload: protocol.MarshalSessionReq(&reqPayload),
	}

	reqRaw, err := protocol.MarshalPacket(&reqPkt)
	if err != nil {
		return fmt.Errorf("marshal SESSION_REQ: %w", err)
	}

	dbgLog.Printf("[sender] sending SESSION_REQ: sessionID=0x%08X -> %s", sessionID, s.cfg.RemoteAddr)
	writeFn(reqRaw)

	// --- Step 4: Open file and prepare send state ---
	file, err := os.Open(s.cfg.FilePath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	chunkSize := protocol.MaxPayload
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

	// --- Step 6: Start heartbeat listener goroutine ---
	var nackMu sync.Mutex
	nackPending := make(map[uint64]struct{})

	sentChunks := make(map[uint64][]byte)
	var sentMu sync.Mutex

	doneCh := make(chan struct{})

	type teardownMsg struct {
		pktType protocol.PacketType
		payload []byte
	}
	teardownCh := make(chan teardownMsg, 1)

	go func() {
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

				if hb.HighestContiguous > 0 {
					sentMu.Lock()
					for seq := range sentChunks {
						if seq <= hb.HighestContiguous {
							delete(sentChunks, seq)
						}
					}
					sentMu.Unlock()
				}

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
	var seqNum uint64
	startTime := time.Now()

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
			sentMu.Lock()
			chunk, ok := sentChunks[nackSeq]
			sentMu.Unlock()

			if !ok {
				continue
			}

			hdr := protocol.Header{
				Type:        protocol.PacketData,
				SessionID:   sessionID,
				SequenceNum: nackSeq,
				BlockGroup:  nackSeq / uint64(protocol.DefaultFECConfig().BlockSize),
				PayloadLen:  uint16(len(chunk)),
			}
			if nackSeq == totalChunks-1 {
				hdr.Flags = protocol.FlagEndOfFile
			}

			hdrSize, _ := protocol.MarshalHeader(sendBuf, &hdr)
			copy(sendBuf[hdrSize:], chunk)
			writeFn(sendBuf[:hdrSize+len(chunk)])

			if bucket != nil {
				bucket.Pace(hdrSize + len(chunk))
			}
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
			Type:        protocol.PacketData,
			SessionID:   sessionID,
			SequenceNum: seqNum,
			BlockGroup:  seqNum / uint64(protocol.DefaultFECConfig().BlockSize),
			PayloadLen:  uint16(n),
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

		chunkCopy := make([]byte, n)
		copy(chunkCopy, readBuf[:n])
		sentMu.Lock()
		sentChunks[seqNum] = chunkCopy
		sentMu.Unlock()

		writeFn(sendBuf[:totalSize])

		s.bytesSent.Store(int64(seqNum) * int64(chunkSize))

		if calibration != nil {
			calibration.PacketSent()
		}

		parityResult := blockEncoder.AddShard(hdr.BlockGroup, readBuf[:n])
		if parityResult != nil {
			sendParityPackets(writeFn, sessionID, parityResult, sendBuf, bucket, s.cfg.SendDelay)
		}

		seqNum++

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
		sendParityPackets(writeFn, sessionID, tailResult, sendBuf, bucket, s.cfg.SendDelay)
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
		return s.handleTeardown(conn, writeFn, recvChan, sessionID, msg.pktType, msg.payload, sendBuf, sentChunks, &sentMu, totalChunks, dbgLog)
	default:
	}

	close(doneCh)

	nackMu.Lock()
	pendingNACKs := make([]uint64, 0, len(nackPending))
	for seq := range nackPending {
		pendingNACKs = append(pendingNACKs, seq)
	}
	nackPending = make(map[uint64]struct{})
	nackMu.Unlock()

	retransmitNACKs(writeFn, sessionID, pendingNACKs, sendBuf, sentChunks, &sentMu, totalChunks, bucket)

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
			return s.handleTeardown(conn, writeFn, recvChan, sessionID, pkt.Header.Type, pkt.Payload, sendBuf, sentChunks, &sentMu, totalChunks, dbgLog)

		case protocol.PacketHeartbeat:
			hb, err := protocol.UnmarshalHeartbeat(pkt.Payload)
			if err != nil {
				continue
			}
			if bucket != nil {
				bucket.OnHeartbeat(&hb)
			}
			if len(hb.NACKs) > 0 {
				dbgLog.Printf("[sender] teardown: retransmitting %d NACKed packets", len(hb.NACKs))
				s.nacksSent.Add(int64(len(hb.NACKs)))
				retransmitNACKs(writeFn, sessionID, hb.NACKs, sendBuf, sentChunks, &sentMu, totalChunks, bucket)
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
	sendBuf []byte,
	sentChunks map[uint64][]byte,
	sentMu *sync.Mutex,
	totalChunks uint64,
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
		sentChunks = nil
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
	sentChunks map[uint64][]byte,
	sentMu *sync.Mutex,
	totalChunks uint64,
	bucket *TokenBucket,
) {
	for _, nackSeq := range nacks {
		sentMu.Lock()
		chunk, ok := sentChunks[nackSeq]
		sentMu.Unlock()
		if !ok {
			continue
		}

		hdr := protocol.Header{
			Type:        protocol.PacketData,
			SessionID:   sessionID,
			SequenceNum: nackSeq,
			BlockGroup:  nackSeq / uint64(protocol.DefaultFECConfig().BlockSize),
			PayloadLen:  uint16(len(chunk)),
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
		writeFn(sendBuf[:hs+len(chunk)])
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

		writeFn(sendBuf[:totalSize])

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
