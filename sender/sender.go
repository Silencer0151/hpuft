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

// Sender manages a file transfer session.
type Sender struct {
	cfg Config
}

// New creates a new Sender.
func New(cfg Config) *Sender {
	return &Sender{cfg: cfg}
}

// Send performs a complete file transfer.
func (s *Sender) Send() error {
	// --- Step 1: Read file info and compute hash ---
	fileInfo, err := os.Stat(s.cfg.FilePath)
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}
	fileSize := uint64(fileInfo.Size())

	log.Printf("[sender] hashing file: %s (%d bytes)", s.cfg.FilePath, fileSize)
	checksum, err := receiver.HashFile(s.cfg.FilePath)
	if err != nil {
		return fmt.Errorf("hash file: %w", err)
	}
	log.Printf("[sender] file hash: 0x%016X", checksum)

	// --- Step 2: Open UDP socket ---
	remoteAddr, err := net.ResolveUDPAddr("udp", s.cfg.RemoteAddr)
	if err != nil {
		return fmt.Errorf("resolve remote addr: %w", err)
	}

	conn, err := net.DialUDP("udp", nil, remoteAddr)
	if err != nil {
		return fmt.Errorf("dial udp: %w", err)
	}
	defer conn.Close()

	conn.SetWriteBuffer(16 * 1024 * 1024)

	// --- Step 3: Generate SessionID and send SESSION_REQ ---
	sessionID := generateSessionID()

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

	log.Printf("[sender] sending SESSION_REQ: sessionID=0x%08X -> %s", sessionID, s.cfg.RemoteAddr)
	if _, err := conn.Write(reqRaw); err != nil {
		return fmt.Errorf("send SESSION_REQ: %w", err)
	}

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
		calibration = NewCalibrationState(s.cfg.Calibration, s.cfg.InitialRate)
		log.Printf("[sender] congestion control ENABLED, starting rate=%.2f MB/s", startRate/1e6)
	} else if s.cfg.NoDelay {
		log.Printf("[sender] congestion control DISABLED (nodelay mode)")
	} else if s.cfg.SendDelay > 0 {
		log.Printf("[sender] congestion control DISABLED (fixed delay=%v)", s.cfg.SendDelay)
	}

	// --- Step 5b: Create FEC block encoder ---
	fecCfg := protocol.DefaultFECConfig()
	blockEncoder := NewBlockEncoder(fecCfg)
	log.Printf("[sender] FEC enabled: block_size=%d, initial_parity=%.0f%%",
		fecCfg.BlockSize, fecCfg.InitialParityPct*100)

	// --- Step 6: Start heartbeat listener goroutine ---
	var nackMu sync.Mutex
	// nackPending is a set of sequence numbers awaiting retransmission.
	// Using a map instead of a slice deduplicates NACKs: the same seq
	// arriving in multiple consecutive heartbeats is only retransmitted
	// once per drain cycle, preventing the ~167-entry queue from growing
	// without bound when the receiver persistently reports the same gaps.
	nackPending := make(map[uint64]struct{})

	// --- Step 7 (pre-declared): Chunk cache for NACK retransmission ---
	// Declared here so the heartbeat goroutine can prune acknowledged entries.
	sentChunks := make(map[uint64][]byte)
	var sentMu sync.Mutex

	// doneCh signals the heartbeat goroutine to stop
	doneCh := make(chan struct{})

	// transferComplete is set when we receive TRANSFER_COMPLETE or SESSION_REJECT
	type teardownMsg struct {
		pktType protocol.PacketType
		payload []byte
	}
	teardownCh := make(chan teardownMsg, 1)

	go func() {
		hbBuf := make([]byte, protocol.MTUHardCap)
		for {
			select {
			case <-doneCh:
				return
			default:
			}

			conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			n, err := conn.Read(hbBuf)
			if err != nil {
				if os.IsTimeout(err) {
					continue
				}
				return
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

				// End calibration burst early on first heartbeat
				if calibration != nil {
					calibration.OnHeartbeat()
				}

				// Update FEC parity ratio based on observed loss
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
						log.Printf("[sender] queued %d new NACKs (%d total pending)", newCount, total)
					}
				}

				// Prune acknowledged entries from the chunk cache.
				// All sequences ≤ HighestContiguous have been received at the
				// destination and can never be NACKed again. Without pruning,
				// sentChunks retains every chunk for the entire transfer
				// (~470 MB for a 237 MB file), causing repeated GC pauses.
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
		// --- Priority: retransmit NACKed packets ---
		// Cap retransmits per iteration so new data always makes forward progress.
		// Without this, 100+ NACKs monopolize the link and seqNum never advances.
		// nackPending is a deduplicating set: the same seq from multiple
		// consecutive heartbeats is only drained once.
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
				// Cache was pruned because HighestContiguous advanced past this
				// sequence (FEC or late arrival recovered it). Silently skip.
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

			conn.Write(sendBuf[:hdrSize+len(chunk)])

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

		// Add calibration flag during burst phase
		if calibration != nil {
			hdr.Flags |= calibration.Flags()
		}

		hdrSize, _ := protocol.MarshalHeader(sendBuf, &hdr)
		copy(sendBuf[hdrSize:], readBuf[:n])
		totalSize := hdrSize + n

		// Cache for NACK retransmission
		chunkCopy := make([]byte, n)
		copy(chunkCopy, readBuf[:n])
		sentMu.Lock()
		sentChunks[seqNum] = chunkCopy
		sentMu.Unlock()

		if _, err := conn.Write(sendBuf[:totalSize]); err != nil {
			close(doneCh)
			return fmt.Errorf("send DATA seq=%d: %w", seqNum, err)
		}

		// Track calibration burst progress
		if calibration != nil {
			calibration.PacketSent()
		}

		// --- FEC: feed to block encoder, send parity if block is complete ---
		parityResult := blockEncoder.AddShard(hdr.BlockGroup, readBuf[:n])
		if parityResult != nil {
			sendParityPackets(conn, sessionID, parityResult, sendBuf, bucket, s.cfg.SendDelay)
		}

		seqNum++

		// --- Pacing ---
		// During calibration burst: use fixed spacing
		// After calibration: use token bucket or fixed delay
		if calibration != nil && calibration.Pace() {
			// calibration handled the pacing
		} else if bucket != nil {
			bucket.Pace(totalSize)
		} else if s.cfg.SendDelay > 0 {
			time.Sleep(s.cfg.SendDelay)
		}

		// Progress logging every 10%
		if totalChunks >= 10 && seqNum%(totalChunks/10) == 0 {
			elapsed := time.Since(startTime)
			bytesPerSec := float64(seqNum*uint64(chunkSize)) / elapsed.Seconds()
			pct := float64(seqNum) / float64(totalChunks) * 100

			if bucket != nil {
				rate := bucket.Rate()
				log.Printf("[sender] progress: %.0f%% (%d/%d) actual=%.2f MB/s target=%.2f MB/s",
					pct, seqNum, totalChunks, bytesPerSec/1e6, rate/1e6)
			} else {
				log.Printf("[sender] progress: %.0f%% (%d/%d) rate=%.2f MB/s",
					pct, seqNum, totalChunks, bytesPerSec/1e6)
			}
		}
	}

	// --- FEC: flush tail block parity ---
	tailResult := blockEncoder.FlushTail()
	if tailResult != nil {
		sendParityPackets(conn, sessionID, tailResult, sendBuf, bucket, s.cfg.SendDelay)
		log.Printf("[sender] sent %d tail block parity packets for block %d (%d data shards)",
			tailResult.ParityCount, tailResult.BlockGroup, tailResult.DataCount)
	}

	elapsed := time.Since(startTime)
	bytesPerSec := float64(fileSize) / elapsed.Seconds()

	if bucket != nil {
		stats := bucket.Stats()
		log.Printf("[sender] all %d packets sent in %v (%.2f MB/s) | CC: +%d =%d -%d",
			seqNum, elapsed.Round(time.Millisecond), bytesPerSec/1e6,
			stats.Increases, stats.Holds, stats.Decreases)
	} else {
		log.Printf("[sender] all %d packets sent in %v (%.2f MB/s)",
			seqNum, elapsed.Round(time.Millisecond), bytesPerSec/1e6)
	}

	log.Printf("[sender] waiting for TRANSFER_COMPLETE...")

	// --- Step 9: Wait for TRANSFER_COMPLETE ---
	// Stop the heartbeat goroutine so it doesn't compete for socket reads.
	// Check if it already received a teardown message first.
	select {
	case msg := <-teardownCh:
		close(doneCh)
		return s.handleTeardown(conn, sessionID, msg.pktType, msg.payload, sendBuf, sentChunks, &sentMu, totalChunks)
	default:
	}

	close(doneCh) // stop heartbeat goroutine

	// Drain any NACKs that were queued before we stopped the goroutine
	nackMu.Lock()
	pendingNACKs := make([]uint64, 0, len(nackPending))
	for seq := range nackPending {
		pendingNACKs = append(pendingNACKs, seq)
	}
	nackPending = make(map[uint64]struct{})
	nackMu.Unlock()

	retransmitNACKs(conn, sessionID, pendingNACKs, sendBuf, sentChunks, &sentMu, totalChunks)

	// Now we own the socket — handle teardown synchronously
	conn.SetReadDeadline(time.Now().Add(s.cfg.Session.SenderProbeTimeout))
	rawBuf := make([]byte, protocol.MTUHardCap)

	for {

		n, err := conn.Read(rawBuf)
		if err != nil {
			if os.IsTimeout(err) {
				return fmt.Errorf("timeout waiting for TRANSFER_COMPLETE")
			}
			return fmt.Errorf("read: %w", err)
		}

		pkt, err := protocol.UnmarshalPacket(rawBuf[:n])
		if err != nil {
			continue
		}
		if pkt.Header.SessionID != sessionID {
			continue
		}

		switch pkt.Header.Type {
		case protocol.PacketTransferComplete, protocol.PacketSessionReject:
			return s.handleTeardown(conn, sessionID, pkt.Header.Type, pkt.Payload, sendBuf, sentChunks, &sentMu, totalChunks)

		case protocol.PacketHeartbeat:
			hb, err := protocol.UnmarshalHeartbeat(pkt.Payload)
			if err != nil {
				continue
			}
			if bucket != nil {
				bucket.OnHeartbeat(&hb)
			}
			if len(hb.NACKs) > 0 {
				log.Printf("[sender] teardown: retransmitting %d NACKed packets", len(hb.NACKs))
				retransmitNACKs(conn, sessionID, hb.NACKs, sendBuf, sentChunks, &sentMu, totalChunks)
			}
			// Reset deadline — we're still making progress
			conn.SetReadDeadline(time.Now().Add(s.cfg.Session.SenderProbeTimeout))
		}
	}
}

// handleTeardown processes TRANSFER_COMPLETE or SESSION_REJECT and manages linger.
func (s *Sender) handleTeardown(
	conn *net.UDPConn,
	sessionID uint32,
	pktType protocol.PacketType,
	payload []byte,
	sendBuf []byte,
	sentChunks map[uint64][]byte,
	sentMu *sync.Mutex,
	totalChunks uint64,
) error {
	switch pktType {
	case protocol.PacketTransferComplete:
		log.Printf("[sender] received TRANSFER_COMPLETE")

		ackPkt := protocol.Packet{
			Header: protocol.Header{
				Type:      protocol.PacketACKClose,
				SessionID: sessionID,
			},
		}
		ackRaw, _ := protocol.MarshalPacket(&ackPkt)
		conn.Write(ackRaw)

		log.Printf("[sender] sent ACK_CLOSE, entering linger state")

		lingerEnd := time.Now().Add(s.cfg.Session.LingerDuration)
		conn.SetReadDeadline(lingerEnd)
		rawBuf := make([]byte, protocol.MTUHardCap)

		for time.Now().Before(lingerEnd) {
			n, err := conn.Read(rawBuf)
			if err != nil {
				break
			}
			pkt, err := protocol.UnmarshalPacket(rawBuf[:n])
			if err != nil {
				continue
			}
			if pkt.Header.Type == protocol.PacketTransferComplete &&
				pkt.Header.SessionID == sessionID {
				conn.Write(ackRaw)
			}
		}

		sentChunks = nil
		log.Printf("[sender] linger complete, session finished")
		return nil

	case protocol.PacketSessionReject:
		if len(payload) > 0 {
			reason := protocol.RejectReason(payload[0])
			return fmt.Errorf("transfer rejected by receiver: %s", reason)
		}
		return fmt.Errorf("transfer rejected by receiver")

	default:
		return fmt.Errorf("unexpected teardown packet type: %s", pktType)
	}
}

// retransmitNACKs sends cached data packets for the given sequence numbers.
func retransmitNACKs(
	conn *net.UDPConn,
	sessionID uint32,
	nacks []uint64,
	sendBuf []byte,
	sentChunks map[uint64][]byte,
	sentMu *sync.Mutex,
	totalChunks uint64,
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

		hs, _ := protocol.MarshalHeader(sendBuf, &hdr)
		copy(sendBuf[hs:], chunk)
		conn.Write(sendBuf[:hs+len(chunk)])
	}
}

// sendParityPackets transmits parity packets for a completed FEC block.
func sendParityPackets(
	conn *net.UDPConn,
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
			SequenceNum: uint64(i), // parity index within block
			BlockGroup:  result.BlockGroup,
			PayloadLen:  uint16(len(payload)),
		}

		// Set EOF flag on the last parity packet if this is potentially the tail block
		// (The receiver uses this to know no more blocks are coming)

		hdrSize, _ := protocol.MarshalHeader(sendBuf, &hdr)
		copy(sendBuf[hdrSize:], payload)
		totalSize := hdrSize + len(payload)

		conn.Write(sendBuf[:totalSize])

		// Pace parity packets too
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
