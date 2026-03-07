package receiver

import (
	"fmt"
	"hpuft/protocol"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"
)

// Config holds all receiver configuration.
type Config struct {
	ListenAddr string // e.g., ":9000"
	OutputDir  string // directory to write received files
	Session    protocol.SessionConfig
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
	cfg  Config
	conn *net.UDPConn
}

// New creates a new Receiver bound to the configured listen address.
func New(cfg Config) (*Receiver, error) {
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

	return &Receiver{cfg: cfg, conn: conn}, nil
}

// Run listens for and handles a single file transfer, then returns.
// For Phase 1 this handles one session at a time.
func (r *Receiver) Run() error {
	defer r.conn.Close()

	log.Printf("[receiver] listening on %s", r.conn.LocalAddr())

	// --- Phase 1: Wait for SESSION_REQ ---
	rawBuf := make([]byte, protocol.MTUHardCap)
	var senderAddr *net.UDPAddr
	var sessionID uint32
	var reqPayload protocol.SessionReqPayload

	for {
		n, addr, err := r.conn.ReadFromUDP(rawBuf)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}

		pkt, err := protocol.UnmarshalPacket(rawBuf[:n])
		if err != nil {
			log.Printf("[receiver] malformed packet from %s: %v", addr, err)
			continue
		}

		if pkt.Header.Type != protocol.PacketSessionReq {
			continue
		}

		req, err := protocol.UnmarshalSessionReq(pkt.Payload)
		if err != nil {
			log.Printf("[receiver] malformed SESSION_REQ: %v", err)
			continue
		}

		senderAddr = addr
		sessionID = pkt.Header.SessionID
		reqPayload = req

		log.Printf("[receiver] SESSION_REQ from %s: sessionID=0x%08X file=%q size=%d checksum=0x%016X",
			addr, sessionID, req.FileName, req.FileSize, req.Checksum)
		break
	}

	// --- Phase 2: Allocate buffer and disk writer ---
	chunkSize := protocol.MaxPayload

	recvBuf := NewReceiveBuffer(reqPayload.FileSize, chunkSize)
	defer recvBuf.Close()

	outputPath := filepath.Join(r.cfg.OutputDir, filepath.Base(reqPayload.FileName))
	writer, err := NewDiskWriter(recvBuf, outputPath, reqPayload.FileSize, chunkSize)
	if err != nil {
		return fmt.Errorf("create disk writer: %w", err)
	}
	defer writer.Close()

	log.Printf("[receiver] allocated buffer: %d chunks of %d bytes, writing to %s",
		recvBuf.Stats().TotalChunks, chunkSize, outputPath)

	// --- Phase 3: Start heartbeat generator ---
	hbGen := NewHeartbeatGenerator(r.conn, senderAddr, sessionID, recvBuf, writer)
	hbGen.Start()
	defer hbGen.Stop()

	log.Printf("[receiver] heartbeat generator started")

	// --- Phase 3b: Create FEC block decoder ---
	fecCfg := protocol.DefaultFECConfig()
	blockDecoder := NewBlockDecoder(fecCfg.BlockSize, recvBuf)
	log.Printf("[receiver] FEC decoder enabled: block_size=%d", fecCfg.BlockSize)

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

		pkt, err := protocol.UnmarshalPacket(rawBuf[:n])
		if err != nil {
			continue
		}

		if pkt.Header.SessionID != sessionID {
			continue
		}

		switch pkt.Header.Type {
		case protocol.PacketData:
			isNew, err := recvBuf.Insert(pkt.Header.SequenceNum, pkt.Payload)
			if err != nil {
				log.Printf("[receiver] insert error seq=%d: %v", pkt.Header.SequenceNum, err)
				continue
			}

			// Track metrics for heartbeat
			if isNew {
				hbGen.RecordPacket(len(pkt.Payload))
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

	log.Printf("[receiver] all %d chunks received, finalizing...", recvBuf.Stats().TotalChunks)

	computedHash, err := writer.Finalize()
	if err != nil {
		return fmt.Errorf("finalize: %w", err)
	}

	stats := recvBuf.Stats()
	fecStats := blockDecoder.Stats()
	log.Printf("[receiver] transfer stats: received=%d duplicates=%d | FEC: blocks_recovered=%d shards_recovered=%d",
		stats.PacketsReceived, stats.Duplicates, fecStats.BlocksRecovered, fecStats.ShardsRecovered)

	if computedHash != reqPayload.Checksum {
		log.Printf("[receiver] HASH MISMATCH: computed=0x%016X expected=0x%016X",
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

	log.Printf("[receiver] hash verified: 0x%016X", computedHash)

	// --- Phase 6: Graceful Teardown ---
	completePkt := protocol.Packet{
		Header: protocol.Header{
			Type:      protocol.PacketTransferComplete,
			SessionID: sessionID,
		},
	}
	completeRaw, _ := protocol.MarshalPacket(&completePkt)

	ackReceived := false
	for attempt := 0; attempt <= r.cfg.Session.ReceiverTeardownRetries; attempt++ {
		if attempt > 0 {
			log.Printf("[receiver] retransmitting TRANSFER_COMPLETE (attempt %d/%d)",
				attempt, r.cfg.Session.ReceiverTeardownRetries)
		}

		r.conn.WriteToUDP(completeRaw, senderAddr)

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
			break
		}
	}

	if ackReceived {
		log.Printf("[receiver] ACK_CLOSE received, entering linger state")
	} else {
		log.Printf("[receiver] no ACK_CLOSE after retries, proceeding with unilateral teardown (transfer verified)")
	}

	time.Sleep(r.cfg.Session.LingerDuration)

	log.Printf("[receiver] transfer complete: %s (%d bytes)", outputPath, reqPayload.FileSize)
	return nil
}
