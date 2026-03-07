package receiver

import (
	"hpuft/protocol"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// HeartbeatGenerator periodically sends HEARTBEAT packets to the sender
// with network delivery rate, storage flush rate, loss metrics, and NACKs.
type HeartbeatGenerator struct {
	mu sync.Mutex

	conn      *net.UDPConn
	peerAddr  *net.UDPAddr
	sessionID uint32

	buf    *ReceiveBuffer
	writer *DiskWriter

	// Metrics tracking
	bytesReceivedWindow   atomic.Int64 // bytes received since last heartbeat
	packetsReceivedWindow atomic.Int64
	packetsLostWindow     atomic.Int64

	// Previous flush position for computing storage rate
	lastFlushBytes uint64
	lastBeatTime   time.Time

	// Current interval (rate-proportional)
	interval time.Duration

	// Stop channel
	stopCh   chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// NewHeartbeatGenerator creates a heartbeat generator.
func NewHeartbeatGenerator(
	conn *net.UDPConn,
	peerAddr *net.UDPAddr,
	sessionID uint32,
	buf *ReceiveBuffer,
	writer *DiskWriter,
) *HeartbeatGenerator {
	return &HeartbeatGenerator{
		conn:         conn,
		peerAddr:     peerAddr,
		sessionID:    sessionID,
		buf:          buf,
		writer:       writer,
		interval:     100 * time.Millisecond, // start at lowest tier
		lastBeatTime: time.Now(),
		stopCh:       make(chan struct{}),
		done:         make(chan struct{}),
	}
}

// Start begins the heartbeat loop in a background goroutine.
func (hg *HeartbeatGenerator) Start() {
	go hg.loop()
}

// Stop signals the heartbeat loop to exit and waits for it to finish.
// Safe to call multiple times.
func (hg *HeartbeatGenerator) Stop() {
	hg.stopOnce.Do(func() {
		close(hg.stopCh)
	})
	<-hg.done
}

// RecordPacket should be called for every DATA packet received.
// Updates the metrics window for the next heartbeat.
func (hg *HeartbeatGenerator) RecordPacket(payloadLen int) {
	hg.bytesReceivedWindow.Add(int64(payloadLen))
	hg.packetsReceivedWindow.Add(1)
}

// RecordLoss should be called when a packet is determined lost
// (not recoverable by FEC). Updates loss metrics.
func (hg *HeartbeatGenerator) RecordLoss(count int) {
	hg.packetsLostWindow.Add(int64(count))
}

func (hg *HeartbeatGenerator) loop() {
	defer close(hg.done)

	ticker := time.NewTicker(hg.interval)
	defer ticker.Stop()

	for {
		select {
		case <-hg.stopCh:
			return
		case <-ticker.C:
			hg.sendHeartbeat()

			// Update interval based on observed rate
			newInterval := hg.computeInterval()
			if newInterval != hg.interval {
				hg.interval = newInterval
				ticker.Reset(newInterval)
			}
		}
	}
}

func (hg *HeartbeatGenerator) sendHeartbeat() {
	hg.mu.Lock()
	defer hg.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(hg.lastBeatTime).Seconds()
	if elapsed <= 0 {
		elapsed = 0.001 // guard against zero division
	}

	// --- Network Delivery Rate ---
	bytesReceived := hg.bytesReceivedWindow.Swap(0)
	networkRate := uint32(float64(bytesReceived) / elapsed)

	// --- Storage Flush Rate ---
	currentFlush := hg.writer.BytesWritten()
	flushedSinceLastBeat := currentFlush - hg.lastFlushBytes
	storageRate := uint32(float64(flushedSinceLastBeat) / elapsed)
	hg.lastFlushBytes = currentFlush

	// --- Loss Rate ---
	packetsReceived := hg.packetsReceivedWindow.Swap(0)
	packetsLost := hg.packetsLostWindow.Swap(0)
	totalPackets := packetsReceived + packetsLost
	var lossBasisPoints uint16
	if totalPackets > 0 {
		lossBasisPoints = uint16(packetsLost * 10000 / totalPackets)
	}

	// --- Highest Contiguous ---
	highestContig := hg.buf.HighestContiguous()
	var hcUint64 uint64
	if highestContig >= 0 {
		hcUint64 = uint64(highestContig)
	}

	// --- NACKs ---
	// Only NACK sequences between the contiguous frontier and the highest
	// sequence number we've actually received. Never NACK sequences the
	// sender hasn't transmitted yet.
	stats := hg.buf.Stats()
	_ = stats // available for future use
	highestReceived := hg.buf.HighestReceived()
	var nacks []uint64
	if highestContig >= 0 && highestReceived > uint64(highestContig) {
		scanStart := uint64(highestContig) + 1
		scanEnd := highestReceived + 1
		// Cap scan window to avoid huge NACK lists
		if scanEnd-scanStart > 1000 {
			scanEnd = scanStart + 1000
		}
		nacks = hg.buf.MissingInRange(scanStart, scanEnd)

		// Cap NACK array to fit in a single packet
		// Max NACK payload: (MaxPayload - HeartbeatFixedSize) / 8
		maxNACKs := (protocol.MaxPayload - protocol.HeartbeatFixedSize) / 8
		if len(nacks) > maxNACKs {
			nacks = nacks[:maxNACKs]
		}
	}

	hg.lastBeatTime = now

	// Build and send heartbeat
	payload := protocol.HeartbeatPayload{
		NetworkDeliveryRate: networkRate,
		StorageFlushRate:    storageRate,
		LossRate:            lossBasisPoints,
		HighestContiguous:   hcUint64,
		NACKCount:           uint16(len(nacks)),
		NACKs:               nacks,
	}

	pkt := protocol.Packet{
		Header: protocol.Header{
			Type:      protocol.PacketHeartbeat,
			SessionID: hg.sessionID,
		},
		Payload: protocol.MarshalHeartbeat(&payload),
	}

	raw, err := protocol.MarshalPacket(&pkt)
	if err != nil {
		log.Printf("[heartbeat] marshal error: %v", err)
		return
	}

	if _, err := hg.conn.WriteToUDP(raw, hg.peerAddr); err != nil {
		log.Printf("[heartbeat] send error: %v", err)
	}
}

// computeInterval determines the heartbeat interval based on observed
// receive rate, per spec §6A.
func (hg *HeartbeatGenerator) computeInterval() time.Duration {
	// Use the last window's bytes to estimate rate
	// Since we already swapped the counter, use network rate from last beat
	// We'll approximate from the buffer stats
	stats := hg.buf.Stats()
	if stats.PacketsReceived == 0 {
		return 100 * time.Millisecond
	}

	// Rough estimate: total bytes received / total elapsed time
	// For a more accurate measure, we'd track cumulative bytes,
	// but for interval selection this is sufficient
	return protocol.HeartbeatInterval(uint64(hg.writer.BytesWritten()))
}
