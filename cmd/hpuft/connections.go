package main

import (
	"hpuft/protocol"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// connKey uniquely identifies a client connection by remote address + ID.
type connKey struct {
	Addr string
	ID   uint32
}

// connTable is a thread-safe registry of active server-side connections.
type connTable struct {
	mu    sync.RWMutex
	byKey map[connKey]*serverConn
}

// serverConn wraps protocol.Connection with server-side lifetime state.
// All mutable fields other than those inside protocol.Connection are
// protected by mu.
type serverConn struct {
	*protocol.Connection

	// lastActivityNs is unix nanoseconds of the last received packet.
	// Accessed via atomic store/load so the idle-reaper goroutine can read
	// it without holding the table lock.
	lastActivityNs atomic.Int64

	mu         sync.Mutex
	transferCh chan protocol.RawPacket // non-nil while a transfer goroutine is running
}

// touch records the current time as the last activity timestamp.
func (sc *serverConn) touch() {
	sc.lastActivityNs.Store(time.Now().UnixNano())
}

// lastActivity returns the time of the last recorded activity.
func (sc *serverConn) lastActivity() time.Time {
	return time.Unix(0, sc.lastActivityNs.Load())
}

// getTransferChan returns the active transfer channel (nil when idle).
func (sc *serverConn) getTransferChan() chan protocol.RawPacket {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.transferCh
}

// setTransferChan atomically sets the active transfer channel.
func (sc *serverConn) setTransferChan(ch chan protocol.RawPacket) {
	sc.mu.Lock()
	sc.transferCh = ch
	sc.mu.Unlock()
}

func newConnTable() *connTable {
	return &connTable{byKey: make(map[connKey]*serverConn)}
}

func (t *connTable) get(addr *net.UDPAddr, id uint32) *serverConn {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.byKey[connKey{addr.String(), id}]
}

func (t *connTable) put(sc *serverConn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.byKey[connKey{sc.RemoteAddr.String(), sc.ID}] = sc
}

func (t *connTable) remove(addr *net.UDPAddr, id uint32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.byKey, connKey{addr.String(), id})
}

// reapIdle evicts connections that have been idle for longer than
// protocol.ConnIdleTimeout. Only ConnIdle connections are evicted —
// ConnTransferring connections are left alone even if their LastActivity
// is stale (the transfer itself keeps the session valid).
// Called from a background ticker inside runServe.
func (t *connTable) reapIdle(now time.Time) {
	cutoff := now.Add(-protocol.ConnIdleTimeout)

	t.mu.Lock()
	defer t.mu.Unlock()

	for k, sc := range t.byKey {
		if sc.State() != protocol.ConnIdle {
			continue
		}
		if sc.lastActivity().Before(cutoff) {
			log.Printf("[serve] evicting idle connection %s (ID=0x%08x)", k.Addr, k.ID)
			delete(t.byKey, k)
		}
	}
}
