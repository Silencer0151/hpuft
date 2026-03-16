package main

import (
	"flag"
	"hpuft/protocol"
	"hpuft/sender"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
)

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listenAddr := fs.String("listen", ":9001", "address to listen on for PULL_REQ")
	dir := fs.String("dir", ".", "directory of files available to serve")
	fs.Parse(args)

	manifest := buildManifest(*dir)
	log.Printf("[serve] manifest: %d files available in %s", len(manifest), *dir)

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

	log.Printf("[serve] listening on %s", conn.LocalAddr())

	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// busy is 0 when idle, 1 when a transfer is in progress.
	// Single-lane: exactly one transfer at a time.
	var busy int32

	rawBuf := make([]byte, protocol.MTUHardCap)

	for {
		n, clientAddr, err := conn.ReadFromUDP(rawBuf)
		if err != nil {
			log.Printf("[serve] read error: %v", err)
			continue
		}

		pkt, err := protocol.UnmarshalPacket(rawBuf[:n])
		if err != nil || pkt.Header.Type != protocol.PacketPullReq {
			continue
		}

		req, err := protocol.UnmarshalPullReq(pkt.Payload)
		if err != nil || req.FileName == "" {
			log.Printf("[serve] malformed PULL_REQ from %s: %v", clientAddr, err)
			continue
		}

		// --- Busy check ---
		if !atomic.CompareAndSwapInt32(&busy, 0, 1) {
			sendReject(conn, clientAddr, pkt.Header.SessionID, protocol.RejectServerBusy)
			log.Printf("[serve] busy — rejected %q from %s", req.FileName, clientAddr)
			continue
		}

		// --- Manifest check (allowlist) ---
		filePath, ok := manifest[req.FileName]
		if !ok {
			sendReject(conn, clientAddr, pkt.Header.SessionID, protocol.RejectFileNotFound)
			atomic.StoreInt32(&busy, 0)
			log.Printf("[serve] file not found: %q (requested by %s)", req.FileName, clientAddr)
			continue
		}

		log.Printf("[serve] serving %q to %s (sessionID=0x%08X)", req.FileName, clientAddr, pkt.Header.SessionID)

		// Run the transfer in a goroutine so the listener loop stays live.
		// The sender dials a fresh connection to clientAddr; port-restricted
		// cone NAT on the client side will accept it because the client
		// previously sent to our IP (which is what punches the hole).
		go func(filePath, clientAddrStr string, sessionID uint32) {
			defer atomic.StoreInt32(&busy, 0)

			cfg := sender.DefaultConfig()
			cfg.FilePath = filePath
			cfg.RemoteAddr = clientAddrStr
			cfg.SessionID = sessionID // reuse the ID from PULL_REQ

			s := sender.New(cfg)
			if err := s.Send(); err != nil {
				log.Printf("[serve] transfer failed: %v", err)
			}
		}(filePath, clientAddr.String(), pkt.Header.SessionID)
	}
}

// buildManifest scans dir (non-recursively) and returns an allowlist map of
// filename → absolute path. Only regular files are included; directories and
// symlinks are skipped. The map is built once at startup so requests for files
// added after launch are not served (intentional security boundary).
func buildManifest(dir string) map[string]string {
	manifest := make(map[string]string)

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
		manifest[name] = abs
	}

	return manifest
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
