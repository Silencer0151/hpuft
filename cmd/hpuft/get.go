package main

import (
	"crypto/rand"
	"encoding/binary"
	"flag"
	"hpuft/protocol"
	"hpuft/receiver"
	"log"
	"net"
	"os"
	"time"
)

func runGet(args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	serveAddr := fs.String("addr", "127.0.0.1:9001", "serve daemon address (host:port)")
	fileName := fs.String("file", "", "name of the file to request (required)")
	outDir := fs.String("out", ".", "directory to write the received file")
	fs.Parse(args)

	if *fileName == "" {
		log.Fatal("usage: hpuft get -file <name> [-addr host:port] [-out dir]")
	}

	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// Ensure output directory exists.
	if err := os.MkdirAll(*outDir, 0755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	// Bind a local socket on an OS-assigned ephemeral port.
	// Using ListenUDP (not DialUDP) gives us a fixed local port so the NAT
	// mapping created by the outbound PULL_REQ is reused for the inbound
	// SESSION_REQ and all subsequent transfer traffic.
	localConn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		log.Fatalf("bind local socket: %v", err)
	}
	defer localConn.Close()
	localConn.SetReadBuffer(16 * 1024 * 1024)

	log.Printf("[get] local socket: %s", localConn.LocalAddr())

	// Resolve serve address.
	rAddr, err := net.ResolveUDPAddr("udp", *serveAddr)
	if err != nil {
		log.Fatalf("resolve serve addr: %v", err)
	}

	// Generate a session ID. The serve daemon will reuse this ID in its
	// SESSION_REQ so both sides agree without a separate negotiation step.
	sessionID := newGetSessionID()

	// Build and send PULL_REQ — this punches the outbound NAT hole.
	pullPkt := protocol.Packet{
		Header: protocol.Header{
			Type:      protocol.PacketPullReq,
			SessionID: sessionID,
		},
		Payload: protocol.MarshalPullReq(&protocol.PullReqPayload{FileName: *fileName}),
	}
	pullRaw, err := protocol.MarshalPacket(&pullPkt)
	if err != nil {
		log.Fatalf("marshal PULL_REQ: %v", err)
	}

	if _, err := localConn.WriteToUDP(pullRaw, rAddr); err != nil {
		log.Fatalf("send PULL_REQ: %v", err)
	}
	log.Printf("[get] PULL_REQ sent for %q to %s (sessionID=0x%08X)", *fileName, *serveAddr, sessionID)

	// Wait for SESSION_REQ or SESSION_REJECT.
	// The serve daemon dials back to our address; port-restricted cone NAT
	// allows inbound from the serve IP on any port once we've sent to it.
	rawBuf := make([]byte, protocol.MTUHardCap)
	localConn.SetReadDeadline(time.Now().Add(15 * time.Second))

	var serveSenderAddr *net.UDPAddr
	var sessionReq protocol.SessionReqPayload

	for {
		n, from, err := localConn.ReadFromUDP(rawBuf)
		if err != nil {
			if os.IsTimeout(err) {
				log.Fatalf("[get] timeout: no response from %s after 15s", *serveAddr)
			}
			log.Fatalf("[get] read: %v", err)
		}

		pkt, err := protocol.UnmarshalPacket(rawBuf[:n])
		if err != nil {
			continue
		}
		if pkt.Header.SessionID != sessionID {
			continue
		}

		switch pkt.Header.Type {
		case protocol.PacketSessionReject:
			reason := "unknown"
			if len(pkt.Payload) > 0 {
				reason = protocol.RejectReason(pkt.Payload[0]).String()
			}
			log.Fatalf("[get] rejected by serve: %s", reason)

		case protocol.PacketSessionReq:
			req, err := protocol.UnmarshalSessionReq(pkt.Payload)
			if err != nil {
				log.Fatalf("[get] malformed SESSION_REQ: %v", err)
			}
			serveSenderAddr = from
			sessionReq = req
			log.Printf("[get] SESSION_REQ received from %s: file=%q size=%d",
				from, req.FileName, req.FileSize)
		}

		if serveSenderAddr != nil {
			break
		}
	}

	// Clear the deadline before handing off to the receiver.
	localConn.SetReadDeadline(time.Time{})

	// Hand the existing socket and pre-parsed session to the receiver.
	// Phase 1 (waiting for SESSION_REQ) is skipped since we already have it.
	cfg := receiver.DefaultConfig()
	cfg.OutputDir = *outDir
	cfg.Conn = localConn
	cfg.IncomingSession = &receiver.IncomingSession{
		SenderAddr: serveSenderAddr,
		SessionID:  sessionID,
		Req:        sessionReq,
	}

	r, err := receiver.New(cfg)
	if err != nil {
		log.Fatalf("[get] init receiver: %v", err)
	}

	if err := r.Run(); err != nil {
		log.Fatalf("[get] transfer failed: %v", err)
	}
}

func newGetSessionID() uint32 {
	var b [4]byte
	rand.Read(b[:])
	return binary.BigEndian.Uint32(b[:])
}
