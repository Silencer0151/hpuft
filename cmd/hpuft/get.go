package main

import (
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
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
	debug := fs.Bool("debug", false, "stream raw protocol telemetry to stderr")
	fs.Parse(args)

	if *fileName == "" {
		fmt.Fprintln(os.Stderr, "usage: hpuft get -file <name> [-addr host:port] [-out dir] [-debug]")
		os.Exit(1)
	}

	if *debug {
		log.SetFlags(log.Ltime | log.Lmicroseconds)
		log.SetOutput(os.Stderr)
	}

	if err := os.MkdirAll(*outDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "[get] create output dir: %v\n", err)
		os.Exit(1)
	}

	// Bind a local socket on an OS-assigned ephemeral port.
	// Using ListenUDP (not DialUDP) gives us a fixed local port so the NAT
	// mapping created by the outbound PULL_REQ is reused for the inbound
	// SESSION_REQ and all subsequent transfer traffic.
	localConn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[get] bind local socket: %v\n", err)
		os.Exit(1)
	}
	defer localConn.Close()
	localConn.SetReadBuffer(16 * 1024 * 1024)

	rAddr, err := net.ResolveUDPAddr("udp", *serveAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[get] resolve serve addr: %v\n", err)
		os.Exit(1)
	}

	// Generate a session ID. The serve daemon reuses it in its SESSION_REQ.
	sessionID := newGetSessionID()

	// Build and send PULL_REQ — this punches the outbound NAT hole.
	fmt.Fprintf(os.Stdout, "[get] Punching NAT hole via PULL_REQ for %q -> %s\n", *fileName, *serveAddr)

	pullPkt := protocol.Packet{
		Header: protocol.Header{
			Type:      protocol.PacketPullReq,
			SessionID: sessionID,
		},
		Payload: protocol.MarshalPullReq(&protocol.PullReqPayload{FileName: *fileName}),
	}
	pullRaw, err := protocol.MarshalPacket(&pullPkt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[get] marshal PULL_REQ: %v\n", err)
		os.Exit(1)
	}
	if _, err := localConn.WriteToUDP(pullRaw, rAddr); err != nil {
		fmt.Fprintf(os.Stderr, "[get] send PULL_REQ: %v\n", err)
		os.Exit(1)
	}

	// Wait for SESSION_REQ or SESSION_REJECT.
	// All data flows through the single control port — no probe packet needed.
	rawBuf := make([]byte, protocol.MTUHardCap)
	localConn.SetReadDeadline(time.Now().Add(15 * time.Second))

	var serveSenderAddr *net.UDPAddr
	var sessionReq protocol.SessionReqPayload

	for {
		n, from, err := localConn.ReadFromUDP(rawBuf)
		if err != nil {
			if os.IsTimeout(err) {
				fmt.Fprintf(os.Stderr, "[get] timeout: no response from %s after 15s\n", *serveAddr)
				os.Exit(1)
			}
			fmt.Fprintf(os.Stderr, "[get] read: %v\n", err)
			os.Exit(1)
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
			fmt.Fprintf(os.Stderr, "[get] rejected by serve: %s\n", reason)
			os.Exit(1)

		case protocol.PacketSessionReq:
			req, err := protocol.UnmarshalSessionReq(pkt.Payload)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[get] malformed SESSION_REQ: %v\n", err)
				os.Exit(1)
			}
			serveSenderAddr = from
			sessionReq = req
		}

		if serveSenderAddr != nil {
			break
		}
	}

	localConn.SetReadDeadline(time.Time{})

	fmt.Fprintf(os.Stdout, "[get] Received SESSION_REQ. Allocating %s ring buffer...\n",
		humanBytes(int64(sessionReq.FileSize)))

	cfg := receiver.DefaultConfig()
	cfg.OutputDir = *outDir
	cfg.Conn = localConn
	cfg.Debug = *debug
	cfg.IncomingSession = &receiver.IncomingSession{
		SenderAddr: serveSenderAddr,
		SessionID:  sessionID,
		Req:        sessionReq,
	}

	r, err := receiver.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[get] init receiver: %v\n", err)
		os.Exit(1)
	}

	if !*debug {
		done := make(chan struct{})
		go RunRecvProgress(r, done)
		start := time.Now()
		err = r.Run()
		close(done)
		time.Sleep(20 * time.Millisecond)

		if err != nil {
			fmt.Fprintf(os.Stderr, "\n[get] FAILED: %v\n", err)
			os.Exit(1)
		}
		elapsed := time.Since(start)
		mbps := float64(sessionReq.FileSize) / elapsed.Seconds() / 1e6
		fmt.Fprintf(os.Stdout, "[get] TRANSFER COMPLETE: %s in %s (%.1f MB/s) | FEC rebuilt: %d pkts\n",
			*fileName, elapsed.Round(time.Millisecond), mbps, r.Progress().Rebuilt)
	} else {
		if err := r.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "[get] FAILED: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stdout, "[get] TRANSFER COMPLETE\n")
	}
}

func newGetSessionID() uint32 {
	var b [4]byte
	rand.Read(b[:])
	return binary.BigEndian.Uint32(b[:])
}
