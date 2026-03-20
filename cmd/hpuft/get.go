package main

import (
	"crypto/ecdh"
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"hpuft/protocol"
	"hpuft/receiver"
	"log"
	"net"
	"os"
	"os/signal"
	"time"
)

func runGet(args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	serveAddr := fs.String("addr", "127.0.0.1:9001", "serve daemon address (host:port)")
	fileName := fs.String("file", "", "name of the file to request (required)")
	outDir := fs.String("out", ".", "directory to write the received file")
	debug := fs.Bool("debug", false, "stream raw protocol telemetry to stderr")
	encrypt := fs.Bool("encrypt", false, "enable AES-128-GCM per-packet encryption")
	fs.Parse(args)

	if *fileName == "" {
		fmt.Fprintln(os.Stderr, "usage: hpuft get -file <name> [-addr host:port] [-out dir] [-debug] [-encrypt]")
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

	// Generate ephemeral key if encrypting.
	var ephemPriv *ecdh.PrivateKey
	if *encrypt {
		ephemPriv, err = protocol.GenerateEphemeralKey()
		if err != nil {
			fmt.Fprintf(os.Stderr, "[get] generate key: %v\n", err)
			os.Exit(1)
		}
	}

	// Build and send PULL_REQ — this punches the outbound NAT hole.
	fmt.Fprintf(os.Stdout, "[get] Punching NAT hole via PULL_REQ for %q -> %s\n", *fileName, *serveAddr)

	pullPayload := &protocol.PullReqPayload{
		FileName:  *fileName,
		Encrypted: *encrypt,
	}
	if *encrypt && ephemPriv != nil {
		copy(pullPayload.PubKey[:], ephemPriv.PublicKey().Bytes())
	}

	pullHdrFlags := protocol.Flag(0)
	if *encrypt {
		pullHdrFlags |= protocol.FlagEncrypted
	}

	pullPkt := protocol.Packet{
		Header: protocol.Header{
			Type:      protocol.PacketPullReq,
			SessionID: sessionID,
			Flags:     pullHdrFlags,
		},
		Payload: protocol.MarshalPullReq(pullPayload),
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
			encrypted := pkt.Header.Flags&protocol.FlagEncrypted != 0
			req, err := protocol.UnmarshalSessionReq(pkt.Payload, encrypted)
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

	// Derive session key if encrypted.
	var getEncKey *[16]byte
	if *encrypt && ephemPriv != nil && sessionReq.Encrypted {
		key, err := protocol.DeriveSessionKey(ephemPriv, sessionReq.PubKey[:], sessionID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[get] derive session key: %v\n", err)
			os.Exit(1)
		}
		getEncKey = &key
	}

	cfg := receiver.DefaultConfig()
	cfg.OutputDir = *outDir
	cfg.Conn = localConn
	cfg.Debug = *debug
	cfg.Encrypt = *encrypt
	cfg.EncKey = getEncKey
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

	// sendDisconnect notifies the sender we're leaving so it can release
	// resources (e.g. the serve daemon's busy flag) immediately.
	sendDisconnect := func() {
		pkt := protocol.Packet{
			Header: protocol.Header{
				Type:      protocol.PacketSessionReject,
				SessionID: sessionID,
			},
			Payload: []byte{byte(protocol.RejectClientDisconnect)},
		}
		if raw, err := protocol.MarshalPacket(&pkt); err == nil {
			localConn.WriteToUDP(raw, serveSenderAddr)
		}
	}

	if !*debug {
		start := time.Now()
		errCh := make(chan error, 1)
		go func() { errCh <- r.Run() }()
		err = RunRecvTUI(r, *fileName, *serveAddr, errCh)

		// If the TUI exited before the transfer completed (Ctrl+C),
		// tell the sender to stop so it releases busy immediately.
		p := r.Progress()
		if p.BytesReceived < p.TotalBytes {
			sendDisconnect()
			fmt.Fprintf(os.Stderr, "[get] interrupted — notified server\n")
			os.Exit(1)
		}

		if err != nil {
			sendDisconnect()
			fmt.Fprintf(os.Stderr, "[get] FAILED: %v\n", err)
			os.Exit(1)
		}
		elapsed := time.Since(start)
		mbps := float64(sessionReq.FileSize) / elapsed.Seconds() / 1e6
		fmt.Fprintf(os.Stdout, "[get] TRANSFER COMPLETE: %s in %s (%.1f MB/s) | FEC rebuilt: %d pkts\n",
			*fileName, elapsed.Round(time.Millisecond), mbps, r.Progress().Rebuilt)
	} else {
		// In debug mode, trap SIGINT to send disconnect before exiting.
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt)
		go func() {
			<-sigCh
			sendDisconnect()
			fmt.Fprintf(os.Stderr, "\n[get] interrupted — notified server\n")
			os.Exit(1)
		}()

		if err := r.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "[get] FAILED: %v\n", err)
			os.Exit(1)
		}
		signal.Stop(sigCh)
		fmt.Fprintf(os.Stdout, "[get] TRANSFER COMPLETE\n")
	}
}

func newGetSessionID() uint32 {
	var b [4]byte
	rand.Read(b[:])
	return binary.BigEndian.Uint32(b[:])
}
