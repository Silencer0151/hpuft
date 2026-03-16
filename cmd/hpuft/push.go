package main

import (
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"hpuft/protocol"
	"hpuft/sender"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"
)

func runPush(args []string) {
	fs := flag.NewFlagSet("push", flag.ExitOnError)
	serveAddr := fs.String("addr", "127.0.0.1:9001", "serve daemon address (host:port)")
	filePath := fs.String("file", "", "path to file to push (required)")
	debug := fs.Bool("debug", false, "stream raw protocol telemetry to stderr")
	fs.Parse(args)

	if *filePath == "" {
		fmt.Fprintln(os.Stderr, "usage: hpuft push -file <path> [-addr host:port] [-debug]")
		os.Exit(1)
	}

	fileInfo, err := os.Stat(*filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[push] error: %v\n", err)
		os.Exit(1)
	}

	if *debug {
		log.SetFlags(log.Ltime | log.Lmicroseconds)
		log.SetOutput(os.Stderr)
	}

	fileName := filepath.Base(*filePath)

	fmt.Fprintf(os.Stdout, "[push] Target: %s (%s)\n", fileName, humanBytes(fileInfo.Size()))
	fmt.Fprintf(os.Stdout, "[push] Pushing to %s...\n", *serveAddr)

	// Bind local socket for control handshake.
	localConn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[push] bind socket: %v\n", err)
		os.Exit(1)
	}
	defer localConn.Close()

	rAddr, err := net.ResolveUDPAddr("udp", *serveAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[push] resolve serve addr: %v\n", err)
		os.Exit(1)
	}

	sessionID := newPushSessionID()

	// Send PUSH_REQ.
	pushPkt := protocol.Packet{
		Header: protocol.Header{
			Type:      protocol.PacketPushReq,
			SessionID: sessionID,
		},
		Payload: protocol.MarshalPushReq(&protocol.PushReqPayload{
			FileSize: uint64(fileInfo.Size()),
			FileName: fileName,
		}),
	}
	raw, err := protocol.MarshalPacket(&pushPkt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[push] marshal PUSH_REQ: %v\n", err)
		os.Exit(1)
	}
	if _, err := localConn.WriteToUDP(raw, rAddr); err != nil {
		fmt.Fprintf(os.Stderr, "[push] send PUSH_REQ: %v\n", err)
		os.Exit(1)
	}

	// Wait for PUSH_ACCEPT or SESSION_REJECT.
	rawBuf := make([]byte, protocol.MTUHardCap)
	localConn.SetReadDeadline(time.Now().Add(15 * time.Second))

	var dataPort uint16
	for {
		n, _, err := localConn.ReadFromUDP(rawBuf)
		if err != nil {
			if os.IsTimeout(err) {
				fmt.Fprintf(os.Stderr, "[push] timeout: no response from %s after 15s\n", *serveAddr)
				os.Exit(1)
			}
			fmt.Fprintf(os.Stderr, "[push] read: %v\n", err)
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
			fmt.Fprintf(os.Stderr, "[push] rejected by serve: %s\n", reason)
			os.Exit(1)

		case protocol.PacketPushAccept:
			accept, err := protocol.UnmarshalPushAccept(pkt.Payload)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[push] malformed PUSH_ACCEPT: %v\n", err)
				os.Exit(1)
			}
			dataPort = accept.Port
		}

		if dataPort != 0 {
			break
		}
	}
	localConn.SetReadDeadline(time.Time{})

	// Build data address: serve's host + ephemeral data port.
	serveHost, _, err := net.SplitHostPort(*serveAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[push] parse serve addr: %v\n", err)
		os.Exit(1)
	}
	dataAddr := fmt.Sprintf("%s:%d", serveHost, dataPort)

	fmt.Fprintf(os.Stdout, "[push] PUSH_ACCEPT received. Sending to data port %d...\n", dataPort)

	cfg := sender.DefaultConfig()
	cfg.FilePath = *filePath
	cfg.RemoteAddr = dataAddr
	cfg.SessionID = sessionID
	cfg.Debug = *debug

	s := sender.New(cfg)

	if !*debug {
		done := make(chan struct{})
		go RunSendProgress(s, done)
		start := time.Now()
		err = s.Send()
		close(done)
		time.Sleep(20 * time.Millisecond)

		if err != nil {
			fmt.Fprintf(os.Stderr, "\n[push] FAILED: %v\n", err)
			os.Exit(1)
		}
		elapsed := time.Since(start)
		mbps := float64(fileInfo.Size()) / elapsed.Seconds() / 1e6
		fmt.Fprintf(os.Stdout, "[push] TRANSFER COMPLETE: %s in %s (%.1f MB/s)\n",
			fileName, elapsed.Round(time.Millisecond), mbps)
	} else {
		if err := s.Send(); err != nil {
			fmt.Fprintf(os.Stderr, "[push] FAILED: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stdout, "[push] TRANSFER COMPLETE\n")
	}
}

func newPushSessionID() uint32 {
	var b [4]byte
	rand.Read(b[:])
	return binary.BigEndian.Uint32(b[:])
}
