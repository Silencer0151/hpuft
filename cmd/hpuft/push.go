package main

import (
	"crypto/ecdh"
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"hpuft/protocol"
	"hpuft/receiver"
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
	encrypt := fs.Bool("encrypt", false, "enable AES-128-GCM per-packet encryption")
	fs.Parse(args)

	if *filePath == "" {
		fmt.Fprintln(os.Stderr, "usage: hpuft push -file <path> [-addr host:port] [-debug] [-encrypt]")
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

	fileHash, err := receiver.HashFile(*filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[push] hash file: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "[push] Target: %s (%s)\n", fileName, humanBytes(fileInfo.Size()))
	fmt.Fprintf(os.Stdout, "[push] Pushing to %s...\n", *serveAddr)

	// Bind local socket for control handshake.
	localConn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[push] bind socket: %v\n", err)
		os.Exit(1)
	}
	defer localConn.Close()
	localConn.SetWriteBuffer(16 * 1024 * 1024)

	rAddr, err := net.ResolveUDPAddr("udp", *serveAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[push] resolve serve addr: %v\n", err)
		os.Exit(1)
	}

	sessionID := newPushSessionID()

	// Generate ephemeral key if encrypting.
	var ephemPriv *ecdh.PrivateKey
	if *encrypt {
		ephemPriv, err = protocol.GenerateEphemeralKey()
		if err != nil {
			fmt.Fprintf(os.Stderr, "[push] generate key: %v\n", err)
			os.Exit(1)
		}
	}

	// Build PUSH_REQ payload.
	pushReqPayload := &protocol.PushReqPayload{
		FileSize:  uint64(fileInfo.Size()),
		FileHash:  fileHash,
		FileName:  fileName,
		Encrypted: *encrypt,
	}
	if *encrypt && ephemPriv != nil {
		copy(pushReqPayload.PubKey[:], ephemPriv.PublicKey().Bytes())
	}

	pushHdrFlags := protocol.Flag(0)
	if *encrypt {
		pushHdrFlags |= protocol.FlagEncrypted
	}

	// Send PUSH_REQ.
	pushPkt := protocol.Packet{
		Header: protocol.Header{
			Type:      protocol.PacketPushReq,
			SessionID: sessionID,
			Flags:     pushHdrFlags,
		},
		Payload: protocol.MarshalPushReq(pushReqPayload),
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

	var pushEncKey *[16]byte
	var pushIVBase *[8]byte
	accepted := false
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
			accept, err := protocol.UnmarshalPushAccept(pkt.Payload, *encrypt)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[push] malformed PUSH_ACCEPT: %v\n", err)
				os.Exit(1)
			}
			if *encrypt && ephemPriv != nil {
				key, derivedIVBase, err := protocol.DeriveSessionKey(ephemPriv, accept.PubKey[:], sessionID)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[push] derive session key: %v\n", err)
					os.Exit(1)
				}
				pushEncKey = &key
				pushIVBase = &derivedIVBase
			}
			accepted = true
		}

		if accepted {
			break
		}
	}
	localConn.SetReadDeadline(time.Time{})

	// Data flows over the same control socket — no new connection needed.
	fmt.Fprintf(os.Stdout, "[push] PUSH_ACCEPT received. Starting transfer...\n")

	cfg := sender.DefaultConfig()
	cfg.FilePath = *filePath
	cfg.RemoteAddr = *serveAddr // for logging
	cfg.SessionID = sessionID
	cfg.MuxConn = localConn
	cfg.MuxAddr = rAddr
	cfg.PushFlow = true // session already established via PUSH_REQ/PUSH_ACCEPT
	cfg.Debug = *debug
	if *encrypt {
		cfg.Encrypt = true
		cfg.EncKey = pushEncKey
		cfg.IVBase = pushIVBase
	}

	s := sender.New(cfg)

	if !*debug {
		start := time.Now()
		errCh := make(chan error, 1)
		go func() { errCh <- s.Send() }()
		err = RunSendTUI(s, fileName, *serveAddr, errCh)

		if err != nil {
			fmt.Fprintf(os.Stderr, "[push] FAILED: %v\n", err)
			os.Exit(1)
		}
		elapsed := time.Since(start)
		p := s.Progress()
		dataElapsed := elapsed
		repairDur := time.Duration(0)
		if p.RepairStartNs > 0 && p.StartNs > 0 {
			dataElapsed = time.Duration(p.RepairStartNs - p.StartNs)
			repairDur = elapsed - dataElapsed
		}
		mbps := float64(fileInfo.Size()) / dataElapsed.Seconds() / 1e6
		if repairDur > 0 {
			fmt.Fprintf(os.Stdout, "[push] TRANSFER COMPLETE: %s in %s (%.1f MB/s data, +%s repair)\n",
				fileName, elapsed.Round(time.Millisecond), mbps, repairDur.Round(time.Millisecond))
		} else {
			fmt.Fprintf(os.Stdout, "[push] TRANSFER COMPLETE: %s in %s (%.1f MB/s)\n",
				fileName, elapsed.Round(time.Millisecond), mbps)
		}
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
