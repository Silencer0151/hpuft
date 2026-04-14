package main

import (
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
	clientID := fs.String("id", "", "optional client identifier (≤32 bytes)")
	debug := fs.Bool("debug", false, "stream raw protocol telemetry to stderr")
	fs.Parse(args)

	if *fileName == "" {
		fmt.Fprintln(os.Stderr, "usage: hpuft get -file <name> [-addr host:port] [-out dir] [-id clientID] [-debug]")
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

	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[get] bind socket: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	conn.SetReadBuffer(16 * 1024 * 1024)

	rAddr, err := net.ResolveUDPAddr("udp", *serveAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[get] resolve addr: %v\n", err)
		os.Exit(1)
	}

	pc, err := protocol.DialConnection(conn, rAddr, *clientID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[get] connect: %v\n", err)
		os.Exit(1)
	}

	respRaw, err := pc.SendRequest(protocol.BuildGetRequest(*fileName))
	if err != nil {
		pc.Close()
		fmt.Fprintf(os.Stderr, "[get] request: %v\n", err)
		os.Exit(1)
	}
	status, reason, body, err := protocol.ParseResponse(respRaw)
	if err != nil {
		pc.Close()
		fmt.Fprintf(os.Stderr, "[get] parse response: %v\n", err)
		os.Exit(1)
	}
	if status != protocol.StatusOK {
		pc.Close()
		fmt.Fprintf(os.Stderr, "[get] rejected: %s\n", reason)
		os.Exit(1)
	}

	// GET OK body: FileSize(8) | FileHash(8) | InitialRate(4)
	if len(body) < 20 {
		pc.Close()
		fmt.Fprintf(os.Stderr, "[get] response body too short: %d bytes\n", len(body))
		os.Exit(1)
	}
	fileSize := binary.BigEndian.Uint64(body[0:8])
	fileHash := binary.BigEndian.Uint64(body[8:16])
	initialRate := binary.BigEndian.Uint32(body[16:20])

	fmt.Fprintf(os.Stdout, "[get] Receiving %s (%s)...\n", *fileName, humanBytes(int64(fileSize)))
	pc.SetState(protocol.ConnTransferring)

	cfg := receiver.DefaultConfig()
	cfg.OutputDir = *outDir
	cfg.Conn = conn
	cfg.Debug = *debug
	cfg.EncKey = &pc.SessionKey
	cfg.IVBase = &pc.IVBase
	cfg.IncomingSession = &receiver.IncomingSession{
		SenderAddr:   pc.RemoteAddr,
		ConnectionID: pc.ID,
		Meta: receiver.TransferMeta{
			FileName:    *fileName,
			FileSize:    fileSize,
			FileHash:    fileHash,
			InitialRate: initialRate,
		},
	}

	r, err := receiver.New(cfg)
	if err != nil {
		pc.Close()
		fmt.Fprintf(os.Stderr, "[get] init receiver: %v\n", err)
		os.Exit(1)
	}

	if !*debug {
		start := time.Now()
		errCh := make(chan error, 1)
		go func() { errCh <- r.Run() }()
		err = RunRecvProgress(r, errCh)
		if err != nil {
			pc.Close()
			fmt.Fprintf(os.Stderr, "[get] FAILED: %v\n", err)
			os.Exit(1)
		}
		p := r.Progress()
		if p.BytesReceived < p.TotalBytes {
			pc.Close()
			fmt.Fprintf(os.Stderr, "[get] interrupted — notified server\n")
			os.Exit(1)
		}
		elapsed := time.Since(start)
		mbps := float64(fileSize) / elapsed.Seconds() / 1e6
		fmt.Fprintf(os.Stdout, "[get] TRANSFER COMPLETE: %s in %s (%.1f MB/s) | FEC rebuilt: %d pkts\n",
			*fileName, elapsed.Round(time.Millisecond), mbps, r.Progress().Rebuilt)
	} else {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt)
		go func() {
			<-sigCh
			pc.Close()
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

	pc.SetState(protocol.ConnIdle)
	pc.Close()
}
