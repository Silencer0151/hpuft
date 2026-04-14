package main

import (
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

func runPut(args []string) {
	fs := flag.NewFlagSet("put", flag.ExitOnError)
	serveAddr := fs.String("addr", "127.0.0.1:9001", "serve daemon address (host:port)")
	filePath := fs.String("file", "", "path to file to upload (required)")
	rateMBps := fs.Float64("rate", 0, "initial send rate in MB/s (0 = calibration)")
	clientID := fs.String("id", "", "optional client identifier (≤32 bytes)")
	debug := fs.Bool("debug", false, "protocol/CC telemetry on stderr")
	fs.Parse(args)

	if *filePath == "" {
		fmt.Fprintln(os.Stderr, "usage: hpuft put -file <path> [-addr host:port] [-rate MB/s] [-id clientID] [-debug]")
		os.Exit(1)
	}

	fileInfo, err := os.Stat(*filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[put] stat file: %v\n", err)
		os.Exit(1)
	}

	if *debug {
		log.SetFlags(log.Ltime | log.Lmicroseconds)
		log.SetOutput(os.Stderr)
	}

	fileHash, err := receiver.HashFile(*filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[put] hash file: %v\n", err)
		os.Exit(1)
	}

	fileName := filepath.Base(*filePath)
	fmt.Fprintf(os.Stdout, "[put] Target: %s (%s)\n", fileName, humanBytes(fileInfo.Size()))

	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[put] bind socket: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	conn.SetWriteBuffer(16 * 1024 * 1024)
	conn.SetReadBuffer(16 * 1024 * 1024)

	rAddr, err := net.ResolveUDPAddr("udp", *serveAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[put] resolve addr: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "[put] Connecting to %s...\n", *serveAddr)
	pc, err := protocol.DialConnection(conn, rAddr, *clientID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[put] connect: %v\n", err)
		os.Exit(1)
	}

	initialRate := uint32(*rateMBps * 1e6)
	reqBody := protocol.BuildPutRequest(uint64(fileInfo.Size()), fileHash, initialRate, fileName)
	respRaw, err := pc.SendRequest(reqBody)
	if err != nil {
		pc.Close()
		fmt.Fprintf(os.Stderr, "[put] request: %v\n", err)
		os.Exit(1)
	}
	status, reason, _, err := protocol.ParseResponse(respRaw)
	if err != nil {
		pc.Close()
		fmt.Fprintf(os.Stderr, "[put] parse response: %v\n", err)
		os.Exit(1)
	}
	if status != protocol.StatusOK {
		pc.Close()
		fmt.Fprintf(os.Stderr, "[put] rejected: %s\n", reason)
		os.Exit(1)
	}

	pc.SetState(protocol.ConnTransferring)

	sCfg := sender.DefaultConfig()
	sCfg.FilePath = *filePath
	sCfg.RemoteAddr = *serveAddr
	sCfg.ConnectionID = pc.ID
	sCfg.MuxConn = conn
	sCfg.MuxAddr = rAddr
	sCfg.EncKey = &pc.SessionKey
	sCfg.IVBase = &pc.IVBase
	sCfg.Debug = *debug

	s := sender.New(sCfg)

	if !*debug {
		start := time.Now()
		errCh := make(chan error, 1)
		go func() { errCh <- s.Send() }()
		err = RunSendProgress(s, errCh)
		if err != nil {
			pc.Close()
			fmt.Fprintf(os.Stderr, "[put] FAILED: %v\n", err)
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
			fmt.Fprintf(os.Stdout, "[put] TRANSFER COMPLETE: %s in %s (%.1f MB/s data, +%s repair)\n",
				fileName, elapsed.Round(time.Millisecond), mbps, repairDur.Round(time.Millisecond))
		} else {
			fmt.Fprintf(os.Stdout, "[put] TRANSFER COMPLETE: %s in %s (%.1f MB/s)\n",
				fileName, elapsed.Round(time.Millisecond), mbps)
		}
	} else {
		if err := s.Send(); err != nil {
			pc.Close()
			fmt.Fprintf(os.Stderr, "[put] FAILED: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stdout, "[put] TRANSFER COMPLETE\n")
	}

	pc.SetState(protocol.ConnIdle)
	pc.Close()
}
