package main

import (
	"flag"
	"fmt"
	"hpuft/receiver"
	"hpuft/sender"
	"log"
	"os"
	"path/filepath"
	"time"
)

func runSend(args []string) {
	cfg := sender.DefaultConfig()

	fs := flag.NewFlagSet("send", flag.ExitOnError)
	var delayUS int
	var rateMBps float64
	var noDelay bool
	var noCC bool
	var debug bool

	fs.StringVar(&cfg.RemoteAddr, "addr", cfg.RemoteAddr, "receiver address")
	fs.StringVar(&cfg.FilePath, "file", "", "path to file to send (required)")
	fs.IntVar(&delayUS, "delay", -1, "inter-packet delay in microseconds (disables CC)")
	fs.Float64Var(&rateMBps, "rate", 0, "initial send rate in MB/s")
	fs.BoolVar(&noDelay, "nodelay", false, "send as fast as possible (disables CC)")
	fs.BoolVar(&noCC, "nocc", false, "disable congestion control (use fixed rate)")
	fs.BoolVar(&debug, "debug", false, "stream raw protocol and CC telemetry to stderr")
	fs.BoolVar(&cfg.Encrypt, "encrypt", false, "enable AES-128-GCM per-packet encryption")
	fs.Parse(args)

	if cfg.FilePath == "" {
		fmt.Fprintln(os.Stderr, "usage: hpuft send -file <path> [-addr host:port] [-rate MB/s] [-nodelay] [-nocc] [-debug] [-encrypt]")
		os.Exit(1)
	}

	if noDelay {
		cfg.NoDelay = true
	} else if delayUS >= 0 {
		cfg.SendDelay = time.Duration(delayUS) * time.Microsecond
	} else if rateMBps > 0 {
		cfg.InitialRate = uint32(rateMBps * 1e6)
	}
	if noCC {
		cfg.NoCongestionControl = true
	}
	cfg.Debug = debug

	// --- Human-readable startup (always on stdout) ---
	fileInfo, err := os.Stat(cfg.FilePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[send] error: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "[send] Target: %s (%s)\n",
		filepath.Base(cfg.FilePath), humanBytes(fileInfo.Size()))

	checksum, err := receiver.HashFile(cfg.FilePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[send] hash error: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "[send] Hash:   0x%016X\n", checksum)
	fmt.Fprintf(os.Stdout, "[send] Connecting to %s...\n", cfg.RemoteAddr)

	if debug {
		log.SetFlags(log.Ltime | log.Lmicroseconds)
		log.SetOutput(os.Stderr)
	}

	s := sender.New(cfg)

	if !debug {
		start := time.Now()
		errCh := make(chan error, 1)
		go func() { errCh <- s.Send() }()
		err = RunSendTUI(s, filepath.Base(cfg.FilePath), cfg.RemoteAddr, errCh)

		if err != nil {
			fmt.Fprintf(os.Stderr, "[send] FAILED: %v\n", err)
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
			fmt.Fprintf(os.Stdout, "[send] TRANSFER COMPLETE: %s in %s (%.1f MB/s data, +%s repair)\n",
				filepath.Base(cfg.FilePath), elapsed.Round(time.Millisecond), mbps, repairDur.Round(time.Millisecond))
		} else {
			fmt.Fprintf(os.Stdout, "[send] TRANSFER COMPLETE: %s in %s (%.1f MB/s)\n",
				filepath.Base(cfg.FilePath), elapsed.Round(time.Millisecond), mbps)
		}
	} else {
		if err := s.Send(); err != nil {
			fmt.Fprintf(os.Stderr, "[send] FAILED: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stdout, "[send] TRANSFER COMPLETE\n")
	}
}
