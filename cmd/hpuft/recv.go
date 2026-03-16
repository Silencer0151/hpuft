package main

import (
	"flag"
	"fmt"
	"hpuft/receiver"
	"log"
	"os"
	"time"
)

func runRecv(args []string) {
	cfg := receiver.DefaultConfig()

	fs := flag.NewFlagSet("recv", flag.ExitOnError)
	var debug bool

	fs.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "UDP address to listen on")
	fs.StringVar(&cfg.OutputDir, "out", cfg.OutputDir, "directory to write received files")
	fs.BoolVar(&debug, "debug", false, "stream raw protocol telemetry to stderr")
	fs.Parse(args)

	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "[recv] create output dir: %v\n", err)
		os.Exit(1)
	}

	cfg.Debug = debug

	if debug {
		log.SetFlags(log.Ltime | log.Lmicroseconds)
		log.SetOutput(os.Stderr)
	}

	fmt.Fprintf(os.Stdout, "[recv] Listening on %s (output: %s)\n", cfg.ListenAddr, cfg.OutputDir)

	r, err := receiver.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[recv] init error: %v\n", err)
		os.Exit(1)
	}

	if !debug {
		done := make(chan struct{})
		go RunRecvProgress(r, done)
		start := time.Now()
		err = r.Run()
		close(done)
		time.Sleep(20 * time.Millisecond) // let progress bar goroutine flush final line

		if err != nil {
			fmt.Fprintf(os.Stderr, "\n[recv] FAILED: %v\n", err)
			os.Exit(1)
		}
		p := r.Progress()
		elapsed := time.Since(start)
		mbps := float64(p.TotalBytes) / elapsed.Seconds() / 1e6
		fmt.Fprintf(os.Stdout, "[recv] TRANSFER COMPLETE in %s (%.1f MB/s) | FEC rebuilt: %d pkts\n",
			elapsed.Round(time.Millisecond), mbps, p.Rebuilt)
	} else {
		if err := r.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "[recv] FAILED: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stdout, "[recv] TRANSFER COMPLETE\n")
	}
}
