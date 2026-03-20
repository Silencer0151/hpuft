package main

import (
	"flag"
	"fmt"
	"hpuft/receiver"
	"log"
	"os"
	"os/signal"
	"time"
)

func runRecv(args []string) {
	cfg := receiver.DefaultConfig()

	fs := flag.NewFlagSet("recv", flag.ExitOnError)
	var debug bool

	fs.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "UDP address to listen on")
	fs.StringVar(&cfg.OutputDir, "out", cfg.OutputDir, "directory to write received files")
	fs.BoolVar(&debug, "debug", false, "stream raw protocol telemetry to stderr")
	fs.BoolVar(&cfg.Encrypt, "encrypt", false, "enable AES-128-GCM per-packet encryption")
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
		start := time.Now()
		errCh := make(chan error, 1)
		go func() { errCh <- r.Run() }()
		err = RunRecvTUI(r, "receiving", cfg.ListenAddr, errCh)

		// If the TUI exited before the transfer completed (Ctrl+C),
		// tell the sender to stop immediately.
		p := r.Progress()
		if p.BytesReceived < p.TotalBytes {
			r.SendDisconnect()
			fmt.Fprintf(os.Stderr, "[recv] interrupted — notified sender\n")
			os.Exit(1)
		}

		if err != nil {
			r.SendDisconnect()
			fmt.Fprintf(os.Stderr, "[recv] FAILED: %v\n", err)
			os.Exit(1)
		}
		elapsed := time.Since(start)
		mbps := float64(p.TotalBytes) / elapsed.Seconds() / 1e6
		fmt.Fprintf(os.Stdout, "[recv] TRANSFER COMPLETE in %s (%.1f MB/s) | FEC rebuilt: %d pkts\n",
			elapsed.Round(time.Millisecond), mbps, p.Rebuilt)
	} else {
		// In debug mode, trap SIGINT to send disconnect before exiting.
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt)
		go func() {
			<-sigCh
			r.SendDisconnect()
			fmt.Fprintf(os.Stderr, "\n[recv] interrupted — notified sender\n")
			os.Exit(1)
		}()

		if err := r.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "[recv] FAILED: %v\n", err)
			os.Exit(1)
		}
		signal.Stop(sigCh)
		fmt.Fprintf(os.Stdout, "[recv] TRANSFER COMPLETE\n")
	}
}
