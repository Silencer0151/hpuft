package main

import (
	"flag"
	"log"
	"os"

	"hpuft/receiver"
)

func main() {
	cfg := receiver.DefaultConfig()

	flag.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "UDP address to listen on (e.g., :9000)")
	flag.StringVar(&cfg.OutputDir, "out", cfg.OutputDir, "directory to write received files")
	flag.Parse()

	// Ensure output directory exists
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	log.SetFlags(log.Ltime | log.Lmicroseconds)

	r, err := receiver.New(cfg)
	if err != nil {
		log.Fatalf("init receiver: %v", err)
	}

	if err := r.Run(); err != nil {
		log.Fatalf("transfer failed: %v", err)
	}
}
