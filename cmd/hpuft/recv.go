package main

import (
	"flag"
	"log"
	"os"

	"hpuft/receiver"
)

func runRecv(args []string) {
	cfg := receiver.DefaultConfig()

	fs := flag.NewFlagSet("recv", flag.ExitOnError)
	fs.Usage = func() {
		log.Print("usage: hpuft recv [-listen :9000] [-out ./output]")
		fs.PrintDefaults()
	}

	fs.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "UDP address to listen on")
	fs.StringVar(&cfg.OutputDir, "out", cfg.OutputDir, "directory to write received files")
	fs.Parse(args)

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
