package main

import (
	"flag"
	"log"
	"time"

	"hpuft/sender"
)

func main() {
	cfg := sender.DefaultConfig()

	var delayUS int
	var rateMBps float64
	var noDelay bool
	var noCC bool

	flag.StringVar(&cfg.RemoteAddr, "addr", cfg.RemoteAddr, "receiver address (e.g., 127.0.0.1:9000)")
	flag.StringVar(&cfg.FilePath, "file", "", "path to file to send (required)")
	flag.IntVar(&delayUS, "delay", -1, "inter-packet delay in microseconds (disables CC)")
	flag.Float64Var(&rateMBps, "rate", 0, "initial send rate in MB/s (CC adjusts from here)")
	flag.BoolVar(&noDelay, "nodelay", false, "send as fast as possible (disables CC)")
	flag.BoolVar(&noCC, "nocc", false, "disable congestion control (use fixed rate)")
	flag.Parse()

	if cfg.FilePath == "" {
		log.Fatal("usage: sender -file <path> [-addr host:port] [-rate MB/s] [-delay µs] [-nodelay] [-nocc]")
	}

	// Apply rate or delay
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

	log.SetFlags(log.Ltime | log.Lmicroseconds)

	s := sender.New(cfg)
	if err := s.Send(); err != nil {
		log.Fatalf("transfer failed: %v", err)
	}
}
