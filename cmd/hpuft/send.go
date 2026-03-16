package main

import (
	"flag"
	"log"
	"time"

	"hpuft/sender"
)

func runSend(args []string) {
	cfg := sender.DefaultConfig()

	fs := flag.NewFlagSet("send", flag.ExitOnError)
	fs.Usage = func() {
		log.Print("usage: hpuft send -file <path> [-addr host:port] [-rate MB/s] [-delay µs] [-nodelay] [-nocc]")
		fs.PrintDefaults()
	}

	var delayUS int
	var rateMBps float64
	var noDelay bool
	var noCC bool

	fs.StringVar(&cfg.RemoteAddr, "addr", cfg.RemoteAddr, "receiver address")
	fs.StringVar(&cfg.FilePath, "file", "", "path to file to send (required)")
	fs.IntVar(&delayUS, "delay", -1, "inter-packet delay in microseconds (disables CC)")
	fs.Float64Var(&rateMBps, "rate", 0, "initial send rate in MB/s")
	fs.BoolVar(&noDelay, "nodelay", false, "send as fast as possible (disables CC)")
	fs.BoolVar(&noCC, "nocc", false, "disable congestion control (use fixed rate)")
	fs.Parse(args)

	if cfg.FilePath == "" {
		fs.Usage()
		log.Fatal("flag -file is required")
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

	log.SetFlags(log.Ltime | log.Lmicroseconds)

	s := sender.New(cfg)
	if err := s.Send(); err != nil {
		log.Fatalf("transfer failed: %v", err)
	}
}
