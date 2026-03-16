package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync/atomic"
	"time"
)

func runProxy(args []string) {
	fs := flag.NewFlagSet("proxy", flag.ExitOnError)
	fs.Usage = func() {
		log.Print("usage: hpuft proxy [-listen :9500] [-target host:9000] [-loss pct] [-seed n]")
		fs.PrintDefaults()
	}

	var (
		listenAddr string
		targetAddr string
		lossPct    float64
		seed       int64
	)

	fs.StringVar(&listenAddr, "listen", ":9500", "address to listen on (sender connects here)")
	fs.StringVar(&targetAddr, "target", "127.0.0.1:9000", "address to forward to (receiver)")
	fs.Float64Var(&lossPct, "loss", 0, "packet loss percentage (0-100)")
	fs.Int64Var(&seed, "seed", 0, "random seed (0 = time-based)")
	fs.Parse(args)

	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))

	lAddr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		log.Fatalf("resolve listen: %v", err)
	}
	listenConn, err := net.ListenUDP("udp", lAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	defer listenConn.Close()
	listenConn.SetReadBuffer(16 * 1024 * 1024)

	tAddr, err := net.ResolveUDPAddr("udp", targetAddr)
	if err != nil {
		log.Fatalf("resolve target: %v", err)
	}

	fwdConn, err := net.DialUDP("udp", nil, tAddr)
	if err != nil {
		log.Fatalf("dial target: %v", err)
	}
	defer fwdConn.Close()
	fwdConn.SetWriteBuffer(16 * 1024 * 1024)

	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.Printf("[proxy] %s -> %s | loss=%.1f%% seed=%d", listenAddr, targetAddr, lossPct, seed)

	var totalFwd, totalDrop, totalRetFwd, totalRetDrop int64
	var senderAddr *net.UDPAddr

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			fwd := atomic.LoadInt64(&totalFwd)
			drop := atomic.LoadInt64(&totalDrop)
			rfwd := atomic.LoadInt64(&totalRetFwd)
			rdrop := atomic.LoadInt64(&totalRetDrop)
			total := fwd + drop
			rtotal := rfwd + rdrop
			if total > 0 {
				actualLoss := float64(drop) / float64(total) * 100
				fmt.Fprintf(os.Stderr, "[proxy] sender->recv: fwd=%d drop=%d (%.1f%%) | recv->sender: fwd=%d drop=%d (%.1f%%)\n",
					fwd, drop, actualLoss, rfwd, rdrop, float64(rdrop)/float64(max64(rtotal, 1))*100)
			}
		}
	}()

	buf := make([]byte, 2048)

	go func() {
		retBuf := make([]byte, 2048)
		for {
			n, err := fwdConn.Read(retBuf)
			if err != nil {
				continue
			}
			if senderAddr == nil {
				continue
			}
			if lossPct > 0 && rng.Float64()*100 < lossPct {
				atomic.AddInt64(&totalRetDrop, 1)
				continue
			}
			atomic.AddInt64(&totalRetFwd, 1)
			listenConn.WriteToUDP(retBuf[:n], senderAddr)
		}
	}()

	for {
		n, addr, err := listenConn.ReadFromUDP(buf)
		if err != nil {
			continue
		}
		if senderAddr == nil {
			senderAddr = addr
			log.Printf("[proxy] sender connected from %s", addr)
		}
		if lossPct > 0 && rng.Float64()*100 < lossPct {
			atomic.AddInt64(&totalDrop, 1)
			continue
		}
		atomic.AddInt64(&totalFwd, 1)
		fwdConn.Write(buf[:n])
	}
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
