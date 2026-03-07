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

// LossyProxy forwards UDP packets between two endpoints while
// randomly dropping packets at a configurable rate.
func main() {
	var (
		listenAddr string
		targetAddr string
		lossPct    float64
		seed       int64
	)

	flag.StringVar(&listenAddr, "listen", ":9500", "address to listen on (sender connects here)")
	flag.StringVar(&targetAddr, "target", "127.0.0.1:9000", "address to forward to (receiver)")
	flag.Float64Var(&lossPct, "loss", 0, "packet loss percentage (0-100)")
	flag.Int64Var(&seed, "seed", 0, "random seed (0 = time-based)")
	flag.Parse()

	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))

	// Listen for packets from sender
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

	// Target (receiver) address
	tAddr, err := net.ResolveUDPAddr("udp", targetAddr)
	if err != nil {
		log.Fatalf("resolve target: %v", err)
	}

	// Forward socket to receiver
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

	// Stats ticker
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

	// Forward: sender -> proxy -> receiver (with loss)
	// Return: receiver -> proxy -> sender (with loss on return path too)
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

			// Apply loss to return path (heartbeats, TRANSFER_COMPLETE, etc.)
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

		// Remember sender address for return path
		if senderAddr == nil {
			senderAddr = addr
			log.Printf("[proxy] sender connected from %s", addr)
		}

		// Apply loss
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
