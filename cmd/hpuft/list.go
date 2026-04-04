package main

import (
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"hpuft/protocol"
	"net"
	"os"
	"time"
)

func runList(args []string) {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	serveAddr := fs.String("addr", "127.0.0.1:9001", "serve daemon address (host:port)")
	fs.Parse(args)

	rAddr, err := net.ResolveUDPAddr("udp", *serveAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[list] resolve addr: %v\n", err)
		os.Exit(1)
	}

	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[list] bind socket: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	var sidBytes [4]byte
	rand.Read(sidBytes[:])
	sessionID := binary.BigEndian.Uint32(sidBytes[:])

	reqPkt := protocol.Packet{
		Header: protocol.Header{
			Type:      protocol.PacketListReq,
			SessionID: sessionID,
		},
	}
	raw, err := protocol.MarshalPacket(&reqPkt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[list] marshal: %v\n", err)
		os.Exit(1)
	}

	buf := make([]byte, protocol.MTUHardCap)
	// Retry up to 3 times with a 2s deadline each.
	for attempt := 0; attempt < 3; attempt++ {
		if _, err := conn.WriteToUDP(raw, rAddr); err != nil {
			fmt.Fprintf(os.Stderr, "[list] send: %v\n", err)
			os.Exit(1)
		}
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			if os.IsTimeout(err) {
				continue
			}
			fmt.Fprintf(os.Stderr, "[list] read: %v\n", err)
			os.Exit(1)
		}
		pkt, err := protocol.UnmarshalPacket(buf[:n])
		if err != nil || pkt.Header.Type != protocol.PacketListResp || pkt.Header.SessionID != sessionID {
			continue
		}
		entries := protocol.UnmarshalListResp(pkt.Payload)
		if len(entries) == 0 {
			fmt.Println("(no files available)")
			return
		}
		for _, e := range entries {
			fmt.Printf("%-40s %s\n", e.Name, humanBytes(int64(e.Size)))
		}
		return
	}

	fmt.Fprintf(os.Stderr, "[list] no response from %s after 3 attempts\n", *serveAddr)
	os.Exit(1)
}
