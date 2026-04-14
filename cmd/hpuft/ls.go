package main

import (
	"flag"
	"fmt"
	"hpuft/protocol"
	"net"
	"os"
)

func runLs(args []string) {
	fs := flag.NewFlagSet("ls", flag.ExitOnError)
	serveAddr := fs.String("addr", "127.0.0.1:9001", "serve daemon address (host:port)")
	clientID := fs.String("id", "", "optional client identifier (≤32 bytes)")
	fs.Parse(args)

	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ls] bind socket: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	rAddr, err := net.ResolveUDPAddr("udp", *serveAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ls] resolve addr: %v\n", err)
		os.Exit(1)
	}

	pc, err := protocol.DialConnection(conn, rAddr, *clientID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ls] connect: %v\n", err)
		os.Exit(1)
	}
	defer pc.Close()

	respRaw, err := pc.SendRequest(protocol.BuildListRequest())
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ls] request: %v\n", err)
		os.Exit(1)
	}
	status, reason, body, err := protocol.ParseResponse(respRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ls] parse response: %v\n", err)
		os.Exit(1)
	}
	if status != protocol.StatusOK {
		fmt.Fprintf(os.Stderr, "[ls] error: %s\n", reason)
		os.Exit(1)
	}

	entries := protocol.UnmarshalListBody(body)
	if len(entries) == 0 {
		fmt.Println("(no files available)")
		return
	}
	for _, e := range entries {
		fmt.Printf("%-40s %s\n", e.Name, humanBytes(int64(e.Size)))
	}
}
