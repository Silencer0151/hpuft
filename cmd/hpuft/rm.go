package main

import (
	"flag"
	"fmt"
	"hpuft/protocol"
	"net"
	"os"
)

func runRm(args []string) {
	fs := flag.NewFlagSet("rm", flag.ExitOnError)
	serveAddr := fs.String("addr", "127.0.0.1:9001", "serve daemon address (host:port)")
	fileName := fs.String("file", "", "name of the file to delete (required)")
	clientID := fs.String("id", "", "optional client identifier (≤32 bytes)")
	fs.Parse(args)

	if *fileName == "" {
		fmt.Fprintln(os.Stderr, "usage: hpuft rm -file <name> [-addr host:port] [-id clientID]")
		os.Exit(1)
	}

	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[rm] bind socket: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	rAddr, err := net.ResolveUDPAddr("udp", *serveAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[rm] resolve addr: %v\n", err)
		os.Exit(1)
	}

	pc, err := protocol.DialConnection(conn, rAddr, *clientID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[rm] connect: %v\n", err)
		os.Exit(1)
	}
	defer pc.Close()

	respRaw, err := pc.SendRequest(protocol.BuildDeleteRequest(*fileName))
	if err != nil {
		fmt.Fprintf(os.Stderr, "[rm] request: %v\n", err)
		os.Exit(1)
	}
	status, reason, _, err := protocol.ParseResponse(respRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[rm] parse response: %v\n", err)
		os.Exit(1)
	}
	if status != protocol.StatusOK {
		fmt.Fprintf(os.Stderr, "[rm] error: %s\n", reason)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "[rm] Deleted %q from %s\n", *fileName, *serveAddr)
}
