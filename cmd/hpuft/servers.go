package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
)

func runServers(args []string) {
	fs := flag.NewFlagSet("servers", flag.ExitOnError)
	masterAddr := fs.String("master", "", "master tracker address (host:port) — required")
	fs.Parse(args)

	if *masterAddr == "" {
		fmt.Fprintln(os.Stderr, "usage: hpuft servers -master <host:port>")
		os.Exit(1)
	}

	conn, err := net.Dial("tcp", *masterAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[servers] connect %s: %v\n", *masterAddr, err)
		os.Exit(1)
	}
	defer conn.Close()

	// Send 0x04 Query frame: [uint32(1)][0x04]
	// Length counts the type byte, matching the same convention as burst frames.
	var query [5]byte
	binary.BigEndian.PutUint32(query[0:4], 1)
	query[4] = 0x04
	if _, err := conn.Write(query[:]); err != nil {
		fmt.Fprintf(os.Stderr, "[servers] send query: %v\n", err)
		os.Exit(1)
	}

	// Read length-prefixed response: [uint32 payloadLen][payload]
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		fmt.Fprintf(os.Stderr, "[servers] read response length: %v\n", err)
		os.Exit(1)
	}
	payloadLen := binary.BigEndian.Uint32(lenBuf[:])

	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(conn, payload); err != nil {
			fmt.Fprintf(os.Stderr, "[servers] read response payload: %v\n", err)
			os.Exit(1)
		}
	}

	daemons, err := parseQueryResponse(payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[servers] parse response: %v\n", err)
		os.Exit(1)
	}

	printDaemons(daemons)
}

type daemonInfo struct {
	ip    string
	port  uint16
	files []fileEntry
}

type fileEntry struct {
	hash [32]byte
	size uint64
	name string
}

func parseQueryResponse(data []byte) ([]daemonInfo, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("response too short for daemon count")
	}
	count := binary.BigEndian.Uint32(data[0:4])
	off := 4

	daemons := make([]daemonInfo, 0, count)
	for i := uint32(0); i < count; i++ {
		// 2-byte IP length + IP bytes
		if off+2 > len(data) {
			return nil, fmt.Errorf("truncated at daemon %d IP length", i)
		}
		ipLen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		if off+ipLen > len(data) {
			return nil, fmt.Errorf("truncated at daemon %d IP string", i)
		}
		ip := string(data[off : off+ipLen])
		off += ipLen

		// 2-byte UDP port
		if off+2 > len(data) {
			return nil, fmt.Errorf("truncated at daemon %d port", i)
		}
		port := binary.BigEndian.Uint16(data[off:])
		off += 2

		// 4-byte file count
		if off+4 > len(data) {
			return nil, fmt.Errorf("truncated at daemon %d file count", i)
		}
		fileCount := binary.BigEndian.Uint32(data[off:])
		off += 4

		files := make([]fileEntry, 0, fileCount)
		for j := uint32(0); j < fileCount; j++ {
			// 32-byte SHA-256 hash
			if off+32 > len(data) {
				return nil, fmt.Errorf("truncated at daemon %d file %d hash", i, j)
			}
			var fe fileEntry
			copy(fe.hash[:], data[off:off+32])
			off += 32

			// 8-byte size
			if off+8 > len(data) {
				return nil, fmt.Errorf("truncated at daemon %d file %d size", i, j)
			}
			fe.size = binary.BigEndian.Uint64(data[off:])
			off += 8

			// 2-byte name length + name bytes
			if off+2 > len(data) {
				return nil, fmt.Errorf("truncated at daemon %d file %d name length", i, j)
			}
			nameLen := int(binary.BigEndian.Uint16(data[off:]))
			off += 2
			if off+nameLen > len(data) {
				return nil, fmt.Errorf("truncated at daemon %d file %d name", i, j)
			}
			fe.name = string(data[off : off+nameLen])
			off += nameLen

			files = append(files, fe)
		}

		daemons = append(daemons, daemonInfo{ip: ip, port: port, files: files})
	}

	return daemons, nil
}

func printDaemons(daemons []daemonInfo) {
	if len(daemons) == 0 {
		fmt.Println("(no daemons registered)")
		return
	}

	for i, d := range daemons {
		addr := fmt.Sprintf("%s:%d", d.ip, d.port)
		fileWord := "file"
		if len(d.files) != 1 {
			fileWord = "files"
		}
		fmt.Printf("Daemon %d  %s  (%d %s)\n", i+1, addr, len(d.files), fileWord)
		fmt.Println("  ┌────────────────────────────────────────────────────────────────────────────")
		for _, f := range d.files {
			fmt.Printf("  │  %-38s  %s\n", f.name, humanBytes(int64(f.size)))
			fmt.Printf("  │  sha256: %x\n", f.hash)
		}
		if len(d.files) == 0 {
			fmt.Println("  │  (no files)")
		}
		fmt.Println("  └────────────────────────────────────────────────────────────────────────────")
		if i < len(daemons)-1 {
			fmt.Println()
		}
	}
}
