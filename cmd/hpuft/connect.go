package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hpuft/protocol"
	"hpuft/receiver"
	"hpuft/sender"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// runConnect opens a persistent connection to a serve daemon and starts an
// interactive REPL. A single background goroutine owns the socket read loop
// and dispatches packets so KeepAlive pings and shell commands share the
// socket safely.
func runConnect(args []string) {
	fs := flag.NewFlagSet("connect", flag.ExitOnError)
	serveAddr := fs.String("addr", "127.0.0.1:9001", "serve daemon address (host:port)")
	clientID := fs.String("id", "", "optional client identifier (≤32 bytes)")
	fs.Parse(args)

	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[connect] bind socket: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	conn.SetReadBuffer(16 * 1024 * 1024)
	conn.SetWriteBuffer(16 * 1024 * 1024)

	rAddr, err := net.ResolveUDPAddr("udp", *serveAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[connect] resolve addr: %v\n", err)
		os.Exit(1)
	}

	pc, err := protocol.DialConnection(conn, rAddr, *clientID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[connect] handshake: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "Connected to %s (ID=0x%08x)\n", *serveAddr, pc.ID)
	fmt.Fprintln(os.Stdout, "Type 'help' for available commands.")

	// Single socket reader: routes incoming packets to the active consumer.
	d := newReplDispatch(conn, pc)
	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		d.run()
	}()

	// Send PING every 10s so the server's idle timer resets.
	kaStop := make(chan struct{})
	go pc.KeepAlive(10*time.Second, kaStop)

	sc := bufio.NewScanner(os.Stdin)
	fmt.Fprint(os.Stdout, "hpuft> ")
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 {
			fmt.Fprint(os.Stdout, "hpuft> ")
			continue
		}
		switch fields[0] {
		case "ls":
			shellLs(pc, d)
		case "get":
			shellGet(pc, d, conn, rAddr, fields[1:])
		case "put":
			shellPut(pc, d, conn, rAddr, fields[1:])
		case "rm":
			shellRm(pc, d, fields[1:])
		case "help":
			fmt.Println("commands: ls | get <file> [-o dir] | put <path> | rm <file> | exit")
		case "exit", "quit":
			goto done
		default:
			fmt.Fprintf(os.Stderr, "unknown command %q — type 'help'\n", fields[0])
		}
		fmt.Fprint(os.Stdout, "hpuft> ")
	}
done:
	close(kaStop)
	d.stop()
	<-dispatchDone
	pc.Close()
}

// ── dispatcher ────────────────────────────────────────────────────────────────

// replDispatch is the single socket-reader for the REPL. It owns all reads
// from conn and routes packets:
//
//   - PONG          → discard (keepalive reply)
//   - RESPONSE      → rpcCh  (current pending shell command)
//   - data-plane    → transCh (active transfer goroutine)
type replDispatch struct {
	conn   *net.UDPConn
	pc     *protocol.Connection
	stopCh chan struct{}

	mu       sync.Mutex
	transCh  chan<- protocol.RawPacket // set while a transfer is running
	rpcCh    chan<- []byte             // set while a shell RPC is in flight
}

func newReplDispatch(conn *net.UDPConn, pc *protocol.Connection) *replDispatch {
	return &replDispatch{
		conn:   conn,
		pc:     pc,
		stopCh: make(chan struct{}),
	}
}

func (d *replDispatch) stop() {
	select {
	case <-d.stopCh:
	default:
		close(d.stopCh)
	}
}

func (d *replDispatch) setTransferChan(ch chan<- protocol.RawPacket) {
	d.mu.Lock()
	d.transCh = ch
	d.mu.Unlock()
}

func (d *replDispatch) setRPCChan(ch chan<- []byte) {
	d.mu.Lock()
	d.rpcCh = ch
	d.mu.Unlock()
}

func (d *replDispatch) run() {
	buf := make([]byte, protocol.MTUHardCap)
	for {
		select {
		case <-d.stopCh:
			return
		default:
		}

		d.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, from, err := d.conn.ReadFromUDP(buf)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue // normal deadline tick; check stopCh and loop
			}
			return // fatal read error
		}

		raw := make([]byte, n)
		copy(raw, buf[:n])

		hdr, err := protocol.UnmarshalHeader(raw)
		if err != nil || hdr.ConnectionID != d.pc.ID {
			continue
		}

		d.mu.Lock()
		transCh := d.transCh
		rpcCh := d.rpcCh
		d.mu.Unlock()

		switch hdr.Type {
		case protocol.PacketPong:
			// discard — keepalive reply, nothing to do

		case protocol.PacketResponse:
			if rpcCh != nil {
				select {
				case rpcCh <- raw:
				default: // channel full; caller will retry on timeout
				}
			}

		case protocol.PacketData, protocol.PacketParity,
			protocol.PacketHeartbeat, protocol.PacketComplete,
			protocol.PacketAckClose:
			if transCh != nil {
				// Forward source addr alongside the bytes so the receiver can
				// lock its heartbeat destination onto the C sender's data-plane
				// ephemeral port (the C daemon socket drops HEARTBEATs).
				select {
				case transCh <- protocol.RawPacket{Data: raw, Src: from}:
				default: // drop; sender/receiver will retransmit
				}
			}
		}
	}
}

// sendRPC is the dispatcher-aware RPC: it sets the rpcCh BEFORE sending the
// REQUEST so no RESPONSE can be lost to a race, then waits up to
// RequestRetryTimeout per attempt.
func (d *replDispatch) sendRPC(plaintext []byte) ([]byte, error) {
	reqID := d.pc.NextRequestID()
	raw, err := d.pc.EncryptRequest(reqID, plaintext)
	if err != nil {
		return nil, err
	}

	ch := make(chan []byte, 4) // buffer: dispatcher must not block
	d.setRPCChan((chan<- []byte)(ch))
	defer d.setRPCChan(nil)

	for attempt := 0; attempt < protocol.RequestMaxRetries; attempt++ {
		if _, err := d.conn.WriteToUDP(raw, d.pc.RemoteAddr); err != nil {
			return nil, fmt.Errorf("send request: %w", err)
		}
		select {
		case resp := <-ch:
			hdr, herr := protocol.UnmarshalHeader(resp)
			if herr != nil || hdr.SequenceNum != reqID {
				// Stale response (e.g. delayed retry from a prior call); wait again.
				attempt--
				if attempt < -10 { // safety valve against infinite stale loop
					break
				}
				continue
			}
			return d.pc.DecryptResponse(resp)
		case <-time.After(protocol.RequestRetryTimeout):
			// timeout — retransmit
		}
	}
	return nil, errors.New("request timeout: no RESPONSE after retries")
}

// ── shell helpers ─────────────────────────────────────────────────────────────

func shellLs(pc *protocol.Connection, d *replDispatch) {
	respRaw, err := d.sendRPC(protocol.BuildListRequest())
	if err != nil {
		fmt.Fprintf(os.Stderr, "ls: %v\n", err)
		return
	}
	status, reason, body, err := protocol.ParseResponse(respRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ls: parse response: %v\n", err)
		return
	}
	if status != protocol.StatusOK {
		fmt.Fprintf(os.Stderr, "ls: %s\n", reason)
		return
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

func shellRm(pc *protocol.Connection, d *replDispatch, args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: rm <file>")
		return
	}
	respRaw, err := d.sendRPC(protocol.BuildDeleteRequest(args[0]))
	if err != nil {
		fmt.Fprintf(os.Stderr, "rm: %v\n", err)
		return
	}
	status, reason, _, err := protocol.ParseResponse(respRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "rm: parse response: %v\n", err)
		return
	}
	if status != protocol.StatusOK {
		fmt.Fprintf(os.Stderr, "rm: %s\n", reason)
		return
	}
	fmt.Fprintf(os.Stdout, "deleted %q\n", args[0])
}

func shellGet(pc *protocol.Connection, d *replDispatch, conn *net.UDPConn, rAddr *net.UDPAddr, args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: get <file> [-o dir]")
		return
	}
	fileName := args[0]
	outDir := "."
	for i := 1; i < len(args); i++ {
		if args[i] == "-o" && i+1 < len(args) {
			outDir = args[i+1]
			i++
		}
	}

	respRaw, err := d.sendRPC(protocol.BuildGetRequest(fileName))
	if err != nil {
		fmt.Fprintf(os.Stderr, "get: %v\n", err)
		return
	}
	status, reason, body, err := protocol.ParseResponse(respRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get: parse response: %v\n", err)
		return
	}
	if status != protocol.StatusOK {
		fmt.Fprintf(os.Stderr, "get: %s\n", reason)
		return
	}
	if len(body) < 20 {
		fmt.Fprintf(os.Stderr, "get: response body too short (%d bytes)\n", len(body))
		return
	}
	fileSize := binary.BigEndian.Uint64(body[0:8])
	fileHash := binary.BigEndian.Uint64(body[8:16])
	initialRate := binary.BigEndian.Uint32(body[16:20])

	fmt.Fprintf(os.Stdout, "get: receiving %s (%s)...\n", fileName, humanBytes(int64(fileSize)))

	transCh := make(chan protocol.RawPacket, 4096)
	d.setTransferChan(transCh)
	pc.SetState(protocol.ConnTransferring)

	cfg := receiver.DefaultConfig()
	cfg.OutputDir = outDir
	cfg.Conn = conn
	cfg.RecvChan = transCh
	cfg.SenderAddr = pc.RemoteAddr
	cfg.EncKey = &pc.SessionKey
	cfg.IVBase = &pc.IVBase
	cfg.IncomingSession = &receiver.IncomingSession{
		SenderAddr:   pc.RemoteAddr,
		ConnectionID: pc.ID,
		Meta: receiver.TransferMeta{
			FileName:    fileName,
			FileSize:    fileSize,
			FileHash:    fileHash,
			InitialRate: initialRate,
		},
	}

	r, err := receiver.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get: init receiver: %v\n", err)
		d.setTransferChan(nil)
		pc.SetState(protocol.ConnIdle)
		return
	}

	start := time.Now()
	errCh := make(chan error, 1)
	go func() { errCh <- r.Run() }()
	if err := RunRecvProgress(r, errCh); err != nil {
		fmt.Fprintf(os.Stderr, "get: FAILED: %v\n", err)
	} else {
		elapsed := time.Since(start)
		mbps := float64(fileSize) / elapsed.Seconds() / 1e6
		fmt.Fprintf(os.Stdout, "get: COMPLETE in %s (%.1f MB/s) | FEC rebuilt: %d\n",
			elapsed.Round(time.Millisecond), mbps, r.Progress().Rebuilt)
	}

	d.setTransferChan(nil)
	pc.SetState(protocol.ConnIdle)
}

func shellPut(pc *protocol.Connection, d *replDispatch, conn *net.UDPConn, rAddr *net.UDPAddr, args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: put <path>")
		return
	}
	filePath := args[0]

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "put: stat: %v\n", err)
		return
	}
	fileHash, err := receiver.HashFile(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "put: hash: %v\n", err)
		return
	}
	fileName := filepath.Base(filePath)

	respRaw, err := d.sendRPC(protocol.BuildPutRequest(uint64(fileInfo.Size()), fileHash, 0, fileName))
	if err != nil {
		fmt.Fprintf(os.Stderr, "put: %v\n", err)
		return
	}
	status, reason, _, err := protocol.ParseResponse(respRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "put: parse response: %v\n", err)
		return
	}
	if status != protocol.StatusOK {
		fmt.Fprintf(os.Stderr, "put: %s\n", reason)
		return
	}

	transCh := make(chan protocol.RawPacket, 4096)
	d.setTransferChan(transCh)
	pc.SetState(protocol.ConnTransferring)

	sCfg := sender.DefaultConfig()
	sCfg.FilePath = filePath
	sCfg.RemoteAddr = rAddr.String()
	sCfg.ConnectionID = pc.ID
	sCfg.MuxConn = conn
	sCfg.MuxAddr = rAddr
	sCfg.RecvChan = transCh
	sCfg.EncKey = &pc.SessionKey
	sCfg.IVBase = &pc.IVBase

	s := sender.New(sCfg)
	start := time.Now()
	errCh := make(chan error, 1)
	go func() { errCh <- s.Send() }()
	if err := RunSendProgress(s, errCh); err != nil {
		fmt.Fprintf(os.Stderr, "put: FAILED: %v\n", err)
	} else {
		elapsed := time.Since(start)
		mbps := float64(fileInfo.Size()) / elapsed.Seconds() / 1e6
		fmt.Fprintf(os.Stdout, "put: COMPLETE in %s (%.1f MB/s)\n",
			elapsed.Round(time.Millisecond), mbps)
	}

	d.setTransferChan(nil)
	pc.SetState(protocol.ConnIdle)
}
