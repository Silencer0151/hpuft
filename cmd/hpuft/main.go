// Command hpuft is the HP-UDP fat binary.
//
// Usage:
//
//	hpuft send   -file <path> [-addr host:port] [-rate MB/s] [-nodelay] [-nocc]
//	hpuft recv   [-listen :9000] [-out ./output]
//	hpuft serve  [-listen :9001] [-data :9002] [-dir .]
//	hpuft get    -file <name> [-addr host:9001] [-out .]
//	hpuft push   -file <path> [-addr host:9001]
//	hpuft list   [-addr host:9001]
//	hpuft proxy  [-listen :9500] [-target host:9000] [-loss pct] [-seed n]
//	hpuft test   [-files f1,f2] [-loss 0,1,5,10,15] [-timeout 120]
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	cmd, args := os.Args[1], os.Args[2:]

	switch cmd {
	case "send":
		runSend(args)
	case "recv":
		runRecv(args)
	case "serve":
		runServe(args)
	case "get":
		runGet(args)
	case "push":
		runPush(args)
	case "list":
		runList(args)
	case "servers":
		runServers(args)
	case "proxy":
		runProxy(args)
	case "test":
		runTest(args)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: hpuft <command> [flags]

Commands:
  send   Push a file to a waiting receiver
  recv   Listen for an incoming push transfer
  serve  Persistent daemon: serve files on request (single-lane)
  get    Pull a file from a serve daemon (NAT-traversal friendly)
  push   Push a file to a serve daemon (bidirectional hub)
  list     List files available on a serve daemon
  servers  Query master tracker for active daemons
  proxy  Lossy UDP proxy for testing
  test   Run end-to-end integration tests

Run 'hpuft <command> -help' for per-command flags.
`)
}
