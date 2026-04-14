// Command hpuft is the HP-UDP fat binary.
//
// Usage:
//
//	hpuft serve    [-listen :9001] [-dir .] [-master host:port]
//	hpuft put      -file <path> [-addr host:9001] [-rate MB/s] [-id clientID]
//	hpuft get      -file <name> [-addr host:9001] [-out .] [-id clientID]
//	hpuft ls       [-addr host:9001] [-id clientID]
//	hpuft rm       -file <name> [-addr host:9001] [-id clientID]
//	hpuft connect  [-addr host:9001] [-id clientID]
//	hpuft servers  [-master host:port]
//	hpuft proxy    [-listen :9500] [-target host:9000] [-loss pct] [-seed n]
//	hpuft test     [-files f1,f2] [-loss 0,1,5,10,15] [-timeout 120]
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
	case "serve":
		runServe(args)
	case "put":
		runPut(args)
	case "get":
		runGet(args)
	case "ls":
		runLs(args)
	case "rm":
		runRm(args)
	case "connect":
		runConnect(args)
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
  serve    Persistent daemon: accept connections and serve files (single-lane)
  put      Upload a file to a serve daemon
  get      Download a file from a serve daemon
  ls       List files available on a serve daemon
  rm       Delete a file on a serve daemon
  connect  Interactive shell session with a serve daemon
  servers  Query master tracker for active daemons
  proxy    Lossy UDP proxy for testing
  test     Run end-to-end integration tests

Run 'hpuft <command> -help' for per-command flags.
`)
}
