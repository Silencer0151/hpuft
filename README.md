# hpuft

High-Performance UDP File Transfer — a loss-driven, FEC-protected UDP protocol designed for maximum throughput on both LAN and long-fat networks.

## Installation

### Requirements
- [Go](https://go.dev/) 1.21+

### Build

**Windows:**
```bat
build.bat
```

**Linux / macOS:**
```bash
./build.sh
```

Or directly:
```bash
go build -o hpuft ./cmd/hpuft
```

## Usage

`hpuft` is a single binary with subcommands.

### `send` — push a file to a waiting receiver
```bash
hpuft send -file <path> [-addr host:port] [-rate MB/s] [-nodelay] [-nocc]

  -file     path to the file to send (required)
  -addr     receiver address (default: 127.0.0.1:9000)
  -rate     initial send rate in MB/s (congestion control adjusts from here)
  -delay    fixed inter-packet delay in microseconds (disables CC)
  -nodelay  send as fast as possible, no pacing (disables CC)
  -nocc     disable congestion control, use fixed rate
```

### `recv` — listen for an incoming push transfer
```bash
hpuft recv [-listen :9000] [-out ./output]

  -listen   UDP address to listen on (default: :9000)
  -out      directory to write received files (default: .)
```

### `serve` — persistent pull daemon (single-lane)
```bash
hpuft serve [-listen :9001] [-dir .]

  -listen   address to listen on for PULL_REQ packets (default: :9001)
  -dir      directory of files available to serve (default: .)
```

The serve daemon scans `--dir` at startup and builds an allowlist of available
files. It handles one transfer at a time — concurrent requests receive
`SERVER_BUSY` and can retry. The daemon stays running after each transfer.

### `get` — pull a file from a serve daemon (NAT-friendly)
```bash
hpuft get -file <name> [-addr host:9001] [-out .]

  -file   name of the file to request (required)
  -addr   address of the serve daemon (default: 127.0.0.1:9001)
  -out    directory to write the received file (default: .)
```

The `get` command punches a NAT hole by sending a `PULL_REQ` to the serve
daemon. The daemon fires back the `SESSION_REQ` (and the full transfer) through
the open hole — no port forwarding required on the client side.

### `proxy` — lossy UDP proxy for testing
```bash
hpuft proxy [-listen :9500] [-target host:9000] [-loss pct] [-seed n]

  -listen   address the sender connects to (default: :9500)
  -target   address to forward to, i.e. the receiver (default: 127.0.0.1:9000)
  -loss     packet loss percentage 0–100 (default: 0)
  -seed     random seed for reproducible loss patterns (default: time-based)
```

### `test` — end-to-end integration tests
```bash
hpuft test [-files f1,f2,...] [-loss 0,1,5,10,15] [-out dir] [-timeout 120]

  -files    comma-separated list of files to transfer (default: auto-detect testdata/)
  -loss     comma-separated loss percentages to run (default: 0,1,5,10,15)
  -out      output directory for received files (default: temp dir, cleaned up after)
  -timeout  per-transfer timeout in seconds (default: 120)
```

## Typical workflows

### Direct push (both sides reachable / on LAN)
```bash
# Terminal 1 — receiver
hpuft recv -out ./received

# Terminal 2 — sender
hpuft send -file ./myfile.bin
```

### Pull via serve daemon (only the server needs a public IP)
```bash
# Server — run once, serves any file in ~/shared
hpuft serve -listen :9001 -dir ~/shared

# Client pulls a file (no port forwarding needed on the client)
hpuft get -file bigfile.iso -addr server-ip:9001 -out ./downloads
```

### Simulated 5% loss test
```bash
hpuft proxy -loss 5 &
hpuft recv -out ./received &
hpuft send -file ./myfile.bin -addr 127.0.0.1:9500
```

## Unit tests

```bash
go test ./...
```
