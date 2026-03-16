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

### Send a file
```bash
hpuft send -file <path> [-addr host:port] [-rate MB/s] [-nodelay] [-nocc]

  -file     path to the file to send (required)
  -addr     receiver address (default: 127.0.0.1:9000)
  -rate     initial send rate in MB/s (congestion control adjusts from here)
  -delay    fixed inter-packet delay in microseconds (disables CC)
  -nodelay  send as fast as possible, no pacing (disables CC)
  -nocc     disable congestion control, use fixed rate
```

### Receive a file
```bash
hpuft recv [-listen :9000] [-out ./output]

  -listen   UDP address to listen on (default: :9000)
  -out      directory to write received files (default: .)
```

### Lossy proxy (for testing)
```bash
hpuft proxy [-listen :9500] [-target host:9000] [-loss pct] [-seed n]

  -listen   address the sender connects to (default: :9500)
  -target   address to forward packets to, i.e. the receiver (default: 127.0.0.1:9000)
  -loss     packet loss percentage 0–100 (default: 0)
  -seed     random seed for reproducible loss patterns (default: time-based)
```

### Integration tests
```bash
hpuft test [-files f1,f2,...] [-loss 0,1,5,10,15] [-out dir] [-timeout 120]

  -files    comma-separated list of files to transfer (default: auto-detect testdata/)
  -loss     comma-separated loss percentages to run (default: 0,1,5,10,15)
  -out      output directory for received files (default: temp dir, cleaned up after)
  -timeout  per-transfer timeout in seconds (default: 120)
```

## Quick start (local loopback)

```bash
# Terminal 1 — start receiver
hpuft recv -out ./received

# Terminal 2 — send a file
hpuft send -file ./myfile.bin

# With simulated 5% packet loss via proxy:
hpuft proxy -loss 5 &
hpuft recv -out ./received &
hpuft send -file ./myfile.bin -addr 127.0.0.1:9500
```

## Unit tests

```bash
go test ./...
```
