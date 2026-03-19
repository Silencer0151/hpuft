# hpuft

High-Performance UDP File Transfer — a loss-driven, FEC-protected UDP protocol designed for maximum throughput on both LAN and long-fat networks.

> **Protocol spec:** [UDP_FILE_TRANSFER_SPEC.html](UDP_FILE_TRANSFER_SPEC.html) (v5.0)

## Installation

### Requirements
- [Go](https://go.dev/) 1.25+

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
hpuft send -file <path> [-addr host:port] [-rate MB/s] [-delay us] [-nodelay] [-nocc] [-encrypt] [-debug]

  -file     path to the file to send (required)
  -addr     receiver address (default: 127.0.0.1:9000)
  -rate     initial send rate in MB/s (congestion control adjusts from here)
  -delay    fixed inter-packet delay in microseconds (disables CC)
  -nodelay  send as fast as possible, no pacing (disables CC)
  -nocc     disable congestion control, use fixed rate
  -encrypt  enable AES-128-GCM per-packet encryption
  -debug    stream raw protocol and CC telemetry to stderr
```

### `recv` — listen for an incoming push transfer
```bash
hpuft recv [-listen :9000] [-out ./output] [-encrypt] [-debug]

  -listen   UDP address to listen on (default: :9000)
  -out      directory to write received files (default: .)
  -encrypt  enable AES-128-GCM per-packet encryption
  -debug    stream raw protocol telemetry to stderr
```

### `serve` — persistent bidirectional daemon (single-lane)
```bash
hpuft serve [-listen :9001] [-dir .]

  -listen   port for PULL_REQ, PUSH_REQ, and all transfer data (default: :9001)
  -dir      directory to serve files from and accept pushes into (default: .)
```

The serve daemon scans `-dir` at startup and builds an allowlist of available
files. It handles one transfer at a time — concurrent requests receive
`SERVER_BUSY` and can retry. The daemon stays running after each transfer.
Pushed files are validated and added to the live manifest on success.

**Cross-NAT use**: Forward only `-listen` on your router. All control and data
traffic flows through this single port. The NAT hole punched by the initial
`PULL_REQ` or `PUSH_REQ` covers the entire transfer — no second port-forward rule needed.

### `get` — pull a file from a serve daemon (NAT-friendly)
```bash
hpuft get -file <name> [-addr host:9001] [-out .] [-encrypt] [-debug]

  -file     name of the file to request (required)
  -addr     address of the serve daemon (default: 127.0.0.1:9001)
  -out      directory to write the received file (default: .)
  -encrypt  enable AES-128-GCM per-packet encryption
  -debug    stream raw protocol telemetry to stderr
```

The `get` command punches a NAT hole by sending a `PULL_REQ` to the serve
daemon. The daemon fires back the `SESSION_REQ` (and the full transfer) through
the open hole — no port forwarding required on the client side.

### `push` — push a file to a serve daemon
```bash
hpuft push -file <path> [-addr host:9001] [-encrypt] [-debug]

  -file     path to the file to push (required)
  -addr     address of the serve daemon (default: 127.0.0.1:9001)
  -encrypt  enable AES-128-GCM per-packet encryption
  -debug    stream raw protocol telemetry to stderr
```

The `push` command deposits a file into the serve daemon's directory.
Three security rules are always enforced server-side:
1. **Base-name only** — path traversal in the filename is stripped to the final component.
2. **No overwrite** — if the file already exists the push is rejected with `FILE_EXISTS`.
3. **Post-hash promotion** — the file is staged as `.tmp` during transfer and only
   renamed to its final path after xxHash64 verification passes. Failed transfers
   leave no partial file on disk.

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

### Bidirectional hub (serve + push + get, cross-NAT)
```bash
# Server — forward only port 9001 on your router
hpuft serve -listen :9001 -dir ~/shared

# Client A pushes a file
hpuft push -file ./upload.bin -addr server-ip:9001

# Client B pulls a file (no port-forwarding needed on client side)
hpuft get -file upload.bin -addr server-ip:9001 -out ./downloads
```

### Encrypted push/pull
```bash
# Both sides must pass -encrypt — unencrypted clients are rejected mid-transfer
hpuft serve -listen :9001 -dir ~/shared

hpuft push -file ./secret.bin -addr server-ip:9001 -encrypt
hpuft get -file secret.bin -addr server-ip:9001 -encrypt -out ./downloads
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

## How it works (summary)

hpuft sends data over UDP with a custom reliability layer rather than TCP.

**Sender** blasts packets paced by a token-bucket congestion controller that probes upward multiplicatively (Phase 1) and then additively (Phase 2) once loss is detected. Loss is reported by the receiver via NACK lists inside periodic heartbeats.

**Sliding window** caps in-flight data at 50,000 packets (~68 MB of RAM), replacing the unbounded `map` used in earlier versions. When `HighestContiguous` advances in a heartbeat, the tail of the ring buffer is released back to the pool. If the sender gets ahead of the receiver (window full), it yields and continues draining NACK retransmits until the window opens — preventing both memory exhaustion and the deadlock that would occur if NACK processing were blocked during backpressure.

**FEC** (Reed-Solomon) is applied per block of 100 data packets. The parity ratio scales automatically with observed loss: 2% at <0.5% loss up to 20% at >10% loss. Most drops are recovered without a retransmit.

**Heartbeats** carry `NetworkDeliveryRate`, `LossRate`, `HighestContiguous`, `NACKs`, and an echoed `SenderTimestampNs` for same-clock RTT measurement. RTT drives the NACK retransmit cooldown — each dropped sequence is retransmitted at most once per RTT + 25% margin, preventing retransmit storms. The RTT estimate is guarded against stale echo timestamps: if the sender is idle (honoring cooldown), the receiver echoes the same frozen timestamp; the sender only updates its RTT estimate when a strictly newer timestamp arrives.

**Teardown** handles the hard case: if the last packets of the file drop, the receiver's NACK window is empty (it never saw those sequences). The sender detects `HighestContiguous < totalChunks-1` with zero NACKs and proactively injects the missing tail sequences. Retransmits are batched at 10 packets per 2 ms to avoid micro-bursting through OS socket buffers and the serve daemon's channel.

**Encryption** — all four transfer commands accept `-encrypt` to enable AES-128-GCM per-packet encryption (spec §4.5). Both sides generate a fresh X25519 ephemeral keypair per session; the shared secret is derived via HKDF-SHA256 into a 16-byte AES-128 key. For `push`/`get` the key exchange piggybacks on the existing `PUSH_REQ`/`PUSH_ACCEPT` and `PULL_REQ`/`SESSION_REQ` round trips — zero added latency. For direct `send`/`recv` a 1-RTT `SESSION_ACCEPT` message carries the receiver's public key. The 32-byte header is authenticated as AAD but transmitted in cleartext (the receiver needs it for routing); only the payload is encrypted. Private keys are ephemeral and exist only in memory for the duration of the session — perfect forward secrecy with no key management.

**TUI dashboard** — `push` and `get` render a live terminal dashboard (Charmbracelet Bubble Tea) instead of a scrolling log. The dashboard shows throughput, RTT, loss rate, CC phase, cumulative NACKs, and a progress bar. When the main send loop finishes and tail repair begins, the bar switches to `Repairing...` so the user knows the transfer is still making progress rather than stalled.

### Observed performance

| Scenario | Transfer speed |
|---|---|
| GbE LAN (clean, unencrypted) — 1 GB | ~66 MB/s |
| GbE LAN (clean, unencrypted) — 7 GB | ~87 MB/s (CC reaches ceiling after longer ramp) |
| GbE LAN (AES-128-GCM encrypted) — 579 MB | ~49 MB/s |
| GbE LAN (AES-128-GCM encrypted) — 1 GB push | ~69 MB/s |
| WAN simulation (50 ms RTT, 0.1% loss, `tc netem`) | transfer completes reliably; FEC absorbs drops, CC holds near ceiling |
| FTP/TCP (50 ms RTT, 0.1% loss) | ~1.2 MB/s (AIMD halves window on every drop) |

The 1 GB LAN figure is lower than the 7 GB figure because the congestion controller spends a larger fraction of the transfer in the initial probe phase. Longer transfers give the CC more time to find and hold the link ceiling.
