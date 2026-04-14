# hpuft

High-Performance UDP File Transfer — a loss-driven, FEC-protected UDP protocol designed for maximum throughput on both LAN and long-fat networks.

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

`hpuft` is a single binary with subcommands. All client commands connect to a
`serve` daemon — there is no direct peer-to-peer transfer mode.

### `serve` — persistent daemon (single-lane)
```
hpuft serve [-listen :9001] [-dir .] [-master host:port] [-debug]

  -listen   UDP address to listen on (default: :9001)
  -dir      directory to serve files from and accept uploads into (default: .)
  -master   optional master tracker address for daemon discovery
  -debug    enable CC/protocol debug logging for transfers
```

The daemon accepts connections from any number of clients simultaneously, but
executes **one transfer at a time** — concurrent transfer requests receive
`SERVER_BUSY` and can retry. The daemon stays running between transfers.

All control and data traffic flows through the single `-listen` port. Forward
only this port on your router for cross-NAT use — no second rule needed.

**File management rules (server-enforced):**
1. **Base-name only** — path traversal in filenames is stripped to the final component.
2. **No overwrite** — uploading a file that already exists is rejected with `FILE_EXISTS`.
3. **Atomic promotion** — uploaded files are staged as `.tmp` during transfer and only
   renamed to their final path after xxHash64 verification passes. Failed transfers
   leave no partial file on disk.

### `put` — upload a file to a serve daemon
```
hpuft put -file <path> [-addr host:9001] [-rate MB/s] [-id clientID] [-debug]

  -file     path to the file to upload (required)
  -addr     serve daemon address (default: 127.0.0.1:9001)
  -rate     initial send rate in MB/s (0 = auto-calibrate)
  -id       optional client identifier ≤32 bytes
  -debug    stream raw protocol and CC telemetry to stderr
```

### `get` — download a file from a serve daemon
```
hpuft get -file <name> [-addr host:9001] [-out .] [-id clientID] [-debug]

  -file     name of the file to request (required)
  -addr     serve daemon address (default: 127.0.0.1:9001)
  -out      directory to write the received file (default: .)
  -id       optional client identifier ≤32 bytes
  -debug    stream raw protocol telemetry to stderr
```

### `ls` — list files available on a serve daemon
```
hpuft ls [-addr host:9001] [-id clientID]

  -addr     serve daemon address (default: 127.0.0.1:9001)
  -id       optional client identifier ≤32 bytes
```

### `rm` — delete a file on a serve daemon
```
hpuft rm -file <name> [-addr host:9001] [-id clientID]

  -file     name of the file to delete (required)
  -addr     serve daemon address (default: 127.0.0.1:9001)
  -id       optional client identifier ≤32 bytes
```

### `connect` — interactive shell session with a serve daemon
```
hpuft connect [-addr host:9001] [-id clientID]

  -addr     serve daemon address (default: 127.0.0.1:9001)
  -id       optional client identifier ≤32 bytes
```

Opens a persistent REPL over a single connection. Automatic keepalive pings
keep the connection alive between commands. Shell commands:

```
hpuft> ls
hpuft> get <file> [-o dir]
hpuft> put <path>
hpuft> rm <file>
hpuft> exit
```

### `proxy` — lossy UDP proxy for testing
```
hpuft proxy [-listen :9500] [-target host:9000] [-loss pct] [-seed n]

  -listen   address the sender connects to (default: :9500)
  -target   address to forward to (default: 127.0.0.1:9000)
  -loss     packet loss percentage 0–100 (default: 0)
  -seed     random seed for reproducible loss patterns (default: time-based)
```

### `servers` — query master tracker for active daemons
```
hpuft servers [-master host:port]
```

### `test` — end-to-end integration tests
```
hpuft test [-files f1,f2,...] [-loss 0,1,5,10,15] [-timeout 120]

  -files    comma-separated list of files to transfer (default: auto-detect testdata/)
  -loss     comma-separated loss percentages to run (default: 0,1,5,10,15)
  -timeout  per-transfer timeout in seconds (default: 120)
```

## Typical workflows

### Upload and download via serve daemon
```bash
# Server — run once
hpuft serve -listen :9001 -dir ~/shared

# Client uploads a file
hpuft put -file ./bigfile.iso -addr server-ip:9001

# Client downloads a file
hpuft get -file bigfile.iso -addr server-ip:9001 -out ./downloads
```

### List and manage remote files
```bash
hpuft ls -addr server-ip:9001
hpuft rm -file oldfile.bin -addr server-ip:9001
```

### Interactive session
```bash
hpuft connect -addr server-ip:9001
# Connected to server-ip:9001 (ID=0x1a2b3c4d)
hpuft> ls
hpuft> get bigfile.iso -o ./downloads
hpuft> put ./upload.bin
hpuft> exit
```

### Simulated 5% loss test
```bash
hpuft proxy -loss 5 &
hpuft serve -dir ./testdata &
hpuft get -file myfile.bin -addr 127.0.0.1:9500 -out ./received
```

## Unit tests

```bash
go test ./...
```

## How it works

### Protocol overview (v6)

hpuft v6 uses a **persistent, RPC-style connection model** over a single UDP
socket. Every client-daemon interaction follows the same three-phase flow:

```
1. Handshake    HELLO  ──────────────────► serve
                serve  ◄──────────── WELCOME
                (X25519 key exchange → AES-128-GCM session key derived via HKDF-SHA256)

2. RPC          client ──── REQUEST ─────► serve   (PUT / GET / LIST / DELETE)
                serve  ◄─── RESPONSE ───── client

3. Data         serve / client ─── DATA / PARITY / HEARTBEAT / COMPLETE ───►
                (single-lane; one transfer at a time)
```

**Encryption is always on.** There is no `-encrypt` flag — all traffic is
AES-128-GCM encrypted from the first REQUEST onward. The 32-byte fixed header
is authenticated as AAD (transmitted in cleartext for routing); only the payload
is encrypted.

**Connection lifecycle:**
- `ConnHandshaking` → `ConnIdle` after HELLO/WELCOME
- `ConnIdle` → `ConnTransferring` when a PUT/GET is accepted
- `ConnTransferring` → `ConnIdle` when the transfer completes
- Idle connections are reaped after 30 seconds

**RPC idempotency:** The client retransmits REQUEST packets on timeout (up to 3
retries, 2 s each). The server deduplicates by (connectionID, requestID) so
retransmitted requests are never executed twice.

**Connection pooling:** The server keeps idle connections alive so a `connect`
REPL session can issue multiple commands without re-handshaking.

### Data plane (unchanged from v5)

hpuft sends file data over UDP with a custom reliability layer rather than TCP.

**Sender** blasts packets paced by a token-bucket congestion controller that
probes upward multiplicatively (Phase 1) and then additively (Phase 2) once loss
is detected. Loss is reported by the receiver via NACK lists inside periodic
heartbeats.

**Sliding window** caps in-flight data at 50,000 packets (~68 MB of RAM),
replacing the unbounded `map` used in earlier versions. When `HighestContiguous`
advances in a heartbeat, the tail of the ring buffer is released back to the
pool. If the sender gets ahead of the receiver (window full), it yields and
continues draining NACK retransmits until the window opens — preventing both
memory exhaustion and the deadlock that would occur if NACK processing were
blocked during backpressure.

**FEC** (Reed-Solomon) is applied per block of 100 data packets. The parity
ratio scales automatically with observed loss: 2% at <0.5% loss up to 20% at
>10% loss. Most drops are recovered without a retransmit.

**Heartbeats** carry `NetworkDeliveryRate`, `LossRate`, `HighestContiguous`,
`NACKs`, and an echoed `SenderTimestampNs` for same-clock RTT measurement. RTT
drives the NACK retransmit cooldown — each dropped sequence is retransmitted at
most once per RTT + 25% margin, preventing retransmit storms. The RTT estimate
is guarded against stale echo timestamps: if the sender is idle (honoring
cooldown), the receiver echoes the same frozen timestamp; the sender only updates
its RTT estimate when a strictly newer timestamp arrives.

**Teardown** handles the hard case: if the last packets of the file drop, the
receiver's NACK window is empty (it never saw those sequences). The sender
detects `HighestContiguous < totalChunks-1` with zero NACKs and proactively
injects the missing tail sequences. Retransmits are batched at 10 packets per
2 ms to avoid micro-bursting through OS socket buffers and the serve daemon's
channel.

**TUI dashboard** — `put` and `get` render a live terminal dashboard showing
throughput, RTT, loss rate, CC phase, cumulative NACKs, and a progress bar. When
the main send loop finishes and tail repair begins, the bar switches to
`Repairing...` so the user knows the transfer is still making progress.

### Observed performance

| Scenario | Transfer speed |
|---|---|
| GbE LAN (clean) — 1 GB | ~66 MB/s |
| GbE LAN (clean) — 7 GB | ~87 MB/s (CC reaches ceiling after longer ramp) |
| GbE LAN (AES-128-GCM) — 579 MB | ~49 MB/s |
| GbE LAN (AES-128-GCM) — 1 GB | ~69 MB/s |
| WAN simulation (50 ms RTT, 0.1% loss, `tc netem`) | ~40 MB/s; FEC absorbs drops, CC holds near ceiling |
| FTP/TCP (50 ms RTT, 0.1% loss) | ~1.2 MB/s (AIMD halves window on every drop) |

The 1 GB LAN figure is lower than the 7 GB figure because the congestion
controller spends a larger fraction of the transfer in the initial probe phase.
Longer transfers give the CC more time to find and hold the link ceiling.
