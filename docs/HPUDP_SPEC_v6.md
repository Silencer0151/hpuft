# Design Specification: High-Performance UDP File Transfer Protocol (HP-UDP) v6.0

## 1. The Motivation (The "Why")

The development of HP-UDP is driven by a singular goal: to engineer the fastest possible file transfer protocol that mathematically guarantees perfect data integrity across volatile network conditions.

While TCP is the foundational workhorse of the internet, its general-purpose congestion algorithms inherently throttle performance on Long Fat Networks (LFNs). HP-UDP was built to prove that it is possible to outperform TCP in raw throughput by replacing reactive safety nets with proactive, domain-specific algorithms.

This protocol democratizes high-speed data movement, giving developers and engineers the ability to send and receive massive files cleanly, reliably, and at maximum hardware limits. It is a rigorous demonstration of advanced systems engineering, built to prove what is possible when legacy constraints are stripped away.

## 2. Architectural Overview

HP-UDP is an application-layer file transfer mechanism built on top of UDP. The design is lean, avoids unnecessary overhead, and focuses intently on its primary goal of speed while ensuring the reliability required for production use.

### 2.1 Core Pillars

- **Encrypted-by-Default Connections:** A 1-RTT HELLO/WELCOME handshake establishes an encrypted session using ephemeral X25519 key exchange and AES-128-GCM. All connections are encrypted; there is no unencrypted mode.

- **Connection-Oriented Request Framing:** A persistent connection supports sequential request/response operations (GET, PUT, LIST, DELETE, PING) over a single UDP socket. One data transfer at a time per connection; control messages are interleaved freely.

- **Ultra-Fast Data Transfer:** Adaptive Forward Error Correction (FEC), loss-driven congestion control with deficit-accumulator pacing, and rate-proportional Heartbeat feedback — unchanged from v5.2.

- **Guaranteed Integrity:** Graceful teardown linked to pipelined, full-file xxHash64 verification.

- **Resilient Session Management:** Timeout-driven cleanup on both sides with linger states.

### 2.2 Architecture Layers

```
┌──────────────────────────────────────────────────────────┐
│  CLI Layer                                               │
│  One-shot (get/put/ls/rm) or Shell mode (connect)        │
├──────────────────────────────────────────────────────────┤
│  Connection Layer                                        │
│  HELLO / WELCOME / REJECT — 1-RTT encrypted handshake   │
│  PING / PONG — keepalive                                 │
├──────────────────────────────────────────────────────────┤
│  Request Layer                                           │
│  REQUEST / RESPONSE — GET, PUT, LIST, DELETE operations  │
│  Sequential request-level multiplexing (one at a time)   │
├──────────────────────────────────────────────────────────┤
│  Data Layer (HOT PATH — unchanged from v5.x)             │
│  DATA / PARITY / HEARTBEAT / COMPLETE / ACK_CLOSE        │
│  FEC, congestion control, pacing, sliding window         │
├──────────────────────────────────────────────────────────┤
│  UDP Socket                                              │
└──────────────────────────────────────────────────────────┘
```

**Note on Authentication:** HP-UDP provides a lightweight identity field (`ClientID`) in the HELLO payload for logging and optional access control. It does not perform cryptographic authentication — it does not verify that the remote endpoint is who it claims to be. In the target deployment environment (managed networks with known infrastructure behind SDNs), endpoint identity is established at the network layer. Optional certificate or pre-shared-key authentication may be added in a future revision as a separate concern.

## 3. Packet Wire Format (Custom Header)

The protocol utilizes a tightly packed, fixed-width **32-byte** binary header for every datagram. The header is naturally aligned for 64-bit systems (four 8-byte words). The hard MTU cap is **1400 bytes total** (header + payload), yielding a maximum payload of **1368 bytes** (`MTUHardCap(1400) − HeaderSize(32)`). This ensures safe passage within standard 1500-byte ethernet MTUs without IP-level fragmentation.

**Byte order:** **All multi-byte fields in the entire protocol** — header fields, heartbeat payload fields (§7B), request/response payload fields, and NACK arrays — are in **big-endian (network byte order)**. C implementations must use `htonl`/`ntohl` (32-bit) and `htonll`/`ntohll` (64-bit) or equivalent for every multi-byte field on the wire. Go implementations use `binary.BigEndian` methods. This applies uniformly; there are no little-endian fields anywhere in the protocol.

| Byte Offset | Size | Field Name | Description |
|---|---|---|---|
| 0x00 | 1 Byte | PacketType | See §3.1 Packet Types table. |
| 0x01 | 4 Bytes | ConnectionID | Client-generated random identifier for the connection (see §4 for collision handling). |
| 0x05 | 8 Bytes | SequenceNum | Strictly incrementing 64-bit chunk identifier for DATA packets. Used as request/response ID for control packets. Eliminates rollover concerns up to ~16 EB file sizes. |
| 0x0D | 8 Bytes | BlockGroup | 64-bit identifier for the FEC block this packet belongs to. Aligned with SequenceNum address space. Zero for non-FEC packets. |
| 0x15 | 2 Bytes | PayloadLen | Size of the raw data payload (max 1368 bytes unencrypted, 1352 encrypted). |
| 0x17 | 1 Byte | Flags | Bitmask: 0x01 = End of File, 0x02 = Calibration Burst. |
| 0x18 | 8 Bytes | SenderTimestampNs | The sender's monotonic clock timestamp in nanoseconds at the moment each DATA or PARITY packet is built. Non-data control packets leave this field zero. The receiver echoes this value verbatim as EchoTimestampNs in the Heartbeat payload; the sender computes RTT = now_ns − EchoTimestampNs using only its own clock (§7B). |
| 0x20 | Variable | Payload | Raw file bytes, FEC parity data, or protocol metadata. |

### 3.1 Packet Types

| Type | Name | Direction | Layer | Description |
|------|------|-----------|-------|-------------|
| 0x00 | HELLO | C→S | Connection | Connection init + client X25519 pubkey + ClientID |
| 0x01 | WELCOME | S→C | Connection | Connection accept + server X25519 pubkey |
| 0x02 | REQUEST | C→S | Request | Operation request (GET/PUT/LIST/DELETE) — encrypted payload |
| 0x03 | RESPONSE | S→C | Request | Operation result / metadata — encrypted payload |
| 0x04 | REJECT | S→C | Connection/Request | Connection or request refused with reason code |
| 0x05 | DATA | bidir | Data | File payload (encrypted) |
| 0x06 | PARITY | bidir | Data | FEC parity shard (encrypted) |
| 0x07 | HEARTBEAT | recv→sender | Data | Congestion control feedback + NACK array |
| 0x08 | COMPLETE | sender→recv | Data | Transfer complete, begin hash verification |
| 0x09 | ACK_CLOSE | recv→sender | Data | Hash verified, transfer confirmed |
| 0x0A | PING | bidir | Connection | Keepalive probe |
| 0x0B | PONG | bidir | Connection | Keepalive response |

**Header field reuse across layers:**

- **Connection layer** (HELLO, WELCOME, REJECT, PING, PONG): `ConnectionID` is set; `SequenceNum` is zero; `BlockGroup` is zero.
- **Request layer** (REQUEST, RESPONSE): `ConnectionID` is set; `SequenceNum` carries a monotonically increasing **RequestID** (scoped to the connection, starting at 1); `BlockGroup` is zero.
- **Data layer** (DATA, PARITY, HEARTBEAT, COMPLETE, ACK_CLOSE): `ConnectionID` is set; `SequenceNum` and `BlockGroup` carry their standard data-plane semantics.

This reuse means the 32-byte header is identical for all packet types — no per-type header variations, no branching in the hot-path parser.

## 4. Connection Establishment: 1-RTT Encrypted Handshake

Every HP-UDP interaction begins with a connection. Connections are always encrypted; there is no plaintext mode. The handshake is 1-RTT.

### 4.1 ConnectionID Generation

The **client** generates the ConnectionID as a cryptographically random 32-bit integer (C: `getrandom()` or `/dev/urandom`; Go: `crypto/rand`).

- **Collision Handling:** The server maintains a set of active ConnectionIDs. If an incoming `HELLO` carries a ConnectionID that is already in use, the server responds with a `REJECT` (Type `0x04`) containing reason code `CONNECTION_ID_COLLISION (0x01)`. The client generates a new random ConnectionID and retransmits the `HELLO`. At typical concurrency levels (hundreds of concurrent connections), 32-bit random IDs yield negligible collision probability (~1 in 10 million at 200 concurrent connections).

- **Stale Connection Protection:** ConnectionIDs are held in a reserved pool for 10 seconds after connection teardown to prevent late-arriving packets from a completed connection from being misattributed to a new connection reusing the same ID.

### 4.2 Handshake Sequence

```
Client                                Server
  │                                      │
  │──── HELLO ──────────────────────────►│  (ClientPubKey + ClientID)
  │                                      │  Generate keypair
  │                                      │  Derive session key
  │◄──── WELCOME ───────────────────────│  (ServerPubKey)
  │                                      │
  │  Derive session key                  │
  │                                      │
  │  ═══ Connection Established ═══      │
  │  All subsequent payloads encrypted   │
  │                                      │
  │──── REQUEST (PUT/GET/LIST/DEL) ────►│
  │◄──── RESPONSE ──────────────────────│
  │                                      │
  │  ... data transfer if PUT/GET ...    │
  │                                      │
  │──── PING ──────────────────────────►│  (keepalive during idle)
  │◄──── PONG ─────────────────────────│
  │                                      │
```

- **Step 1 (HELLO):** The client transmits a `HELLO` packet. The payload contains:

  | Field | Size | Description |
  |-------|------|-------------|
  | ClientPubKey | 32 bytes | X25519 ephemeral public key |
  | ClientID | 32 bytes | UTF-8 identifier, null-padded (hostname, callsign, or arbitrary label) |

  The client generates a fresh X25519 keypair for each connection. The private key exists only in memory.

- **Step 2 (WELCOME):** The server generates its own X25519 keypair, computes the shared secret, derives the session key (§4.3), and responds with a `WELCOME` packet. The payload contains:

  | Field | Size | Description |
  |-------|------|-------------|
  | ServerPubKey | 32 bytes | X25519 ephemeral public key |

  If the server cannot accept the connection (at capacity, ConnectionID collision, etc.), it sends a `REJECT` instead.

- **Step 3 (Session Key Derivation):** Upon receiving `WELCOME`, the client derives the same session key. The connection is now established and encrypted. All subsequent REQUEST, RESPONSE, DATA, and PARITY payloads are encrypted with AES-128-GCM using this session key.

**Retry:** If the client does not receive `WELCOME` or `REJECT` within 2 seconds, it retransmits the `HELLO` up to 3 times. If no response arrives after all retries, the connection attempt fails.

### 4.3 Session Key Derivation

Both sides independently derive the same symmetric key:

```
shared_secret = X25519(my_private_key, their_public_key)     // 32 bytes
okm           = HKDF-SHA256(
    ikm  = shared_secret,
    salt = ConnectionID (4 bytes, big-endian),
    info = "hp-udp-aes128-v6",
    len  = 24                                  // 16-byte key + 8-byte iv_base
)
session_key   = okm[0..15]                                    // AES-128 key
iv_base       = okm[16..23]                                   // Nonce base
```

The 24-byte HKDF output serves a dual purpose: bytes 0–15 are the AES-128-GCM session key, and bytes 16–23 become the shared `iv_base` used in nonce construction (§4.4). Both sides derive the same `iv_base` deterministically — no additional exchange is needed.

The `ConnectionID` salt ensures that even if the same ephemeral keypair were accidentally reused (implementation bug), different connections would derive different keys and nonce bases. The `info` string binds the key to the protocol version and cipher suite, preventing cross-protocol key reuse.

C implementations: OpenSSL `EVP_KDF` with `OSSL_KDF_NAME_HKDF`, or libsodium `crypto_kdf_hkdf_sha256_expand`. Go: `golang.org/x/crypto/hkdf`.

### 4.4 Per-Packet Encryption

Each DATA, PARITY, REQUEST, and RESPONSE packet is encrypted independently using AES-128-GCM. The packet header (32 bytes) is **not encrypted** — it is passed as Additional Authenticated Data (AAD) so that the receiver can route, reorder, and identify packets before decryption. The header is authenticated by the GCM tag, preventing tampering.

Control packets (HELLO, WELCOME, REJECT, PING, PONG, HEARTBEAT, COMPLETE, ACK_CLOSE) are **not encrypted**. HELLO and WELCOME carry key-exchange material and must be plaintext. HEARTBEAT, COMPLETE, and ACK_CLOSE contain only protocol metadata, not file content. PING/PONG are empty keepalives.

#### Wire Layout (Encrypted Packet)

```
┌──────────────────┬──────────────────────────┬──────────────┐
│ Header (32 B)    │ Ciphertext (PayloadLen B) │ GCM Tag (16B)│
│ cleartext, AAD   │ AES-128-GCM output        │ auth tag     │
└──────────────────┴──────────────────────────┴──────────────┘
 Total ≤ 1400 bytes.  PayloadLen ≤ 1352 (= 1368 − 16 tag).
```

`PayloadLen` in the header reflects the **plaintext length** (which equals the ciphertext length in GCM). The receiver reads `PayloadLen + 16` bytes from the payload area to get ciphertext + tag. **Encrypted MaxPayload = 1352 bytes.**

#### Nonce Construction (12 Bytes)

AES-GCM requires a unique nonce for every packet encrypted under the same key. Nonce reuse completely breaks GCM's confidentiality and authenticity guarantees. The nonce is constructed deterministically from the HKDF-derived `iv_base` and a per-packet counter:

| Bytes | Field | Purpose |
|---|---|---|
| 0–7 | iv_base (8 bytes) | Session-scoped nonce base derived from HKDF output (§4.3). Identical on both sides; never transmitted. |
| 8–11 | Nonce counter (big-endian, 4 bytes) | For DATA/PARITY: low 32 bits of `SequenceNum`. For REQUEST/RESPONSE: low 32 bits of `RequestID`. Unique per packet within the connection. |

**Nonce uniqueness proof:** `SequenceNum` is strictly incrementing and never reused within a transfer. `RequestID` is strictly incrementing and never reused within a connection. The maximum file size is 1 TB; at 1352 bytes per encrypted payload, that is at most ~810 million packets — well below the 2³² (≈4.29 billion) wrap point.

The nonce is **not transmitted on the wire**. Both sides compute it independently from `iv_base` and the `SequenceNum`/`RequestID` in the packet header.

**Nonce space partitioning between request and data layers:** REQUEST and RESPONSE packets use `RequestID` (carried in `SequenceNum` header field) as their nonce counter. DATA and PARITY packets use `SequenceNum`. Since data-plane sequence numbers reset to 0 for each transfer and request IDs increment monotonically across the connection, a collision is possible if, for example, `RequestID = 5` and `SequenceNum = 5` encrypt under the same key. To prevent this, the nonce includes a **domain separator**: bit 31 of the nonce counter (bytes 8–11) is set to 1 for request-layer packets and 0 for data-layer packets. This halves each domain to 2³¹ (~2.1 billion) values, which remains far above the maximum packet count for any single transfer or connection.

```
Data-layer nonce:    iv_base(8B) || 0 || seq_low31(31 bits, big-endian)
Request-layer nonce: iv_base(8B) || 1 || req_low31(31 bits, big-endian)
```

#### Encryption Placement in the Data Path

Encryption is applied **after FEC encoding**. The FEC encoder operates on plaintext data shards and produces plaintext parity shards. Each shard (DATA or PARITY) is then encrypted independently before transmission. On the receiver side, each packet is decrypted individually, then the plaintext shards are passed to the FEC decoder for reconstruction if needed.

```
Sender:  file → chunk → FEC encode (plaintext) → encrypt each shard → transmit
Receiver: receive → decrypt each shard → FEC decode (plaintext) → reassemble → disk
```

This ordering means FEC reconstruction operates on plaintext, which is correct — the Reed-Solomon math must see the original data bytes, not ciphertext.

### 4.5 Security Properties and Non-Goals

- **Confidentiality:** All file content and request metadata is encrypted in transit. An eavesdropper sees only ciphertext and packet headers (which contain sequence numbers and timing, but no file content or filenames).

- **Integrity:** GCM's authentication tag detects any modification to the ciphertext or header. A tampered packet fails decryption and is treated as a lost packet (NACKed for retransmission).

- **Forward secrecy:** Ephemeral keys are destroyed after each connection. Recorded traffic cannot be decrypted retroactively.

- **Replay protection:** The strictly-incrementing nonce combined with the per-connection key means replayed packets from a previous connection will fail decryption (wrong key), and replayed packets within a connection are detected by the existing duplicate-sequence-number check.

- **Non-goal — Authentication:** HP-UDP does not cryptographically authenticate the identity of the remote endpoint. The `ClientID` field is informational only. Any party that can reach the port can initiate a key exchange.

- **Non-goal — Metadata privacy:** Packet headers (including sequence numbers and timing) are visible to observers. Traffic analysis can reveal transfer size and duration. Filenames are encrypted (they travel inside REQUEST payloads). Metadata encryption is out of scope.

### 4.6 Performance Budget

| Operation | Throughput (AES-NI) | Impact at 100 MB/s wire speed |
|---|---|---|
| AES-128-GCM encrypt | 4–6 GB/s single-thread | <3% CPU |
| AES-128-GCM decrypt | 4–6 GB/s single-thread | <3% CPU |
| X25519 scalar multiply | ~50 µs per connection | Negligible |
| HKDF-SHA256 derivation | ~1 µs per connection | Negligible |
| Payload reduction (1368→1352) | 1.2% fewer data bytes per packet | ~1.2% more packets for same file |

**Net throughput impact: <5%.** AES-NI hardware acceleration is present on every x86 CPU manufactured since ~2010 (Intel Westmere / AMD Bulldozer). C implementations should use OpenSSL's `EVP_aes_128_gcm` (which auto-detects AES-NI) or a SIMD-accelerated library. Go's `crypto/aes` + `crypto/cipher` uses AES-NI on amd64 automatically.

**Implementation Note:** Do not allocate and free GCM cipher contexts per packet. Pre-allocate one context at connection start and reuse it across packets, updating only the nonce via `EVP_EncryptInit_ex(ctx, NULL, NULL, NULL, nonce)` (OpenSSL) or by resetting the `cipher.AEAD` seal call with a new nonce (Go). Context reuse eliminates ~25,000 allocations/sec at 35 MB/s throughput.

## 5. Request Framing

Once a connection is established (§4), the client sends operations as `REQUEST` packets and the server replies with `RESPONSE` packets. All REQUEST and RESPONSE payloads are encrypted (§4.4).

### 5.1 Request/Response Payload Format

**REQUEST payload (encrypted):**

| Field | Size | Description |
|-------|------|-------------|
| OpCode | 1 byte | Operation type (see §5.2) |
| OpPayload | variable | Operation-specific data |

**RESPONSE payload (encrypted):**

| Field | Size | Description |
|-------|------|-------------|
| StatusCode | 1 byte | 0x00 = OK, 0x01 = ERROR (see §5.3) |
| RespPayload | variable | Operation-specific response data |

The `RequestID` (carried in the header's `SequenceNum` field) ties a RESPONSE to its REQUEST. The client assigns RequestIDs as a monotonically increasing counter starting at 1 for each connection.

### 5.2 Operations

| OpCode | Name | Direction | Payload | Description |
|--------|------|-----------|---------|-------------|
| 0x01 | PUT | C→S | `FileSize(8B) + FileHash(8B) + InitialRate(4B) + FileName(null-term)` | Client will send a file to the server. Server responds OK to accept, then enters receiver mode. |
| 0x02 | GET | C→S | `FileName(null-term)` | Client requests a file from the server. Server responds with file metadata, then enters sender mode. |
| 0x03 | LIST | C→S | (empty) | Client requests the file manifest. Server responds with file listing. |
| 0x04 | DELETE | C→S | `FileName(null-term)` | Client requests deletion of a file. Server responds OK or ERROR. |
| 0x05 | PING | bidir | (empty) | Keepalive. Responded to with PONG (0x0A/0x0B packet types). Note: PING/PONG use their own packet types rather than REQUEST/RESPONSE framing for minimal overhead. |

### 5.3 Response Status Codes

| StatusCode | Name | Meaning |
|------------|------|---------|
| 0x00 | OK | Operation accepted. |
| 0x01 | ERROR | Operation failed. `RespPayload` contains a 1-byte reason code (§5.4) followed by an optional null-terminated UTF-8 error message. |

### 5.4 Error Reason Codes

| Code | Name | Meaning |
|------|------|---------|
| 0x01 | CONNECTION_ID_COLLISION | The submitted ConnectionID is already active. |
| 0x02 | HASH_MISMATCH | Received file hash does not match the declared value. |
| 0x03 | SERVER_BUSY | Maximum concurrent transfers reached; try again later. |
| 0x04 | FILE_NOT_FOUND | The requested filename is not in the serve manifest. |
| 0x05 | FILE_EXISTS | A PUT was rejected because the filename already exists (no-overwrite policy). |
| 0x06 | CLIENT_DISCONNECT | Sent by the client as a graceful disconnect signal (e.g. Ctrl+C). |
| 0x07 | INVALID_REQUEST | Malformed request payload (bad filename, zero file size, etc.). |
| 0x08 | DELETE_DENIED | Server policy does not permit deletion (optional). |

### 5.5 PUT Flow

```
Client                                Server
  │                                      │
  │── REQUEST (PUT, file metadata) ────►│  Validate, allocate ring buffer
  │◄── RESPONSE (OK) ──────────────────│
  │                                      │
  │  ═══ Data Transfer Phase ═══         │
  │  Client = sender, Server = receiver  │
  │                                      │
  │──── DATA (calibration burst) ──────►│
  │◄──── HEARTBEAT ────────────────────│
  │──── DATA (steady state) ──────────►│
  │──── PARITY ────────────────────────►│
  │◄──── HEARTBEAT (+ NACKs) ─────────│
  │  ... until EOF ...                   │
  │                                      │
  │──── COMPLETE ──────────────────────►│  Hash verified
  │◄──── ACK_CLOSE ────────────────────│
  │                                      │
  │  ═══ Connection returns to idle ═══  │
  │  Client may send another REQUEST     │
```

The PUT request payload mirrors the old SESSION_REQ:

| Field | Size | Description |
|-------|------|-------------|
| FileSize | 8 bytes | Total file size in bytes. Must be non-zero and ≤ configured max (default 1 TB). |
| FileHash | 8 bytes | xxHash64 of the complete file. |
| InitialRate | 4 bytes | Starting rate in bytes/sec. 0 = use calibration mode. |
| FileName | variable, null-terminated | Base filename only. Server strips all path separators. |

**Validation:** Before accepting, the server validates: file size non-zero and within limit, filename non-empty, filename does not already exist in the serve directory (no-overwrite rule), session limit not reached.

On `RESPONSE (OK)`, the client enters sender mode. The calibration burst, steady-state transfer, congestion control, FEC, teardown, and all data-plane mechanics are identical to v5.2 §4C Steps 2–5, §5, §6, §7, §8.

After a successful transfer (ACK_CLOSE received/sent), the connection returns to idle. The client may issue another REQUEST. Data-plane state (sequence numbers, sliding window, FEC blocks) is reset for the next transfer. The connection-layer encryption and ConnectionID persist.

### 5.6 GET Flow

```
Client                                Server
  │                                      │
  │── REQUEST (GET, filename) ─────────►│  Look up file in manifest
  │◄── RESPONSE (OK + file metadata) ──│
  │                                      │
  │  ═══ Data Transfer Phase ═══         │
  │  Server = sender, Client = receiver  │
  │                                      │
  │◄──── DATA (calibration burst) ─────│
  │──── HEARTBEAT ─────────────────────►│
  │◄──── DATA (steady state) ──────────│
  │◄──── PARITY ───────────────────────│
  │──── HEARTBEAT (+ NACKs) ──────────►│
  │  ... until EOF ...                   │
  │                                      │
  │◄──── COMPLETE ─────────────────────│  Hash verified
  │──── ACK_CLOSE ─────────────────────►│
  │                                      │
  │  ═══ Connection returns to idle ═══  │
```

The GET request payload is simply the null-terminated filename.

The RESPONSE (OK) payload contains the file metadata the client needs to prepare for receiving:

| Field | Size | Description |
|-------|------|-------------|
| FileSize | 8 bytes | Total file size. |
| FileHash | 8 bytes | xxHash64 of the file. |
| InitialRate | 4 bytes | Server's starting send rate (0 = calibration). |

On `RESPONSE (OK)`, the server enters sender mode and the client enters receiver mode. The data transfer is identical to PUT but with reversed roles.

**NAT Traversal:** Because the client always initiates the connection (HELLO →), the outbound UDP packet punches the NAT hole. All subsequent packets — including DATA flowing from server to client during a GET — traverse the same NAT mapping. No port forwarding is required on the client side. The server must have a reachable port (forwarded or public IP).

### 5.7 LIST Flow

```
Client                                Server
  │                                      │
  │── REQUEST (LIST) ──────────────────►│  Read manifest
  │◄── RESPONSE (OK + file listing) ───│
```

LIST is always serviced regardless of whether a data transfer is in progress on another connection.

The RESPONSE payload is a UTF-8 string where each entry is a tab-separated `filename\tsize\n` line (filename, a tab character, the file's byte count as a decimal integer, and a newline). Empty payload means no files are available. The list is truncated to fit within MaxPayload (1352 bytes) if the directory is very large. Example: `backup.tar.gz\t524288000\nreport.pdf\t2097152\n`.

### 5.8 DELETE Flow

```
Client                                Server
  │                                      │
  │── REQUEST (DELETE, filename) ──────►│  Remove from manifest + disk
  │◄── RESPONSE (OK or ERROR) ─────────│
```

The server removes the file from disk and from the in-memory manifest under a write-lock. If the file does not exist, the server responds with `ERROR (FILE_NOT_FOUND)`. Servers may optionally refuse DELETE entirely with `ERROR (DELETE_DENIED)`.

**Security:** The same base-name-only rule applies — path separators are stripped from the filename before lookup. Only files in the served directory can be deleted.

### 5.9 Connection Lifecycle

**Idle timeout:** If no REQUEST, PING, DATA, or HEARTBEAT is exchanged for 30 seconds, the server closes the connection and frees the ConnectionID. The client should send PING to keep the connection alive during idle periods.

**PING/PONG:** Use their own packet types (0x0A/0x0B) rather than REQUEST/RESPONSE framing. They carry no payload and are not encrypted (they contain no content). PING resets the idle timer on both sides.

**Graceful close:** The client sends a REJECT with reason `CLIENT_DISCONNECT (0x06)`. The server frees the connection slot and moves the ConnectionID to the reserved pool. If a data transfer is in progress, it is aborted.

**Ungraceful close:** If the client disappears, the idle timeout (or the data-plane inactivity timeout during a transfer) handles cleanup.

## 6. Core Reliability Mechanisms

### 6A. Sequence Buffering (Zero-Blocking Receiver)

The receiver will never halt reading from the network socket. Incoming data is immediately mapped into memory.

- **Memory Architecture:** The receiver allocates a contiguous ring buffer based on the initial `FileSize`.

- **Placement:** As datagrams arrive, their `SequenceNum` dictates their exact memory offset. Out-of-order packets are slotted into their correct positions seamlessly.

- **Disk I/O:** Contiguous blocks are flushed from the buffer to disk asynchronously. Go: a dedicated goroutine reads and writes sequentially. C (Linux): `io_uring` submission queue — the main `epoll` event loop submits write SQEs for contiguous regions and reaps completions without blocking the network read path.

### 6B. Adaptive Forward Error Correction (FEC)

To eliminate latency penalties from round-trip retransmissions, the protocol proactively embeds mathematical redundancy that adapts to observed network conditions.

#### Block Grouping

Data packets are organized into sequential `BlockGroups`. The default block size is 100 data packets per group. The `BlockGroup` identifier for a DATA packet is computed as:

```
BlockGroup = SequenceNum / BlockSize   (integer division)
```

PARITY packets use the same `BlockGroup` value as the data packets they protect. The `SequenceNum` field of a PARITY packet is its **zero-based index within the block** (0, 1, 2, …, m−1), not a global sequence number.

#### Dynamic Parity Ratio

The parity packet count per block is dynamically adjusted based on the observed packet loss rate, reported via Heartbeat metrics (§7).

| Observed Loss Rate | Parity Ratio | Parity Packets per 100-Packet Block |
|---|---|---|
| < 0.5% | 2% | 2 |
| 0.5% – 2% | 5% | 5 |
| 2% – 5% | 10% | 10 |
| 5% – 10% | 15% | 15 |
| > 10% | 20% | 20 |

The sender initializes at **5% parity** during calibration and adjusts after the first Heartbeat containing loss data. Adjustments are applied on **block group boundaries** — mid-block changes are not permitted, as this would invalidate the Reed-Solomon coding parameters for that group.

#### Parity Generation

Parity packets are generated using **Reed-Solomon erasure coding** over GF(2⁸) with the irreducible polynomial `x⁸ + x⁴ + x³ + x² + 1` (0x11d). For a block of `k` data packets with `m` parity packets, any `k` of the `k+m` total packets are sufficient to reconstruct the original data. The encoding uses a Vandermonde-derived matrix whose top `k` rows form an identity matrix, ensuring data packets pass through unchanged and only parity is computed.

#### On-the-Fly Recovery

If the receiver detects missing packets within a completed block group (all expected sequence numbers accounted for or timed out), it attempts FEC reconstruction immediately. Only packets that cannot be recovered via FEC are reported as NACKs in the next Heartbeat.

#### Tail Block Handling

The final block group of a file transfer will almost certainly contain fewer than 100 data packets. The FEC parameters adapt as follows:

- The tail block size `k_tail` equals the remaining packet count after the last full block.
- The parity count `m_tail` is calculated using the current adaptive parity ratio, with a **minimum of 2 parity packets** regardless of block size.
- The sender sets the `EndOfFile` flag (`0x01`) on the final data packet and the final parity packet of the tail block.

The key formulas for computing totals and boundaries are:

```
TotalChunks      = ceil(FileSize / MaxPayload)          // MaxPayload = 1352 (always encrypted)
k_tail           = TotalChunks % BlockSize               // 0 means last block is full
(if k_tail == 0: k_tail = BlockSize)
FinalPayloadSize = FileSize % MaxPayload
(if FinalPayloadSize == 0: FinalPayloadSize = MaxPayload)
```

**EOF Detection in FEC-Recovered Packets:** The `EndOfFile` flag is only meaningful on the wire. Receivers must detect end-of-transfer by checking `SequenceNum == TotalChunks − 1` rather than relying solely on the Flags field.

## 7. Adaptive Congestion and Flow Control

HP-UDP separates **congestion control** (network path capacity) from **flow control** (receiver processing capacity). The congestion controller is **loss-driven**: the primary signal is the observed packet loss rate, not the ratio of delivery rate to send rate.

### 7A. Calibration Burst

The first operation on a connection that involves a data transfer (PUT or GET) begins with a calibration burst to probe link capacity.

The sender transmits **10 packets** with the `Calibration` flag (`0x02`) set, sent back-to-back at wire speed. The token bucket is initialized at a default starting rate of 2 MB/s.

If the `InitialRate` field in the PUT/GET metadata is non-zero, the sender skips calibration mode and begins transmitting at the specified rate immediately. The adaptive congestion controller still takes over after the first Heartbeat.

**Subsequent transfers on the same connection** may reuse the peak rate estimate from the previous transfer as the initial rate, avoiding re-calibration on known links. This is an implementation optimization, not a protocol requirement.

### 7B. The Heartbeat Packet

The receiver periodically sends a `HEARTBEAT` (Type `0x07`) packet to the sender. The heartbeat interval is **rate-proportional** based on the last measured `NetworkDeliveryRate`:

| Last Measured Network Delivery Rate | Heartbeat Interval |
|---|---|
| < 10 MB/s | 100ms |
| 10 – 100 MB/s | 50ms |
| 100 MB/s – 1 GB/s | 25ms |
| > 1 GB/s | 10ms |

#### Heartbeat Payload

The Heartbeat payload contains dual metrics, RTT echo, and a NACK array. All multi-byte fields are **big-endian**:

| Field | Size | Description |
|---|---|---|
| NetworkDeliveryRate | 4 Bytes | Bytes per second successfully received during the last heartbeat interval. |
| StorageFlushRate | 4 Bytes | Bytes per second flushed to disk during the last heartbeat interval. Reported for observability only — not an input to the rate controller. |
| LossRate | 2 Bytes | Packet loss percentage encoded as basis points (e.g., 150 = 1.50%). Primary CC signal. |
| EchoTimestampNs | 8 Bytes | The verbatim `SenderTimestampNs` from the most recently received DATA or PARITY packet. |
| DispersionNs | 8 Bytes | Calibration burst dispersion (nanoseconds). Zero outside calibration. |
| HighestContiguous | 8 Bytes | Highest SequenceNum N such that all packets 0..N have been received or FEC-recovered. |
| NACKCount | 2 Bytes | Number of unrecoverable sequence numbers in the NACK array. |
| NACKArray | 8 Bytes × N | Array of 64-bit SequenceNum values requiring retransmission. Max 166 entries per packet. |

#### RTT Measurement — Same-Clock Design

Each DATA and PARITY packet carries a sender timestamp in `SenderTimestampNs`. The receiver echoes the sender's own timestamp verbatim. The sender computes `RTT = now_ns − EchoTimestampNs` using only its own monotonic clock, eliminating cross-machine clock skew.

#### Frozen-Timestamp RTT Guard

The sender tracks `lastEchoNs` — the highest `EchoTimestampNs` it has processed. RTT is updated only when `echoNs > lastEchoNs`. Stale repeated echoes are silently ignored.

### 7C. Loss-Driven Rate Adjustment Algorithm

The sender adjusts its rate based on the `LossRate` reported in each Heartbeat. Rate increases are **gated to once per RTT**.

#### RTT-Aware Rate Gating

When a heartbeat signals an increase, the sender checks whether at least one RTT has elapsed since the previous increase. If not, the increase is suppressed. Decreases are **not gated** — they are applied immediately.

#### Phased Growth Model

**Phase 1 — Probe (Multiplicative Increase):** While loss < 1%, the sender probes with multiplicative increase, applied once per RTT:

```
S_new = S_current × 1.25
```

**Phase 2 — Congestion Avoidance (Additive Increase):** Once the sender observes loss entering the 1%–5% hold zone for the first time, it permanently transitions to Phase 2. Probing uses additive increase, applied once per RTT:

```
S_new = S_current + (MaxPayload / RTT)
```

#### Decision Logic

Let `L` = reported `LossRate` in basis points, `E` = effective delivery rate (`NetworkDeliveryRate`), `S` = current send rate. On each Heartbeat, the **delivery-collapse guard is checked first**:

| Condition | Action | Rationale |
|---|---|---|
| NACKCount > 0 AND E < S × 0.25 | Hold + permanently transition to Phase 2. | OS socket buffer overflow. Packets dropped before reaching the receiver application. |
| L < 100 (loss < 1%) | Increase (once per RTT): Phase 1: S × 1.25. Phase 2: S + MaxPayload/RTT. | Link has headroom. |
| 100 ≤ L ≤ 500 (1% – 5%) | Hold: S = S. If first time, transition to Phase 2. | FEC handling the loss. Link ceiling discovered. |
| L > 500 (loss > 5%), consecutive confirmation | Decrease: S = smoothed(E) × 0.85 | Drop to 85% of EWMA-smoothed delivery rate. Requires two consecutive above-threshold heartbeats. |

#### EWMA Smoothing

```
smoothed = α × raw_sample + (1 − α) × smoothed_previous     (α = 0.3)
```

#### Auto-Ceiling

```
Phase 1:  if rate > peakDeliveryRate × 4.0: rate = peakDeliveryRate × 4.0
Phase 2:  if rate > peakDeliveryRate × 1.5: rate = peakDeliveryRate × 1.5
```

### 7D. Deficit-Accumulator Pacing

The sender converts the target rate (bytes/sec) into inter-packet timing using a deficit accumulator:

- A `tokens` balance accrues credit at the target rate over elapsed wall-clock time (monotonic clock source).
- Each packet send debits `tokens` by the packet size.
- Tokens are capped at **2ms burst budget**: `max_tokens = rate_bytes_per_sec × 0.002`.
- When deficit ≥ 1ms of sleep, the sender sleeps. Smaller deficits are carried forward.

C: `clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME)`. Go: `time.Sleep()`.

### 7E. NACK-Driven Retransmission

Retransmissions are **interleaved with forward progress**: at most **3 NACKed packets per send-loop iteration**, then the next new data packet. The pending NACK set is a **deduplicated set** (not a FIFO queue).

## 8. Session Timeout and Failure Recovery

### 8A. Receiver Inactivity Timeout

If the receiver does not receive any packets for **5 consecutive expected heartbeat intervals**, with a **minimum floor of 5 seconds**, it declares the transfer dead, frees the ring buffer, and sends an error to the connection layer. The connection itself may remain open — the client can retry the operation.

### 8B. Sender Inactivity Timeout

#### Hard Abort — Heartbeat Staleness

During active data transmission, if no Heartbeat arrives for **3 seconds** (`SenderHeartbeatTimeout`), the sender aborts the transfer. The connection may remain open.

#### Probe State — Intermittent Loss

If the sender does not receive a Heartbeat for 5 consecutive expected intervals, it pauses and enters Probe state, sending a single DATA packet every 500ms. Timeout after 10 seconds of probing.

## 9. Graceful Teardown (The Final Handshake)

### 9A. Normal Teardown Sequence

- **EOF Signal:** The sender sets `EndOfFile` on the final data and parity packets and stops reading new data, keeping the socket open.

- **Pipelined Hashing:** The receiver continuously updates a streaming `xxHash64` as contiguous blocks flush. Upon EOF + hash match, it sends `COMPLETE` (Type `0x08`).

- **Sender Linger:** The sender receives `COMPLETE`, responds with `ACK_CLOSE` (Type `0x09`), frees transfer-specific memory (sliding window, FEC state), and enters a **3-second linger**. Duplicate `COMPLETE` packets are answered with repeat `ACK_CLOSE`.

- **Receiver Linger:** After sending `COMPLETE`, the receiver enters a 3-second linger. If `ACK_CLOSE` is not received, retransmits `COMPLETE` up to 3 times at 1-second intervals. If no `ACK_CLOSE` after all retries, the transfer is considered successful (hash was verified).

- **Return to Idle:** After linger, the connection returns to the idle state. The client may issue another REQUEST. The ConnectionID and encryption context persist.

### 9B. Teardown NACK Handling

During teardown, the sender continues processing Heartbeats synchronously. NACKed packets are retransmitted from the sliding window, paced through the token bucket.

#### RTT-Aware NACK Cooldown

A `nackCooldown` map gates each sequence number to at most one retransmit per `RTT × 1.25`.

#### Tail-Drop Deadlock Prevention

If a heartbeat has `NACKCount == 0` but `HighestContiguous < totalChunks−1`, the sender proactively injects up to 167 missing tail sequences into the retransmit pipeline.

#### Teardown Micro-Burst Prevention

Teardown retransmits are chunked into **batches of 10 packets** with a **2ms sleep** between batches.

### 9C. Hash Mismatch

If the hash does not match, the receiver sends a `REJECT` with reason `HASH_MISMATCH`. The partially written file is deleted. The connection remains open — the client may retry.

### 9D. Receiver-Side Self-Completion

When the receiver constructs a heartbeat with `NACKCount == 0` and `HighestContiguous + 1 >= TotalChunks`, it self-initiates teardown: flush, fsync, hash verify, send `ACK_CLOSE`, enter linger. This makes the receiver resilient to lost `COMPLETE` packets.

## 10. Server Mode

The server is a persistent multi-connection UDP listener that manages a file directory and services GET, PUT, LIST, and DELETE requests from connected clients. It listens on a **single UDP socket** and dispatches packets by `(src_addr, ConnectionID)` tuple.

### 10A. Concurrency

Up to **`HPUDP_MAX_CONNECTIONS = 16` concurrent connections** are supported. Additional HELLO packets beyond that limit receive `REJECT (SERVER_BUSY)`. LIST requests on existing connections are always serviced regardless of whether data transfers are in progress.

Only **one data transfer at a time per connection**. If a client sends a PUT request while a previous transfer is still in progress on the same connection, the server responds with `ERROR (SERVER_BUSY)`. Multiple connections can each have one active transfer concurrently (up to the connection limit).

### 10B. Manifest Lifecycle

The manifest is a filename-to-absolute-path map built at server startup by a non-recursive directory scan. Symlinks and directories are excluded. Files added to the directory after startup are invisible until restart — this is an intentional security boundary. Successful PUT transfers atomically add their promoted file to the in-memory manifest under a write-lock. DELETE removes from both disk and manifest under a write-lock. GET handlers acquire a read-lock.

### 10C. PUT Security Invariants

1. **Base-name-only rule:** Path separators stripped from filename before processing.
2. **No-overwrite rule:** If file exists, respond with `ERROR (FILE_EXISTS)`.
3. **Post-hash atomic rename:** Written to `filename.tmp` during transfer, renamed to final path only after successful hash verification. Failed transfers leave no partial files.

## 11. Resumable Transfers

A transfer interrupted mid-flight can be resumed transparently without wire-level negotiation. The mechanism is entirely receiver-side.

### 11A. Checkpoint Sidecar

The receiver writes a binary checkpoint sidecar (`<output>.hpudp-ckpt`) on every heartbeat. All fields are **little-endian** (local file format, not wire format).

| Offset | Size | Field | Description |
|---|---|---|---|
| 0 | 4 bytes | magic | Always 0x48505543 ("HPUC"). |
| 4 | 4 bytes | version | Always 1. |
| 8 | 8 bytes | file_size | Total declared file size. |
| 16 | 8 bytes | file_hash | xxHash64 of the complete file. Used to match checkpoint to a resumed session. |
| 24 | 8 bytes | total_chunks | ceil(file_size / MaxPayload). |
| 32 | 8 bytes | highest_contiguous | Highest sequence N such that all 0…N received. |
| 40 | variable | recv_bits | Receive bitset: ceil(total_chunks / 8) bytes. |

Written atomically (write to `.tmp`, then rename). Deleted on successful completion.

### 11B. Transparent Resume Flow

Resume is transparent to the sender. The sender always starts a fresh PUT from sequence 0. The receiver checks for a matching checkpoint sidecar (by `file_hash`) and restores state if found.

1. Receiver receives the PUT request. Checks for sidecar at `<output>.hpudp-ckpt`. Match by `file_hash`.
2. Receiver loads sidecar: restores bitset and `highest_contiguous`. The mmap file already contains previously-received data.
3. Sender transmits from seq 0. Receiver skips packets whose bit is already set.
4. On first heartbeat, receiver reports restored `HighestContiguous`. Sender's window advances past acknowledged sequences.
5. Transfer completes, hash verified, sidecar deleted.

## 12. CLI Interface

HP-UDP ships as a single binary with two modes of operation.

### 12A. One-Shot Mode

Each command implicitly opens a connection, performs one operation, and closes:

```bash
# Start a server sharing /data/files
./hpudp serve 0.0.0.0:9000 /data/files

# Send a file to a server
./hpudp put 10.0.0.1:9000 ./backup.tar.gz

# Retrieve a file from a server
./hpudp get 10.0.0.1:9000 backup.tar.gz -o /tmp

# List files on a server
./hpudp ls 10.0.0.1:9000

# Delete a file on a server
./hpudp rm 10.0.0.1:9000 old-backup.tar.gz
```

### 12B. Shell Mode

A persistent connection with an interactive prompt:

```bash
./hpudp connect 10.0.0.1:9000

hpudp> ls
backup.tar.gz     524288000
report.pdf        2097152

hpudp> get backup.tar.gz -o /tmp
[████████████████████████████] 100% | 98.2 MB/s | 5.1s

hpudp> put ./new-report.pdf
[████████████████████████████] 100% | 45.7 MB/s | 0.4s

hpudp> rm old-backup.tar.gz
Deleted.

hpudp> exit
```

PING/PONG keepalives are sent automatically in the background during idle periods.

### 12C. Commands

| Command | Description |
|---------|-------------|
| `serve` | Run the server daemon |
| `put`   | Send a file to a server (one-shot) |
| `get`   | Retrieve a file from a server (one-shot) |
| `ls`    | List files on a server (one-shot) |
| `rm`    | Delete a file on a server (one-shot) |
| `connect` | Open a persistent shell connection |
| `resume` | Identical to `put` — resume is transparent via receiver checkpoint |

### 12D. Options

| Flag | Description |
|------|-------------|
| `-v`, `--verbose` | Debug-level log output |
| `-q`, `--quiet` | Suppress all output except errors |
| `-o`, `--output` | Output directory for received files |
| `-r`, `--rate` | Initial send rate override (e.g., `100M`, `1G`) |
| `--id` | Client identity string (default: hostname) |

## 13. Development Phases

### Progress Bar — Repair State

Once the main send loop completes and the sender enters teardown, the progress bar changes to `100% | Repairing... | NACKs: N`.

1. **Phase 1 (Go Prototype) — COMPLETE:** Validated FEC, Heartbeat state machine, adaptive FEC tuning, loss-driven CC with deficit-accumulator pacing, NACK retransmission. 86 unit tests. Tested on LAN (1 Gbps, 41 MB/s) and WAN (Starlink). Phase 1 bottleneck: per-packet `conn.Write()` syscall overhead (~67,000 syscalls/sec).

2. **Phase 2 (C Productionization):** Translate validated logic into C with:
   - `sendmmsg()`/`recvmmsg()` batching (16–64 packets per syscall)
   - Zero-copy receive via `mmap()` / `PACKET_RX_RING` / `AF_XDP`
   - SIMD Reed-Solomon via Intel ISA-L or AVX2/NEON
   - `io_uring` async disk I/O
   - Connection-oriented framing (v6.0 spec)

3. **Phase 3 (Go Client as Windows Frontend):** The Go prototype serves as the cross-platform client (Windows/macOS), connecting to the high-performance C server on Linux. The Go client implements the v6.0 connection and request framing; the C server is the performance target.

## 14. Lessons Learned (Phase 1)

### A. Delivery-Rate-Ratio CC Is Fundamentally Broken

The v2.0 algorithm increased the rate only when `EffectiveRate ≥ 0.95 × SendRate`. Since delivery rate is bounded by send rate and measurement windows never align perfectly, this ratio consistently falls below 0.95 even on a lossless link. Loss rate is the correct primary signal.

### B. Sub-Millisecond Pacing Is Unreliable in Userspace

Both `time.Sleep(<1ms)` and busy-wait spin loops fail for sub-millisecond pacing. The deficit accumulator sidesteps this by sleeping only when the accumulated deficit justifies a ≥1ms sleep.

### C. Calibration Burst Must Not Flood the Link

A 50 MB/s starting rate on Starlink caused massive initial loss that poisoned `peakRate`. The starting rate must be conservative (2 MB/s default) while the calibration burst runs at wire speed to discover capacity.

### D. NACK Storms Stall Forward Progress

On satellite with ~169 NACKs per heartbeat, processing all NACKs before new data caused the send loop to stall. Capping retransmissions at 3 per iteration restored forward progress.

### E. Early Delivery-Rate Measurements Are Unreliable

During Phase 1 ramp-up, delivery measurements lag the send rate. The auto-ceiling is gated on Phase 2 entry rather than a fixed warmup period.

### F. Socket Ownership During Teardown

Go: the heartbeat listener goroutine must be stopped before entering teardown. C: single-threaded `epoll` has exclusive access by construction.

### G. Per-Packet Syscall Overhead Is the Phase 1 Bottleneck

FTP achieves 93 MB/s where HP-UDP reaches ~41 MB/s on the same LAN. The difference is syscall pattern, not protocol. Phase 2 `sendmmsg()` batching resolves this.

### H. Go-Specific Memory Pressure

1. FEC encoder construction cost: cache the encoder keyed on `(dataShards, parityShards)`.
2. FEC shard buffer allocations: use `sync.Pool` of pre-allocated buffers.
3. Unbounded retransmit cache: replaced with bounded sliding window ring buffer (65,536 slots, power-of-2 for bitmask wrapping).

### I. Auto-Ceiling Overshoot Causes Persistent NACK Storms

Two-tier ceiling (Phase 1: 4×, Phase 2: 1.5×) plus NACK deduplication (set, not queue) resolved persistent 167-NACK storms.

### J. OS Socket Buffer Drops Are Invisible to Loss Rate (LFN)

Delivery-collapse guard (`NACKCount > 0 AND E < S × 0.25`) catches OS-layer drops that the FEC-based loss counter misses. Paced teardown retransmits prevent heartbeat-backlog bursts.

### K. Backpressure Starves NACK Retransmits (Window-Full Deadlock)

Replace `for sw.IsFull { sleep }` with `if sw.IsFull { sleep; continue }` so the outer loop still drains NACKs during backpressure.

### L. Cooldown Spin-Lock During Teardown

Track consecutive cooldown skips; when all entries are in cooldown, break out and return to the teardown main loop.

### M. Receiver-Side Self-Completion

When `NACKCount == 0` and `HighestContiguous + 1 >= TotalChunks`, the receiver self-initiates teardown. Makes receiver resilient to lost `COMPLETE` packets.

### N. Serve Daemon Must Handle PARITY Packets

PARITY handling must be identical to standalone receiver: maintain FEC block pool, attempt RS recovery, update receive bitset and `HighestContiguous`.

## Appendix A: Protocol Constants (Defaults)

| Parameter | Default Value | Configurable |
|---|---|---|
| MTU Hard Cap | 1400 bytes (total) | No |
| Header Size | 32 bytes | No |
| Max Payload (encrypted) | 1352 bytes (1368 − GCM tag 16) | No |
| FEC Block Size | 100 data packets | Yes |
| FEC Initial Parity | 5% | Yes |
| FEC Tail Min Parity | 2 packets | Yes |
| Calibration Burst Size | 10 packets (packet train) | Yes |
| Default Starting Rate | 2 MB/s | Yes |
| EWMA Smoothing Factor (α) | 0.3 | Yes |
| Loss Threshold: Increase | < 100 bp (1%) | Yes |
| Loss Threshold: Hold | 100–500 bp (1–5%) | Yes |
| Loss Threshold: Decrease | > 500 bp (5%) | Yes |
| Consecutive Decrease Signals | 2 | Yes |
| Phase 1 Increase Multiplier | 1.25× per RTT | Yes |
| Phase 2 Additive Increase | MaxPayload / RTT per RTT | Yes |
| Decrease Factor | 0.85× smoothed delivery rate | Yes |
| Auto-Ceiling (Phase 1) | 4× peak delivery rate | Yes |
| Auto-Ceiling (Phase 2) | 1.5× peak delivery rate | Yes |
| Deficit Accumulator Burst Cap | 2ms of credit | Yes |
| Max NACKs per Send Iteration | 3 | Yes |
| Rate Floor | 10 KB/s | Yes |
| Inactivity Timeout (data plane) | max(5 × heartbeat interval, 5s) | Yes |
| Sender Heartbeat Timeout | 3 seconds | Yes |
| Sender Probe Interval | 500ms | Yes |
| Sender Probe Timeout | 10 seconds | Yes |
| Linger Duration | 3 seconds | Yes |
| Receiver Teardown Retries | 3 | Yes |
| Stale ConnectionID Reservation | 10 seconds | Yes |
| Max File Size | 1 TB | Yes |
| Teardown Batch Size | 10 packets per sleep | Yes |
| Teardown Batch Sleep | 2 ms | Yes |
| Delivery-Collapse Threshold | 25% of current send rate | Yes |
| NACK Cooldown Margin | RTT × 1.25 | Yes |
| Tail-Drop Injection Limit | 167 sequences per heartbeat | Yes |
| Sliding Window Slots | 65,536 (2¹⁶, ~89 MB peak) | Yes |
| Encryption Cipher | AES-128-GCM (128-bit key, 128-bit auth tag, 96-bit nonce) | No |
| GCM Tag Size | 16 bytes | No |
| GCM Nonce Size | 12 bytes: iv_base(8B) ‖ domain_bit(1b) ‖ counter(31b) | No |
| Key Exchange | X25519 ephemeral (32-byte public key per side) | No |
| Key Derivation | HKDF-SHA256, salt = ConnectionID (4B BE), info = "hp-udp-aes128-v6", output = 24B | No |
| Connection Idle Timeout | 30 seconds | Yes |
| HELLO Retry Interval | 2 seconds | Yes |
| HELLO Max Retries | 3 | Yes |
| Max Connections (server) | 16 | Yes |
| ClientID Size | 32 bytes (null-padded UTF-8) | No |

## Appendix B: Migration from v5.x

| v5.x Concept | v6.0 Equivalent | Notes |
|---|---|---|
| `SESSION_REQ` | `HELLO` + `REQUEST (PUT)` | Connection establishment separated from transfer initiation. |
| `SESSION_ACCEPT` | `WELCOME` | Key exchange is now part of connection, not per-transfer. |
| `SESSION_REJECT` | `REJECT` or `RESPONSE (ERROR)` | Connection-level vs request-level errors distinguished. |
| `PUSH_REQ` / `PUSH_ACCEPT` | `REQUEST (PUT)` / `RESPONSE (OK)` | Push is just a PUT on an established connection. |
| `PULL_REQ` | `REQUEST (GET)` / `RESPONSE (OK)` | Pull is just a GET on an established connection. |
| `LIST_REQ` / `LIST_RESP` | `REQUEST (LIST)` / `RESPONSE (OK + listing)` | Standard request/response framing. |
| `SessionID` | `ConnectionID` | Same 32-bit field, now scoped to connection lifetime. |
| `-e` / `--encrypt` flag | Removed | Always encrypted. |
| `send` / `recv` commands | `put` / `get` | Client always initiates connection. No receiver-listens mode. |
| `push` / `pull` commands | `put` / `get` | Same operations, uniform naming. |
| Unencrypted mode | Removed | Encryption is mandatory. MaxPayload is always 1352. |

## Appendix C: Revision Log

- **v6.0** — Connection-oriented architecture: mandatory 1-RTT encrypted handshake (HELLO/WELCOME) replaces per-transfer SESSION_REQ/SESSION_ACCEPT; request-level multiplexing (GET/PUT/LIST/DELETE) over persistent connections; single data stream per connection; unified packet type table (12 types, down from 15); nonce domain separation for request vs data layer; `Encrypted` flag removed (always encrypted); `send`/`recv`/`push`/`pull` unified to `put`/`get`; shell mode (interactive prompt) added alongside one-shot CLI; connection idle timeout with PING/PONG keepalive; ClientID field for lightweight identity; server mode replaces serve daemon terminology; DELETE operation added; NAT traversal simplified (client always initiates); unencrypted mode removed.

- **v5.2** — C implementation alignment: HKDF output extended to 24 bytes; GCM nonce redesigned; PUSH_ACCEPT simplified; PUSH_REQ aligned with SESSION_REQ; serve daemon upgraded to 16 concurrent sessions; LIST_RESP tab-separated format; §12 Resume rewritten as transparent receiver-side checkpoint; checkpoint sidecar renamed `.hpudp-ckpt`.

- **v5.1** — Resumable transfers; graceful client disconnect; catalog query (LIST_REQ/LIST_RESP).

- **v5.0** — End-to-end encryption: ephemeral X25519 + AES-128-GCM; SESSION_ACCEPT packet type; Encrypted flag; 1-RTT encrypted handshake.

- **v4.2** — C implementation readiness: universal big-endian; power-of-2 sliding window.

- **v4.1** — Sender sliding window: bounded ring buffer replaces unbounded map.

- **v4.0** — WAN reliability: SenderTimestampNs; frozen-timestamp RTT guard; NACK cooldown; tail-drop prevention; teardown micro-burst prevention; delivery-collapse threshold 50%→25%.

- **v3.1** — Calibration packet train (10 packets); dispersion measurement; two-phase CC; deficit accumulator.

- **v3.0** — Loss-driven CC replaces delivery-rate-ratio.

- **v2.0** — Calibration burst; sender timestamps; multiplicative increase.

- **v1.0** — Initial specification.
