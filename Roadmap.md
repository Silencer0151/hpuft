# HP-UDP Protocol Roadmap — Aspera Feature Parity

> **Goal:** Close the protocol-level gap with IBM Aspera FASP.
> Four workstreams, ordered by dependency and impact.

---

## 1. End-to-End Encryption (v5.0) — SPEC COMPLETE

**Status:** Fully specified in §4.5 of the protocol spec. Implementation pending.

**Why:** Aspera uses AES-128-GCM per-datagram. Without encryption HP-UDP is
unusable on any network where data confidentiality matters. This is table
stakes for any serious deployment, especially DoD/IC environments.

### Design Summary (see spec §4.5 for full details)

**Pure ephemeral X25519 key exchange.** Both sides generate a fresh keypair
per session. Private keys exist only in memory and are zeroed on teardown.
No persistent key material, no key management, **perfect forward secrecy.**
This is the right model for managed networks where both endpoints are known
infrastructure behind SDNs — authentication is handled at the network layer,
the crypto just protects data in transit.

**1-RTT handshake.** The sender includes its ephemeral public key in
SESSION_REQ. The receiver responds with SESSION_ACCEPT (0x0A) carrying its
own ephemeral public key. Both sides derive the session key via HKDF-SHA256.
The sender blocks until SESSION_ACCEPT arrives, then begins encrypted data
transmission. Unencrypted transfers remain 0-RTT (SESSION_ACCEPT is skipped).

**AES-128-GCM per-packet.** Each DATA and PARITY packet is encrypted
independently. The 32-byte header is passed as AAD (authenticated but not
encrypted — receiver needs cleartext headers for routing and reordering).
16-byte GCM auth tag is appended to each packet. MaxPayload drops from
1368 to 1352 bytes (~1.2% overhead).

**Nonce (12 bytes, not transmitted):** SessionID (4B) + PacketType (1B) +
UniqueID (7B). DATA uses lower 56 bits of SequenceNum; PARITY uses
(BlockGroup << 8) | ParityIndex. PacketType separates the two nonce spaces.
Nonce uniqueness is guaranteed by the protocol's strict sequencing.

**Encrypt-after-FEC.** FEC encode operates on plaintext, then each shard
is encrypted. Receiver decrypts each shard, then FEC decodes if needed.

**Serve daemon integration.** PUSH_REQ and PULL_REQ already have a round
trip (PUSH_ACCEPT / the pull response), so the key exchange piggybacks on
existing payloads with zero added latency.

### Performance Budget

| Operation | Throughput (AES-NI) | Impact at 100 MB/s |
|-----------|-------------------|-------------------|
| AES-128-GCM encrypt/decrypt | 4–6 GB/s single-thread | <3% CPU |
| X25519 scalar multiply | ~50 µs per session | Negligible |
| HKDF-SHA256 derivation | ~1 µs per session | Negligible |
| Payload reduction (1368→1352) | 1.2% fewer bytes/packet | ~1.2% more packets |

**Net throughput impact: <5%.**

### Implementation Notes (C)

- OpenSSL `EVP_aes_128_gcm` for AES-GCM (auto-detects AES-NI).
- OpenSSL `EVP_PKEY_derive` or libsodium `crypto_scalarmult_curve25519` for X25519.
- OpenSSL `EVP_KDF` with `OSSL_KDF_NAME_HKDF` for key derivation.
- **Pre-allocate one `EVP_CIPHER_CTX` per session.** Update nonce only via
  `EVP_EncryptInit_ex(ctx, NULL, NULL, NULL, nonce)`. Avoids ~25,000
  context alloc/free per second at 35 MB/s.
- Securely zero private key on session teardown: `OPENSSL_cleanse()` or
  `explicit_bzero()`.

### Implementation Notes (Go)

- `crypto/aes` + `crypto/cipher` (GCM) uses AES-NI on amd64 automatically.
- `golang.org/x/crypto/curve25519` for X25519.
- `golang.org/x/crypto/hkdf` for key derivation.
- Reuse `cipher.AEAD` instance across packets (it's stateless; nonce is
  passed per call).

### Tasks

- [x] Spec section: §4.5 "End-to-End Encryption" — full wire format,
      nonce construction, key exchange flow, payload extensions, security
      properties, performance budget
- [x] Packet type: SESSION_ACCEPT (0x0A)
- [x] Flags bit: 0x04 = Encrypted
- [x] SESSION_REJECT reason code: 0x06 ENCRYPTION_UNSUPPORTED
- [x] Appendix A: encrypted MaxPayload, cipher constants
- [x] Handshake flow: 0-RTT (unencrypted) / 1-RTT (encrypted)
- [x] Payload extensions: SESSION_REQ, PUSH_REQ, PUSH_ACCEPT, PULL_REQ
- [ ] **C implementation:** sender-side encrypt path (keygen → HKDF → GCM seal)
- [ ] **C implementation:** receiver-side decrypt path (GCM open → FEC decode)
- [ ] **C implementation:** SESSION_ACCEPT send/recv in handshake
- [ ] **Go implementation:** sender-side encrypt path
- [ ] **Go implementation:** receiver-side decrypt path
- [ ] **Go implementation:** SESSION_ACCEPT send/recv in handshake
- [ ] CLI flag: `-encrypt` to enable (default: off for backward compat)
- [ ] Update MaxPayload constant (1368 → 1352 when encrypted)
- [ ] Update FEC tail block math for reduced payload
- [ ] Integration test: encrypted transfer through lossy proxy
- [ ] Integration test: unencrypted client → encrypted server (expect ENCRYPTION_UNSUPPORTED)
- [ ] Integration test: encrypted push/pull via serve daemon
- [ ] Benchmark: measure throughput delta with/without encryption

---

## 2. Resumable Transfers (v5.1)

**Why:** Aspera resumes interrupted multi-GB transfers from the last
checkpoint. On unreliable WAN links (satellite, cellular), restarting a
4 GB transfer at 60% wastes hours of bandwidth. This is the single most
impactful usability gap.

### Design Sketch

The core idea: the receiver periodically checkpoints `HighestContiguous`
and the file hash up to that point. On reconnect, the receiver sends a
RESUME_REQ containing the checkpoint, and the sender seeks to that offset.

### New Packet Types

- `0x0B RESUME_REQ` — Client → Server. Payload: original `xxHash64` (8B),
  `FileSize` (8B), `ResumeOffset` (8B = HighestContiguous × MaxPayload),
  `PartialHash` (8B = xxHash64 of bytes 0..ResumeOffset), `FileName`
  (null-terminated). If encrypted: `PubKey` (32B) inserted before `FileName`.
- `0x0C RESUME_ACCEPT` — Server → Client. Payload: confirmed
  `ResumeSequenceNum` (8B). If encrypted: `PubKey` (32B) appended.
  Sender begins from this sequence.

### Checkpoint Mechanism

- Receiver writes a `.hpuft-resume` sidecar file alongside the `.tmp`
  transfer file. Contents: `SessionID`, `HighestContiguous`,
  `PartialHash`, `FileSize`, `FullHash`, `FileName`.
- Sidecar is updated every N seconds (default: 5) or every 1% of file
  progress, whichever comes first.
- On receiver startup, if a `.hpuft-resume` file exists for the requested
  filename with matching `FileSize` and `FullHash`, the receiver sends
  `RESUME_REQ` instead of waiting for a fresh `SESSION_REQ`.

### Integrity Verification

- The sender computes xxHash64 over bytes 0..ResumeOffset from the source
  file and compares against the receiver's `PartialHash`. If they don't
  match (file was modified between sessions), the sender rejects the resume
  and the receiver falls back to a full transfer.
- On full transfer completion, the normal full-file hash verification (§8)
  still runs. Resume does not weaken integrity guarantees.

### Interaction with Encryption

- Resume generates a new SessionID and new ephemeral keys. The resumed
  session is cryptographically independent from the original — there is
  no key material to persist across sessions (forward secrecy preserved).
- RESUME_REQ and RESUME_ACCEPT carry public keys when the Encrypted flag
  is set, following the same pattern as SESSION_REQ/SESSION_ACCEPT.

### Edge Cases

- **Sender-side file modification:** PartialHash mismatch catches this.
- **Receiver-side corruption:** The `.tmp` file is validated against
  PartialHash on resume. If the tmp file is shorter than ResumeOffset
  (truncated crash), the receiver adjusts ResumeOffset down to the actual
  file size and recomputes PartialHash.
- **SessionID reuse:** Resume generates a new SessionID. The old one is
  irrelevant — resume is identified by filename + hash, not session.

### Tasks

- [ ] Spec section: write §7.C "Resumable Transfers"
- [ ] Define RESUME_REQ (0x0B) and RESUME_ACCEPT (0x0C) wire format
- [ ] Receiver: checkpoint sidecar writer (periodic + progress-based)
- [ ] Receiver: detect existing sidecar on startup, send RESUME_REQ
- [ ] Sender: validate PartialHash, seek source file, begin from offset
- [ ] Sender: handle RESUME_REQ in serve daemon flow
- [ ] Integration test: kill transfer at 50%, resume, verify hash
- [ ] Integration test: kill encrypted transfer, resume with new keys
- [ ] Integration test: modify source file between sessions, verify rejection
- [ ] Cleanup: delete sidecar on successful TRANSFER_COMPLETE

---

## 3. Fair Congestion Mode (v5.2)

**Why:** Aspera has a "fair" mode that detects competing TCP flows and
yields bandwidth. Without this, HP-UDP will starve TCP traffic on shared
links. Enterprise IT won't deploy a protocol that kills everything else
on the WAN.

### Design Sketch

The problem: HP-UDP's loss-driven CC doesn't distinguish between loss
caused by its own congestion and loss caused by competing flows. On a
shared link, HP-UDP fills the pipe, TCP backs off (AIMD), HP-UDP sees
less loss and increases further — a positive feedback loop that starves
TCP entirely.

### Approach: Delay-Based Yield

Add an optional RTT-inflation detector alongside the existing loss-driven
CC. When enabled (`-fair` flag), the CC monitors RTT trends:

- **Baseline RTT:** Minimum observed RTT over a sliding window (last 30
  seconds). This approximates the unloaded path latency.
- **RTT inflation ratio:** `current_smoothed_RTT / baseline_RTT`.
- **Yield trigger:** If inflation ratio > 2.0 AND loss < 1% (i.e., the
  link is buffering heavily but not yet dropping), the sender voluntarily
  reduces rate by 10% per RTT. This mimics what TCP would do if it
  detected the same buffering.
- **Recovery:** When inflation ratio drops below 1.5, normal CC resumes.

### Why Not TCP-Friendly Rate Control (TFRC)?

TFRC (RFC 5348) computes a TCP-equivalent rate from loss and RTT. It's
principled but requires accurate loss event rate measurement, which is
noisy on paths with FEC (recovered packets aren't "lost" from HP-UDP's
perspective but would be from TCP's). The delay-based approach is simpler
and doesn't need to model TCP's exact behavior — it just needs to yield
when the link is saturated.

### Interaction with Existing CC

- Fair mode is **additive** — it layers on top of the existing Phase 1/2
  loss-driven controller. The loss-driven CC still handles actual packet
  loss. Fair mode only activates in the zone where the link is buffering
  but not yet dropping (the zone where HP-UDP would otherwise keep pushing
  while TCP backs off).
- When fair mode triggers a yield, it sets a `fairYielding` flag. The
  Phase 1/2 increase logic is suppressed while this flag is set.
- The flag clears when RTT inflation drops below the recovery threshold.

### Tasks

- [ ] Spec section: write §6.E "Fair Congestion Mode"
- [ ] Implement baseline RTT tracker (sliding window minimum)
- [ ] Implement RTT inflation ratio computation in CC
- [ ] Add yield logic: 10% reduction per RTT when inflation > 2.0
- [ ] Add recovery logic: resume increases when inflation < 1.5
- [ ] CLI flag: `-fair` to enable (default: off for backward compat)
- [ ] Test: run HP-UDP + iperf3 TCP simultaneously on shaped link,
      measure TCP throughput with and without `-fair`
- [ ] Tune thresholds: 2.0 yield / 1.5 recovery may need adjustment
      based on real-world testing

---

## 4. Multi-File Transfers (v5.3)

**Why:** Aspera handles directory trees as a single transfer operation.
HP-UDP is single-file. For any real-world use case involving datasets,
log archives, or deployment artifacts, multi-file is expected.

### Design Sketch

Two approaches, in order of complexity:

### Option A: Manifest-Based Sequential (Simpler)

- New packet type `0x0D MULTI_SESSION_REQ`. Payload: file count (4B),
  total size (8B), manifest hash (8B), then for each file:
  `FileSize` (8B) + `FileHash` (8B) + `FileName` (null-terminated).
  If encrypted: `PubKey` (32B) after manifest hash, before file entries.
- Sender transfers files sequentially over the same session. Between
  files, the sender sends a `FILE_BOUNDARY` control packet (new type
  `0x0E`) containing the next file's index and size.
- The CC state, FEC encoder cache, and sliding window persist across
  files — no ramp-up penalty between files.
- The receiver writes each file independently, verifying per-file hash
  on each boundary.
- **Advantage:** Simple. Reuses all existing machinery. CC stays warm.
- **Disadvantage:** Small files serialize. A directory of 10,000 1KB
  files transfers like a 10 MB single file (fine) but each file
  boundary requires a hash verification round-trip.

### Option B: Tar-Stream (Simplest, Good Enough)

- Sender tars the directory into a stream, transfers it as a single
  file, receiver untars on completion.
- **Advantage:** Zero protocol changes. Works today.
- **Disadvantage:** No per-file integrity. No per-file resume. Receiver
  must wait for full transfer before any file is usable.

### Recommendation

Start with **Option A** (manifest-based sequential). It's a clean
protocol extension that preserves per-file integrity, enables per-file
resume (combine with §2), and keeps the CC warm across boundaries.
Option B is a CLI convenience feature that can coexist.

### Tasks

- [ ] Spec section: write §12 "Multi-File Transfers"
- [ ] Define MULTI_SESSION_REQ (0x0D) wire format (manifest in payload)
- [ ] Define FILE_BOUNDARY (0x0E) wire format
- [ ] Sender: iterate manifest, send files sequentially, emit boundary
- [ ] Receiver: parse manifest, create output directory, handle boundaries
- [ ] Per-file hash verification at each boundary
- [ ] CC state persistence across file boundaries (no reset)
- [ ] FEC block group numbering: reset per file or continuous?
      (Recommend: reset per file — simpler, and the tail block of file N
      won't have parity packets that span into file N+1)
- [ ] CLI: `hpuft send -dir ./mydir` or `hpuft push -dir ./mydir`
- [ ] CLI: `hpuft get -file manifest.json` to pull multiple files
- [ ] Integration test: transfer directory tree, verify all files
- [ ] Integration test: resume mid-directory transfer
- [ ] Serve daemon: handle multi-file push/pull

---

## Dependency Order

```
v5.0 Encryption ──► v5.1 Resume ──► v5.3 Multi-File
  (SPEC DONE)            │
                         ▼
                   v5.2 Fair CC (independent, can parallel with Resume)
```

Encryption first because it changes MaxPayload and wire format — everything
downstream must account for the 16-byte auth tag. Resume before multi-file
because multi-file resume depends on the checkpoint mechanism. Fair CC is
independent and can be developed in parallel with anything.

## Packet Type Registry (v5.0+)

| Code | Name | Introduced |
|------|------|-----------|
| 0x00 | SESSION_REQ | v1.0 |
| 0x01 | DATA | v1.0 |
| 0x02 | PARITY | v1.0 |
| 0x03 | HEARTBEAT | v1.0 |
| 0x04 | SESSION_REJECT | v1.0 |
| 0x05 | TRANSFER_COMPLETE | v1.0 |
| 0x06 | ACK_CLOSE | v1.0 |
| 0x07 | PULL_REQ | v1.0 |
| 0x08 | PUSH_REQ | v1.0 |
| 0x09 | PUSH_ACCEPT | v1.0 |
| 0x0A | SESSION_ACCEPT | v5.0 |
| 0x0B | RESUME_REQ | v5.1 (planned) |
| 0x0C | RESUME_ACCEPT | v5.1 (planned) |
| 0x0D | MULTI_SESSION_REQ | v5.3 (planned) |
| 0x0E | FILE_BOUNDARY | v5.3 (planned) |

---

## Out of Scope (Not Protocol-Level)

These are product/deployment features, not protocol gaps. Listed here to
explicitly exclude them from the protocol roadmap:

- Web UI / orchestration dashboard
- S3/cloud storage backend integration
- Multi-node clustering / relay servers
- FIPS 140-2 crypto validation (use a validated OpenSSL build)
- Bandwidth scheduling / QoS policies
- User authentication / access control (network-layer concern in target environments)