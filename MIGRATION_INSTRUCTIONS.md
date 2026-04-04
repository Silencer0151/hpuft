# Go Client Migration: v5.1 to v5.2

This document covers every wire-level change between protocol v5.1 and v5.2.
The C server already implements v5.2 — your Go client must match.

---

## 1. Encryption: HKDF Now Derives 24 Bytes (not 16)

**v5.1:** `HKDF-SHA256(shared, salt, info) → 16 bytes` (AES key only). `iv_base` was random, generated locally at init.

**v5.2:** `HKDF-SHA256(shared, salt, info) → 24 bytes`
- Bytes `[0..15]` = AES-128 session key
- Bytes `[16..23]` = `iv_base` (deterministic, replaces random)

Both sides derive the same `iv_base` from the shared secret. No wire transmission of `iv_base` needed.

Parameters unchanged:
- IKM: X25519 shared secret (32 bytes)
- Salt: SessionID as 4 bytes big-endian
- Info: `"hp-udp-aes128-v5"` (16 bytes, no NUL)

**Go change:** In your HKDF call, request 24 bytes output instead of 16. Split: `key = okm[:16]`, `ivBase = okm[16:24]`.

---

## 2. Encryption: New GCM Nonce Construction

**v5.1:** 12-byte nonce = `SessionID(4B) || PacketType(1B) || UniqueID(7B)` (3-field scheme).

**v5.2:** 12-byte nonce = `iv_base(8B) || seq_low32_be(4B)`
- Bytes `[0..7]`: the HKDF-derived `iv_base` from change #1
- Bytes `[8..11]`: low 32 bits of `SequenceNum`, big-endian

```go
func buildNonce(ivBase [8]byte, seq uint64) [12]byte {
    var nonce [12]byte
    copy(nonce[:8], ivBase[:])
    binary.BigEndian.PutUint32(nonce[8:], uint32(seq))
    return nonce
}
```

AAD is still the 32-byte packet header (unchanged).

---

## 3. PUSH_REQ Payload: Added FileHash + InitialRate

**v5.1:** `FileSize(8B) + [PubKey(32B)] + FileName(NUL)`

**v5.2:** Mirrors SESSION_REQ layout:

| Unencrypted | Encrypted |
|---|---|
| `FileSize(8B)` | `FileSize(8B)` |
| `FileHash(8B)` | `FileHash(8B)` |
| `InitialRate(4B)` | `InitialRate(4B)` |
| `FileName(NUL)` | `PubKey(32B)` |
| | `FileName(NUL)` |

All fields big-endian. `FileHash` is xxHash64 of the full file. `InitialRate` is bytes/sec (0 = use calibration).

**Go change:** When building PUSH_REQ, insert `FileHash` (8B) and `InitialRate` (4B) after `FileSize`, before `PubKey`/`FileName`.

---

## 4. PUSH_ACCEPT Payload: Port Field Removed

**v5.1:** `Port(2B) + [PubKey(32B)]` — client had to reconnect to an ephemeral data port.

**v5.2:**
- Unencrypted: **no payload** (empty)
- Encrypted: `PubKey(32B)` only

The server uses a single shared socket. Data flows on the same address:port that received the PUSH_REQ.

**Go change:** Stop parsing the leading 2-byte port. Don't reconnect to a different port after PUSH_ACCEPT.

---

## 5. LIST_RESP Format: Now Tab-Separated with Size

**v5.1:** Each line was just a filename: `filename\n`

**v5.2:** Each line is: `filename\tsize\n`

`size` is the file's byte count as a decimal integer string.

```
backup.tar.gz	524288000
report.pdf	2097152
```

**Go change:** Split each line on `\t`. Field 0 = filename, field 1 = size (parse as uint64).

---

## 6. Resume: No Wire-Level RESUME_REQ/RESUME_ACCEPT

**v5.1:** Sender sent `RESUME_REQ` (type `0x0B`) before `SESSION_REQ`. Receiver replied with `RESUME_ACCEPT` (type `0x0C`, carrying `highest_contiguous`). Sender started from `highest_contiguous + 1`.

**v5.2:** Resume is transparent and receiver-side only.
- Sender always starts with a normal `SESSION_REQ` from sequence 0
- Receiver checks for a local `.hpudp-ckpt` sidecar matching the file hash
- If found, receiver restores its bitset and reports restored `HighestContiguous` in the first heartbeat
- Sender's sliding window naturally advances past already-received data

Packet types `0x0B` and `0x0C` are reserved but unused.

**Go change:** Remove all RESUME_REQ/RESUME_ACCEPT send/receive logic. Just send SESSION_REQ normally. The receiver handles resume internally. If your Go client is also a receiver, implement the `.hpudp-ckpt` sidecar (see checkpoint format below).

### Checkpoint Sidecar Format (`.hpudp-ckpt`, receiver-side only)

All fields little-endian (local file, not wire):

| Offset | Size | Field |
|---|---|---|
| 0 | 4B | Magic: `0x48505543` ("HPUC") |
| 4 | 4B | Version: `1` |
| 8 | 8B | `file_size` |
| 16 | 8B | `file_hash` (xxHash64) |
| 24 | 8B | `total_chunks` |
| 32 | 8B | `highest_contiguous` |
| 40 | var | `recv_bits`: `ceil(total_chunks / 8)` bytes |

Old `.hpuft-resume` files are obsolete.

---

## Summary Checklist

- [ ] HKDF output: 16 → 24 bytes; split into key(16) + iv_base(8)
- [ ] GCM nonce: `iv_base(8B) || seq_low32_be(4B)` replaces 3-field scheme
- [ ] PUSH_REQ: insert `FileHash(8B)` + `InitialRate(4B)` after `FileSize`
- [ ] PUSH_ACCEPT: remove `Port(2B)` parsing; stay on same socket
- [ ] LIST_RESP: parse `filename\tsize\n` instead of `filename\n`
- [ ] Resume: delete RESUME_REQ/RESUME_ACCEPT code; send normal SESSION_REQ
- [ ] If Go client is a receiver: implement `.hpudp-ckpt` sidecar
