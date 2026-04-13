# HP-UDP Go Refactor Guide: v5.2 → v6.0

> **Revision history**
> - v1: initial draft (assumed generic C-style source layout)
> - v2 (this doc): cross-referenced against the actual Go codebase; corrected
>   type declarations, function names, file roles, and the sender/receiver
>   integration model (both use a `Config` struct, not a free function).

## Source Map

```
hpuft/
├── cmd/hpuft/
│   ├── main.go          ← CLI entry point — UPDATE command routing
│   ├── send.go          ← DELETE (direct send mode removed)
│   ├── recv.go          ← DELETE (direct recv-listens mode removed)
│   ├── push.go          ← DELETE (replaced by put.go)
│   ├── get.go           ← REWRITE (connection-based GET)
│   ├── list.go          ← DELETE (replaced by ls.go)
│   ├── serve.go         ← REFACTOR (connection dispatch + new ops)
│   ├── servers.go       ← UNCHANGED (master-tracker *query* CLI — not session
│   │                     tracking; the v1 doc was wrong about this file)
│   ├── progress.go      ← UNCHANGED
│   ├── tui.go           ← UNCHANGED (consumes sender/receiver Progress structs)
│   ├── proxy.go         ← UNCHANGED
│   ├── test.go          ← REWRITE test harness (send/recv topology removed)
│   └── NEW FILES:
│       ├── put.go       ← One-shot PUT (replaces send.go + push.go)
│       ├── rm.go        ← One-shot DELETE
│       ├── connect.go   ← Shell mode REPL
│       ├── ls.go        ← One-shot LIST (replaces list.go)
│       └── connections.go ← Server-side connection table (split out of serve.go)
│
├── protocol/
│   ├── types.go         ← REFACTOR (new packet type constants, drop Encrypted flag)
│   ├── header.go        ← REFACTOR (SessionID → ConnectionID)
│   ├── header_test.go   ← UPDATE for new constants and field name
│   ├── crypto.go        ← REFACTOR (nonce domain separation, HKDF info "v6")
│   ├── fec.go           ← UNCHANGED
│   ├── fec_test.go      ← UNCHANGED
│   ├── gf256.go         ← UNCHANGED
│   ├── payload.go       ← REFACTOR (drop v5 payload types, add Request/Response)
│   ├── payload_test.go  ← REWRITE (old SessionReq/Heartbeat tests, add Request/Response)
│   └── NEW FILES:
│       └── connection.go ← Connection struct, Dial/Accept, PING/PONG, Request/Response crypto
│
├── sender/              ← DATA PLANE — minimal edits
│   ├── sender.go        ← Field rename SessionID → ConnectionID in Config; remove
│   │                     in-flow SESSION_REQ/SESSION_ACCEPT key-exchange paths
│   │                     (Case 3 & 4 in current code). Pre-derived EncKey+IVBase
│   │                     path already exists and becomes the *only* path.
│   ├── congestion.go           ← UNCHANGED
│   ├── congestion_test.go      ← UNCHANGED
│   ├── calibration.go          ← UNCHANGED
│   ├── calibration_test.go     ← UNCHANGED
│   ├── fec_sender.go           ← UNCHANGED
│   ├── sliding_window.go       ← UNCHANGED
│   ├── sliding_window_test.go  ← UNCHANGED
│   └── cc_sim_test.go          ← UNCHANGED
│
├── receiver/            ← DATA PLANE — minimal edits
│   ├── receiver.go      ← Same as sender: rename field, drop in-flow
│   │                     SESSION_REQ reception (IncomingSession path already
│   │                     exists and becomes the only path).
│   ├── buffer.go               ← UNCHANGED
│   ├── buffer_test.go          ← UNCHANGED
│   ├── checkpoint.go           ← UNCHANGED
│   ├── fec_receiver.go         ← UNCHANGED
│   ├── fec_receiver_test.go    ← UNCHANGED
│   ├── heartbeat.go            ← UNCHANGED
│   ├── writer.go               ← UNCHANGED
│   └── writer_test.go          ← UNCHANGED
│
├── integration/
│   └── transfer_test.go ← REWRITE (serve + put + get loopback)
│
├── tests/
│   ├── test_hpuft.py    ← UPDATE: drop send/recv test, map push→put, list→ls, add rm
│   ├── run_tests.bat    ← UNCHANGED (wraps Python)
│   └── run_tests.sh     ← UNCHANGED (wraps Python)
│
└── test_basic.bat       ← UPDATE: push→put, get path unchanged, filename verbs
```

**Impact summary:** `sender/` and `receiver/` internals (17 files) are frozen
apart from a one-line field rename and the removal of their in-flow key-exchange
branches (they already accept pre-derived keys via `Config.EncKey` + `Config.IVBase`
— that path becomes the only one). Real refactor is in `cmd/hpuft/` (CLI layer)
and `protocol/` (wire format + new connection layer).

### Viability callouts (things the v1 doc got wrong or missed)

1. **`cmd/hpuft/servers.go`** is the CLI subcommand that **queries the master
   tracker** for the list of registered daemons. It has nothing to do with
   per-session state on the daemon. Don't touch it. The server-side connection
   table is a new concern in `serve.go` and should be split into a new file
   `connections.go` — *not* hijack `servers.go`.
2. `sender.Send()` and `receiver.Run()` are methods on structs configured via a
   `Config` struct. There is no `RunSender(...)` free function. The refactor is
   a **Config field rename + removal of branches**, not a signature change.
3. Protocol functions are `MarshalHeader`/`UnmarshalHeader` and
   `MarshalPacket`/`UnmarshalPacket`, not `SerializeHeader`/`ParseHeader`.
4. Packet-type constants are **typed** (`PacketType uint8`), not bare `uint8`.
   Flag constants are **typed** (`Flag uint8`). Declarations must preserve the
   type or the existing `String()` method and flag-mask sites stop compiling.
5. The current sender *already* supports a pre-derived `EncKey`/`IVBase` path
   (Config fields of those names). The v6 refactor removes the SESSION_REQ and
   SESSION_ACCEPT branches (cases 3 & 4 in `sender.go`), leaving only that path.
6. Existing payload types to delete include `SessionReqPayload`, `PullReqPayload`,
   `PushReqPayload`, `PushAcceptPayload`, `PullAcceptPayload`,
   `SessionAcceptPayload`, `ResumeReqPayload`, `ResumeAcceptPayload`, and all
   their Marshal/Unmarshal pairs.
7. The server needs an explicit **`AcceptConnection`** path (HELLO ingestion + WELCOME
   emission + key derivation) that mirrors `DialConnection`; this was missing from v1.
8. Sending a Request is unreliable over raw UDP — **`SendRequest` must retransmit**
   on timeout, not just wait 5 s and fail. Spec the retry policy explicitly.
9. The integration harness `cmd/hpuft/test.go` relies on `send`/`recv`/`proxy`.
   Since `send` and `recv` are being deleted, the harness must be rewritten to
   drive `serve` + `put` + `get` instead of just "updating verbs".

---

## Phase 0: Protocol Constants — The Typed Constant Trap

The single most common way a refactor like this breaks the build is by
declaring the new packet-type constants as bare `uint8`. The existing codebase
has:

```go
type PacketType uint8
const ( PacketData PacketType = 0x01; /* ... */ )
func (p PacketType) String() string { /* switch over all values */ }
```

Many call sites use `pkt.Header.Type == protocol.PacketData` — those compare
`PacketType` to `PacketType`. Declaring new constants as `const PacketData uint8 = 0x05`
silently breaks those comparisons (typed vs. untyped). **Keep the `PacketType`
typed declaration.** Same for `Flag`.

---

## Phase 1: Packet Type Rename and Header Changes

### 1.1 Update `protocol/types.go` — packet type constants

Replace the `PacketType` block:

```go
type PacketType uint8

const (
    // Connection layer
    PacketHello    PacketType = 0x00  // was PacketSessionReq (0x00)
    PacketWelcome  PacketType = 0x01  // was PacketSessionAccept (0x0A)
    PacketRequest  PacketType = 0x02  // NEW
    PacketResponse PacketType = 0x03  // NEW
    PacketReject   PacketType = 0x04  // was PacketSessionReject (0x04)

    // Data layer (renumbered from v5)
    PacketData      PacketType = 0x05  // was 0x01
    PacketParity    PacketType = 0x06  // was 0x02
    PacketHeartbeat PacketType = 0x07  // was 0x03
    PacketComplete  PacketType = 0x08  // was PacketTransferComplete (0x05)
    PacketAckClose  PacketType = 0x09  // was PacketACKClose (0x06)

    // Keepalive
    PacketPing PacketType = 0x0A  // NEW
    PacketPong PacketType = 0x0B  // NEW
)

// Update the String() method to cover the new values, drop the old ones.
func (p PacketType) String() string {
    switch p {
    case PacketHello:     return "HELLO"
    case PacketWelcome:   return "WELCOME"
    case PacketRequest:   return "REQUEST"
    case PacketResponse:  return "RESPONSE"
    case PacketReject:    return "REJECT"
    case PacketData:      return "DATA"
    case PacketParity:    return "PARITY"
    case PacketHeartbeat: return "HEARTBEAT"
    case PacketComplete:  return "COMPLETE"
    case PacketAckClose:  return "ACK_CLOSE"
    case PacketPing:      return "PING"
    case PacketPong:      return "PONG"
    default:              return "UNKNOWN"
    }
}
```

**DELETE** these constants (and their `String()` cases):
`PacketSessionReq`, `PacketSessionReject` (replaced by `PacketReject`),
`PacketTransferComplete`, `PacketACKClose`, `PacketPullReq`, `PacketPushReq`,
`PacketPushAccept`, `PacketSessionAccept`, `PacketResumeReq`, `PacketResumeAccept`,
`PacketListReq`, `PacketListResp`.

### 1.2 Add operation/status/reason constants

Inside REQUEST payloads:

```go
type OpCode uint8
const (
    OpPut    OpCode = 0x01
    OpGet    OpCode = 0x02
    OpList   OpCode = 0x03
    OpDelete OpCode = 0x04
)
```

RESPONSE status byte:

```go
type ResponseStatus uint8
const (
    StatusOK    ResponseStatus = 0x00
    StatusError ResponseStatus = 0x01
)
```

Reason codes (replaces `RejectReason` — rename for consistency or keep; either
is fine as long as all sites update):

```go
type Reason uint8
const (
    ReasonConnectionIDCollision Reason = 0x01  // was RejectSessionIDCollision
    ReasonHashMismatch          Reason = 0x02
    ReasonServerBusy            Reason = 0x03
    ReasonFileNotFound          Reason = 0x04
    ReasonFileExists            Reason = 0x05
    ReasonClientDisconnect      Reason = 0x06  // was 0x08 in v5
    ReasonInvalidRequest        Reason = 0x07
    ReasonDeleteDenied          Reason = 0x08
)
```

**DELETE** `RejectEncryptionUnsupported` and `RejectResumeHashMismatch` — no
plaintext mode and no resume in v6.

### 1.3 Rename `SessionID` → `ConnectionID`

In `protocol/header.go`, change the struct field:

```go
type Header struct {
    Type               PacketType
    ConnectionID       uint32  // was SessionID
    SequenceNum        uint64
    BlockGroup         uint64
    PayloadLen         uint16
    Flags              Flag
    SenderTimestampNs  uint64
}
```

And update the marshal/unmarshal:

```go
// MarshalHeader:
binary.BigEndian.PutUint32(dst[1:5], h.ConnectionID)

// UnmarshalHeader:
ConnectionID: binary.BigEndian.Uint32(src[1:5]),
```

Then do a repo-wide rename `SessionID` → `ConnectionID` across every Go file
(including `sender/`, `receiver/`, `cmd/hpuft/`, tests). Rename `Config.SessionID`
in `sender/sender.go` and `receiver/receiver.go`. Rename the `sessionID`
locals in `cmd/hpuft/*.go`.

Also rename `SessionConfig.StaleIDReservation` → `ConnectionIDReservation`
(semantically the same: how long a torn-down ID stays reserved to prevent
immediate reuse collisions).

### 1.4 Drop the `FlagEncrypted` flag

In `protocol/types.go` keep `Flag` typed and drop the encrypted bit:

```go
type Flag uint8
const (
    FlagEndOfFile        Flag = 0x01
    FlagCalibrationBurst Flag = 0x02
    // DELETED: FlagEncrypted = 0x04 — v6 is always encrypted
)
```

Update `MaxPayload` accounting. Everything on the wire is AES-128-GCM:

```go
const (
    HeaderSize = 32
    MTUHardCap = 1400
    MaxPayload = MTUHardCap - HeaderSize - GCMTagSize // 1352 bytes
)
```

Delete the separate `MaxEncryptedPayload` in `protocol/crypto.go` (or keep
it as an alias for source compatibility, but `MaxPayload` becomes the
canonical post-tag value).

Remove every `if flags & FlagEncrypted != 0` branch (search: `FlagEncrypted`).
In `sender/sender.go` and `receiver/receiver.go`, drop the `Encrypt` config
field and the code paths guarded by it — v6 is unconditionally encrypted.

---

## Phase 2: Connection Layer

### 2.1 `protocol/crypto.go` — nonce domain separation + HKDF bump

Change `BuildNonce` to separate the request layer from the data layer with the
top nonce bit:

```go
// OLD:
// func BuildNonce(ivBase [8]byte, seq uint64) [12]byte
//
// NEW:
func BuildNonce(ivBase [8]byte, counter uint64, requestLayer bool) [12]byte {
    var nonce [12]byte
    copy(nonce[:8], ivBase[:])
    low31 := uint32(counter & 0x7FFFFFFF)
    if requestLayer {
        low31 |= 0x80000000
    }
    binary.BigEndian.PutUint32(nonce[8:], low31)
    return nonce
}
```

**Nonce-space caveat.** Halving the counter space to 2³¹ = ~2.1 billion per
domain means a single transfer of >~2.9 TB (at 1352-byte payloads) would reuse a
nonce on the data layer. Document this and keep it as a known limit for v6; if
a larger transfer is ever needed, rekey mid-stream.

Update every existing call site to pass `false` (data layer):
- `sender/sender.go`: all `protocol.BuildNonce(ivBase, seqNum)` → `protocol.BuildNonce(ivBase, seqNum, false)`
- `receiver/receiver.go`: same
- `sender/fec_sender.go` if it builds nonces

HKDF info bump in `DeriveSessionKey`:

```go
// OLD: "hp-udp-aes128-v5"
// NEW: "hp-udp-aes128-v6"
okm, err := hkdf.Key(sha256.New, shared, salt[:], "hp-udp-aes128-v6", 24)
```

`GenerateEphemeralKey`, `NewSessionCipher`, `EncryptPacket`, `DecryptPacket`
all stay unchanged.

### 2.2 New file `protocol/connection.go`

```go
package protocol

import (
    "crypto/cipher"
    "crypto/ecdh"
    "crypto/rand"
    "encoding/binary"
    "errors"
    "fmt"
    "net"
    "sync"
    "sync/atomic"
    "time"
)

const (
    ClientIDSize      = 32
    ConnIdleTimeout   = 30 * time.Second
    HelloRetryTimeout = 2 * time.Second
    HelloMaxRetries   = 3

    RequestRetryTimeout = 2 * time.Second
    RequestMaxRetries   = 3
)

type ConnState int32
const (
    ConnHandshaking ConnState = iota
    ConnIdle
    ConnTransferring
    ConnClosing
)

// Connection holds the long-lived, per-client state shared by the control
// plane (Request/Response) and the data plane (sender/receiver).
type Connection struct {
    ID         uint32
    RemoteAddr *net.UDPAddr
    Conn       *net.UDPConn // shared mux socket owned by caller

    SessionKey [16]byte
    IVBase     [8]byte
    AEAD       cipher.AEAD

    state         atomic.Int32
    nextRequestID atomic.Uint64 // starts at 1
    idleTimerMu   sync.Mutex
    idleTimer     *time.Timer

    // Server-side: pending response demux. Client-side: unused.
    pending   map[uint64]chan []byte
    pendingMu sync.Mutex
}

func (c *Connection) State() ConnState       { return ConnState(c.state.Load()) }
func (c *Connection) SetState(s ConnState)   { c.state.Store(int32(s)) }
func (c *Connection) NextRequestID() uint64  { return c.nextRequestID.Add(1) }

// --- Client-side dial ---

// DialConnection performs the v6 HELLO/WELCOME handshake on an already-bound
// UDP socket. The caller retains socket ownership.
func DialConnection(conn *net.UDPConn, serverAddr *net.UDPAddr, clientID string) (*Connection, error) {
    if len(clientID) > ClientIDSize {
        return nil, fmt.Errorf("clientID too long: %d > %d", len(clientID), ClientIDSize)
    }

    var idBytes [4]byte
    if _, err := rand.Read(idBytes[:]); err != nil {
        return nil, fmt.Errorf("gen connection id: %w", err)
    }
    connectionID := binary.BigEndian.Uint32(idBytes[:])

    priv, err := GenerateEphemeralKey()
    if err != nil {
        return nil, fmt.Errorf("ephemeral key: %w", err)
    }

    payload := make([]byte, PubKeySize+ClientIDSize)
    copy(payload[:PubKeySize], priv.PublicKey().Bytes())
    copy(payload[PubKeySize:], []byte(clientID)) // null-padded by default

    hdr := Header{
        Type:         PacketHello,
        ConnectionID: connectionID,
        PayloadLen:   uint16(len(payload)),
    }
    pkt := Packet{Header: hdr, Payload: payload}
    raw, err := MarshalPacket(&pkt)
    if err != nil {
        return nil, fmt.Errorf("marshal HELLO: %w", err)
    }

    buf := make([]byte, MTUHardCap)
    for attempt := 0; attempt < HelloMaxRetries; attempt++ {
        if _, err := conn.WriteToUDP(raw, serverAddr); err != nil {
            return nil, fmt.Errorf("send HELLO: %w", err)
        }
        conn.SetReadDeadline(time.Now().Add(HelloRetryTimeout))
        for {
            n, from, err := conn.ReadFromUDP(buf)
            if err != nil {
                if isTimeout(err) { break } // retry HELLO
                conn.SetReadDeadline(time.Time{})
                return nil, fmt.Errorf("read WELCOME: %w", err)
            }
            p, perr := UnmarshalPacket(buf[:n])
            if perr != nil || p.Header.ConnectionID != connectionID {
                continue
            }
            switch p.Header.Type {
            case PacketReject:
                conn.SetReadDeadline(time.Time{})
                return nil, rejectError(p.Payload)
            case PacketWelcome:
                if len(p.Payload) < PubKeySize {
                    continue
                }
                conn.SetReadDeadline(time.Time{})
                key, iv, err := DeriveSessionKey(priv, p.Payload[:PubKeySize], connectionID)
                if err != nil {
                    return nil, fmt.Errorf("derive session key: %w", err)
                }
                aead, err := NewSessionCipher(key)
                if err != nil {
                    return nil, fmt.Errorf("init cipher: %w", err)
                }
                c := &Connection{
                    ID:         connectionID,
                    RemoteAddr: from,   // server may respond from a different src if NATted
                    Conn:       conn,
                    SessionKey: key,
                    IVBase:     iv,
                    AEAD:       aead,
                }
                c.SetState(ConnIdle)
                return c, nil
            }
        }
    }
    return nil, errors.New("HELLO timeout: no WELCOME after retries")
}

// --- Server-side accept ---

// AcceptConnection derives the session key from a received HELLO payload and
// builds the server side of the connection. Caller is responsible for
// generating the WELCOME packet using SendWelcome / returning the raw bytes.
func AcceptConnection(conn *net.UDPConn, remote *net.UDPAddr, connectionID uint32, helloPayload []byte) (*Connection, []byte, error) {
    if len(helloPayload) < PubKeySize {
        return nil, nil, fmt.Errorf("HELLO payload too short: %d", len(helloPayload))
    }

    priv, err := GenerateEphemeralKey()
    if err != nil {
        return nil, nil, fmt.Errorf("ephemeral key: %w", err)
    }
    key, iv, err := DeriveSessionKey(priv, helloPayload[:PubKeySize], connectionID)
    if err != nil {
        return nil, nil, fmt.Errorf("derive session key: %w", err)
    }
    aead, err := NewSessionCipher(key)
    if err != nil {
        return nil, nil, fmt.Errorf("init cipher: %w", err)
    }

    welcomePayload := make([]byte, PubKeySize)
    copy(welcomePayload, priv.PublicKey().Bytes())

    hdr := Header{
        Type:         PacketWelcome,
        ConnectionID: connectionID,
        PayloadLen:   uint16(len(welcomePayload)),
    }
    welcomeRaw, err := MarshalPacket(&Packet{Header: hdr, Payload: welcomePayload})
    if err != nil {
        return nil, nil, fmt.Errorf("marshal WELCOME: %w", err)
    }

    c := &Connection{
        ID:         connectionID,
        RemoteAddr: remote,
        Conn:       conn,
        SessionKey: key,
        IVBase:     iv,
        AEAD:       aead,
    }
    c.SetState(ConnIdle)
    return c, welcomeRaw, nil
}

// --- Ping/Pong ---

func (c *Connection) SendPing() error {
    hdr := Header{Type: PacketPing, ConnectionID: c.ID}
    raw, err := MarshalPacket(&Packet{Header: hdr})
    if err != nil { return err }
    _, err = c.Conn.WriteToUDP(raw, c.RemoteAddr)
    return err
}

func (c *Connection) SendPong() error {
    hdr := Header{Type: PacketPong, ConnectionID: c.ID}
    raw, err := MarshalPacket(&Packet{Header: hdr})
    if err != nil { return err }
    _, err = c.Conn.WriteToUDP(raw, c.RemoteAddr)
    return err
}

func (c *Connection) KeepAlive(interval time.Duration, stop <-chan struct{}) {
    t := time.NewTicker(interval)
    defer t.Stop()
    for {
        select {
        case <-stop: return
        case <-t.C:  _ = c.SendPing()
        }
    }
}

// --- Request / Response crypto ---

// EncryptRequest produces a full wire packet for a REQUEST, encrypting the
// plaintext op-payload with the request-layer nonce and using the header as AAD.
// Returns the raw packet (header | ciphertext | tag).
func (c *Connection) EncryptRequest(reqID uint64, plaintext []byte) ([]byte, error) {
    return c.encryptFramed(PacketRequest, reqID, plaintext)
}

func (c *Connection) EncryptResponse(reqID uint64, plaintext []byte) ([]byte, error) {
    return c.encryptFramed(PacketResponse, reqID, plaintext)
}

func (c *Connection) encryptFramed(t PacketType, reqID uint64, plaintext []byte) ([]byte, error) {
    hdr := Header{
        Type:         t,
        ConnectionID: c.ID,
        SequenceNum:  reqID,
        PayloadLen:   uint16(len(plaintext) + GCMTagSize),
    }
    raw := make([]byte, HeaderSize+len(plaintext))
    if _, err := MarshalHeader(raw, &hdr); err != nil {
        return nil, err
    }
    copy(raw[HeaderSize:], plaintext)
    nonce := BuildNonce(c.IVBase, reqID, true)
    return EncryptPacket(c.AEAD, raw, nonce), nil
}

// DecryptRequest / DecryptResponse authenticate and decrypt the payload of an
// incoming REQUEST / RESPONSE. Caller passes the full raw packet bytes.
func (c *Connection) DecryptRequest(raw []byte) ([]byte, error) {
    return c.decryptFramed(raw)
}
func (c *Connection) DecryptResponse(raw []byte) ([]byte, error) {
    return c.decryptFramed(raw)
}
func (c *Connection) decryptFramed(raw []byte) ([]byte, error) {
    if len(raw) < HeaderSize+GCMTagSize {
        return nil, errors.New("framed packet too short")
    }
    hdr, err := UnmarshalHeader(raw)
    if err != nil { return nil, err }
    nonce := BuildNonce(c.IVBase, hdr.SequenceNum, true)
    return DecryptPacket(c.AEAD, raw, nonce)
}

// --- Request/Response RPC over UDP ---

// SendRequest is the client-side one-shot: encrypt, send, wait for matching
// RESPONSE. Retransmits up to RequestMaxRetries on timeout. The serve loop on
// the peer dedupes by (connectionID, requestID) so retransmit is idempotent.
//
// Returns the decrypted response payload (status byte + body).
func (c *Connection) SendRequest(plaintext []byte) ([]byte, error) {
    reqID := c.NextRequestID()
    raw, err := c.EncryptRequest(reqID, plaintext)
    if err != nil { return nil, err }

    buf := make([]byte, MTUHardCap)
    for attempt := 0; attempt < RequestMaxRetries; attempt++ {
        if _, err := c.Conn.WriteToUDP(raw, c.RemoteAddr); err != nil {
            return nil, fmt.Errorf("send request: %w", err)
        }
        deadline := time.Now().Add(RequestRetryTimeout)
        c.Conn.SetReadDeadline(deadline)
        for {
            n, _, err := c.Conn.ReadFromUDP(buf)
            if err != nil {
                if isTimeout(err) { break } // retry
                c.Conn.SetReadDeadline(time.Time{})
                return nil, err
            }
            hdr, herr := UnmarshalHeader(buf[:n])
            if herr != nil || hdr.ConnectionID != c.ID {
                continue
            }
            if hdr.Type == PacketPong { continue } // ignore idle keepalive
            if hdr.Type != PacketResponse || hdr.SequenceNum != reqID {
                continue
            }
            c.Conn.SetReadDeadline(time.Time{})
            return c.DecryptResponse(buf[:n])
        }
    }
    return nil, errors.New("request timeout: no RESPONSE after retries")
}

// --- Close ---

func (c *Connection) Close() {
    c.SetState(ConnClosing)
    // Send REJECT(ClientDisconnect) as a courtesy so the peer frees state now.
    hdr := Header{Type: PacketReject, ConnectionID: c.ID}
    pkt := Packet{Header: hdr, Payload: []byte{byte(ReasonClientDisconnect)}}
    if raw, err := MarshalPacket(&pkt); err == nil {
        c.Conn.WriteToUDP(raw, c.RemoteAddr)
    }
    // Zero session key material.
    for i := range c.SessionKey { c.SessionKey[i] = 0 }
}

// --- Internal helpers ---

func isTimeout(err error) bool {
    var ne net.Error
    return errors.As(err, &ne) && ne.Timeout()
}

func rejectError(payload []byte) error {
    if len(payload) == 0 {
        return errors.New("rejected by peer (no reason)")
    }
    return fmt.Errorf("rejected by peer: reason=0x%02x", payload[0])
}
```

### 2.3 `protocol/payload.go` — delete old, add new

**Delete** these types and all their marshal/unmarshal pairs:
`SessionReqPayload`, `PullReqPayload`, `PushReqPayload`, `PushAcceptPayload`,
`PullAcceptPayload`, `SessionAcceptPayload`, `ResumeReqPayload`,
`ResumeAcceptPayload`. Also delete `MarshalListResp` / `UnmarshalListResp` —
the LIST response payload stays the same tab-separated format but is now
carried inside a RESPONSE body, so you may keep the helpers under new names
(`MarshalListBody` / `UnmarshalListBody`) to avoid re-implementing the line
packer.

**Keep**: `MarshalHeartbeat`, `UnmarshalHeartbeat` — unchanged. Heartbeats are
part of the data plane and their layout is stable.

**Add** request-layer builders. Note: request bodies are the *plaintext* that
gets passed to `Connection.EncryptRequest`; they do NOT carry a pubkey
anymore (that's in HELLO).

```go
// BuildPutRequest — op=0x01
// Layout: OpPut(1) | FileSize(8) | FileHash(8) | InitialRate(4) | FileName(null-term)
func BuildPutRequest(fileSize, fileHash uint64, initialRate uint32, fileName string) []byte

// BuildGetRequest — op=0x02
// Layout: OpGet(1) | FileName(null-term)
func BuildGetRequest(fileName string) []byte

// BuildListRequest — op=0x03
// Layout: OpList(1)
func BuildListRequest() []byte

// BuildDeleteRequest — op=0x04
// Layout: OpDelete(1) | FileName(null-term)
func BuildDeleteRequest(fileName string) []byte

// ParseRequest returns (opcode, body, error). Server uses this inside
// serve.handleRequest() after DecryptRequest.
func ParseRequest(plaintext []byte) (OpCode, []byte, error)
```

Response builders:

```go
// Success bodies for each op:
// - OpPut, OpDelete: StatusOK(1)
// - OpGet:            StatusOK(1) | FileSize(8) | FileHash(8) | InitialRate(4)
// - OpList:           StatusOK(1) | (list body, same tab-separated format)
//
// Error body for every op:
//   StatusError(1) | Reason(1) | optional message(null-term)

func BuildGetResponseOK(fileSize, fileHash uint64, initialRate uint32) []byte
func BuildPutResponseOK() []byte
func BuildListResponseOK(body []byte) []byte
func BuildDeleteResponseOK() []byte
func BuildResponseError(reason Reason, msg string) []byte

// ParseResponse returns (status, reason, body, error). body is the post-status
// payload for StatusOK, or empty for StatusError.
func ParseResponse(plaintext []byte) (ResponseStatus, Reason, []byte, error)
```

Update `payload_test.go` to cover round-trips for each new builder.

---

## Phase 3: Sender / Receiver — minimum-effort retrofit

The data plane is already designed around pre-derived key material. The v6
integration touches only a handful of lines.

### 3.1 `sender/Config` (in `sender/sender.go`)

- Rename `Config.SessionID` → `Config.ConnectionID`.
- **Delete** `Config.Encrypt` and `Config.PeerPubKey` — unused in v6.
- **Delete** `Config.PushFlow` — now the only flow.
- Keep `Config.EncKey`, `Config.IVBase`, `Config.MuxConn`, `Config.MuxAddr`,
  `Config.RecvChan` — they're how the CLI hands the pre-established session in.

In `Send()`:
- Delete the SESSION_REQ build/write block entirely (currently wrapped in
  `if !s.cfg.PushFlow { ... }`). v6 sender starts at "Step 4: Open file and
  prepare send state" unconditionally.
- Delete Cases 3 (pull flow: `PeerPubKey != nil`) and 4 (direct: wait for
  SESSION_ACCEPT).
- Keep Case 2 (pre-derived key via `EncKey`/`IVBase`) — it becomes the only
  encryption path. The `EncKey` and `IVBase` must now always be non-nil; add
  a guard at the top of `Send()`:

  ```go
  if s.cfg.EncKey == nil || s.cfg.IVBase == nil {
      return errors.New("v6 sender requires pre-derived EncKey and IVBase")
  }
  ```

- Every `protocol.BuildNonce(ivBase, seqNum)` → `protocol.BuildNonce(ivBase, seqNum, false)`.
- Every `hdr.Flags |= protocol.FlagEncrypted` → **delete**.
- Header field `SessionID: sessionID` → `ConnectionID: s.cfg.ConnectionID`
  (after the rename, plus remove the `sessionID := s.cfg.SessionID; if sessionID == 0 { ... }`
  local — it's never zero in v6).

### 3.2 `receiver/Config` (in `receiver/receiver.go`)

Symmetric:
- Rename `Config.SessionID` and the `IncomingSession.SessionID` field →
  `ConnectionID`.
- Delete `Config.Encrypt`. `EncKey`/`IVBase` become mandatory.
- The `IncomingSession` struct is the only entry path in v6. Delete the `else if r.cfg.RecvChan != nil` / SESSION_REQ-wait code and the direct-bind SESSION_REQ wait (if present). The serve daemon constructs an `IncomingSession` after receiving the PUT request, exactly as it does today for the PUSH flow.
- Update `SendDisconnect`: packet type is `PacketReject`, reason is
  `ReasonClientDisconnect`.
- Every `protocol.BuildNonce(ivBase, seq)` → `protocol.BuildNonce(ivBase, seq, false)`.
- `IncomingSession.Req` field previously of type `SessionReqPayload` must be
  replaced since that type is deleted. Introduce a small new struct:

  ```go
  type TransferMeta struct {
      FileName    string
      FileSize    uint64
      FileHash    uint64
      InitialRate uint32
  }
  type IncomingSession struct {
      SenderAddr   *net.UDPAddr
      ConnectionID uint32
      Meta         TransferMeta
  }
  ```

  Grep `cfg.IncomingSession.Req.` to find call sites; each becomes `.Meta.`.

Neither `sender.Send()` nor `receiver.Run()` takes a different arity — the call
sites from the CLI are the same pattern: build `Config`, `.New(cfg)`, invoke.

---

## Phase 4: CLI Refactor — `cmd/hpuft/`

The current CLI uses flag-parsed subcommands (e.g., `runSend(args)` takes
`args []string` and parses with `flag.NewFlagSet`). Keep that idiom.

### 4.1 `cmd/hpuft/main.go`

Replace dispatch + usage:

```go
switch cmd {
case "serve":   runServe(args)
case "put":     runPut(args)
case "get":     runGet(args)
case "ls":      runLs(args)
case "rm":      runRm(args)
case "connect": runConnect(args)
case "servers": runServers(args)  // UNCHANGED: master-tracker query
case "proxy":   runProxy(args)
case "test":    runTest(args)
default:        usage(); os.Exit(1)
}
```

Update the `usage()` help text accordingly. Drop `send`, `recv`, `push`, `list`.

### 4.2 Put/Get/Ls/Rm CLIs

Each new `run*` function follows the same skeleton. Example for `put.go`:

```go
func runPut(args []string) {
    fs := flag.NewFlagSet("put", flag.ExitOnError)
    serveAddr := fs.String("addr", "127.0.0.1:9001", "serve daemon address")
    filePath  := fs.String("file", "", "file to upload (required)")
    rateMBps  := fs.Float64("rate", 0, "initial send rate in MB/s")
    clientID  := fs.String("id", "", "optional client identifier (≤32 bytes)")
    debug     := fs.Bool("debug", false, "protocol/CC telemetry on stderr")
    fs.Parse(args)
    // ...

    conn, err := net.ListenUDP("udp", &net.UDPAddr{})   // ephemeral local port
    // (set read/write buffers)
    rAddr, _ := net.ResolveUDPAddr("udp", *serveAddr)

    pc, err := protocol.DialConnection(conn, rAddr, *clientID)
    if err != nil { /* fatal */ }

    fileSize, fileHash := statAndHash(*filePath)
    reqBody := protocol.BuildPutRequest(fileSize, fileHash, uint32(*rateMBps*1e6), filepath.Base(*filePath))
    respRaw, err := pc.SendRequest(reqBody)
    if err != nil { /* fatal */ }
    status, reason, _, _ := protocol.ParseResponse(respRaw)
    if status != protocol.StatusOK { /* fatal with reason */ }

    pc.SetState(protocol.ConnTransferring)

    sCfg := sender.DefaultConfig()
    sCfg.FilePath       = *filePath
    sCfg.RemoteAddr     = *serveAddr // for logging
    sCfg.ConnectionID   = pc.ID
    sCfg.MuxConn        = conn
    sCfg.MuxAddr        = rAddr
    sCfg.EncKey         = &pc.SessionKey
    sCfg.IVBase         = &pc.IVBase
    sCfg.Debug          = *debug
    // sCfg.RecvChan: leave nil — put is single-transfer so the sender owns the socket reads.

    s := sender.New(sCfg)
    if err := s.Send(); err != nil { /* fatal */ }

    pc.SetState(protocol.ConnIdle)
    pc.Close()
}
```

`runGet` / `runLs` / `runRm` follow the same pattern: Dial → SendRequest →
ParseResponse → (optionally) receiver.New / println / etc. → Close.

For GET the steps are:
- SendRequest(BuildGetRequest(filename))
- ParseResponse → FileSize/FileHash/InitialRate
- Construct `receiver.IncomingSession{SenderAddr: pc.RemoteAddr, ConnectionID: pc.ID, Meta: ...}`
- `receiver.New(cfg).Run()` with `Conn: conn`, `EncKey: &pc.SessionKey`, `IVBase: &pc.IVBase`.

### 4.3 `cmd/hpuft/connect.go` — REPL

```go
func runConnect(args []string) {
    fs := flag.NewFlagSet("connect", flag.ExitOnError)
    serveAddr := fs.String("addr", "127.0.0.1:9001", "serve daemon address")
    clientID  := fs.String("id", "", "optional client identifier")
    fs.Parse(args)

    // ... dial ...
    stop := make(chan struct{})
    go pc.KeepAlive(10*time.Second, stop)

    sc := bufio.NewScanner(os.Stdin)
    fmt.Fprint(os.Stdout, "hpuft> ")
    for sc.Scan() {
        fields := strings.Fields(sc.Text())
        if len(fields) == 0 { continue }
        switch fields[0] {
        case "ls":       shellLs(pc)
        case "get":      shellGet(pc, fields[1:])
        case "put":      shellPut(pc, fields[1:], conn, rAddr)
        case "rm":       shellRm(pc, fields[1:])
        case "exit","quit": goto done
        default:         fmt.Println("commands: ls | get <file> [-o dir] | put <path> | rm <file> | exit")
        }
        fmt.Fprint(os.Stdout, "hpuft> ")
    }
done:
    close(stop)
    pc.Close()
}
```

Each `shell*` helper is the single-shot CLI body minus the Dial/Close. For
`shellPut`/`shellGet`, the sender/receiver goroutine needs a `RecvChan` instead
of owning the socket, since the REPL loop reads in parallel for keepalive
handling. The REPL controller runs a single socket reader that routes packets
by type (PONG → drop, DATA/PARITY/HEARTBEAT → active transfer chan, RESPONSE →
pending RPC map) — same two-level dispatch as the server.

### 4.4 Server refactor — split into `serve.go` + `connections.go`

**`cmd/hpuft/connections.go`** (new file):

```go
package main

import (
    "hpuft/protocol"
    "net"
    "sync"
    "time"
)

type connKey struct {
    Addr string
    ID   uint32
}

type connTable struct {
    mu      sync.RWMutex
    byKey   map[connKey]*serverConn
}

// serverConn wraps protocol.Connection with server-side lifetime state.
type serverConn struct {
    *protocol.Connection
    LastActivity time.Time
    // The two server-side transfer channels are attached when a PUT/GET
    // begins; cleared when it ends.
    TransferCh chan []byte
}

func newConnTable() *connTable { return &connTable{byKey: map[connKey]*serverConn{}} }

func (t *connTable) get(addr *net.UDPAddr, id uint32) *serverConn {
    t.mu.RLock(); defer t.mu.RUnlock()
    return t.byKey[connKey{addr.String(), id}]
}

func (t *connTable) put(sc *serverConn) {
    t.mu.Lock(); defer t.mu.Unlock()
    t.byKey[connKey{sc.RemoteAddr.String(), sc.ID}] = sc
}

func (t *connTable) remove(addr *net.UDPAddr, id uint32) {
    t.mu.Lock(); defer t.mu.Unlock()
    delete(t.byKey, connKey{addr.String(), id})
}

// reapIdle evicts connections idle for more than protocol.ConnIdleTimeout.
// Called from a background ticker in runServe.
func (t *connTable) reapIdle(now time.Time) { /* walk + delete */ }
```

Leave `cmd/hpuft/servers.go` untouched.

**`cmd/hpuft/serve.go`** — two-level dispatch. The overall control loop
replaces the current `runServe` main loop:

```go
func runServe(args []string) {
    // ... flag parsing unchanged (listen, dir, debug, master) ...

    manifest, manifestMu := buildManifest(*dir)
    conns := newConnTable()

    // Busy model: v5 was single-lane. Keep that for v6 — it preserves CC
    // correctness (one transfer ≡ one CC state). Multiple connections may
    // coexist idle; only one may be TRANSFERRING at a time.
    var busy atomic.Int32
    var busyClient string

    go func() {
        t := time.NewTicker(5 * time.Second)
        defer t.Stop()
        for range t.C { conns.reapIdle(time.Now()) }
    }()

    rawBuf := make([]byte, protocol.MTUHardCap)
    for {
        n, addr, err := conn.ReadFromUDP(rawBuf)
        if err != nil { continue }

        hdr, herr := protocol.UnmarshalHeader(rawBuf[:n])
        if herr != nil { continue }

        switch hdr.Type {
        case protocol.PacketHello:
            handleHello(conn, addr, hdr, rawBuf[:n], conns)
            continue
        case protocol.PacketPing:
            if sc := conns.get(addr, hdr.ConnectionID); sc != nil {
                sc.SendPong()
                sc.LastActivity = time.Now()
            }
            continue
        }

        sc := conns.get(addr, hdr.ConnectionID)
        if sc == nil { continue } // drop unknown
        sc.LastActivity = time.Now()

        switch hdr.Type {
        case protocol.PacketRequest:
            handleRequest(sc, rawBuf[:n], conn, *dir, manifest, manifestMu, &busy, &busyClient, *debug)
        case protocol.PacketReject:
            conns.remove(addr, hdr.ConnectionID)

        case protocol.PacketData, protocol.PacketParity,
             protocol.PacketHeartbeat, protocol.PacketComplete,
             protocol.PacketAckClose:
            // Forward to the active transfer goroutine through sc.TransferCh.
            ch := sc.TransferCh
            if ch == nil { continue }
            raw := make([]byte, n); copy(raw, rawBuf[:n])
            select {
            case ch <- raw:
            default: /* drop; sender/receiver will retransmit */
            }
        }
    }
}
```

`handleHello` calls `protocol.AcceptConnection`, inserts into `conns`, writes
the WELCOME bytes it returns. On ConnectionID collision, send `PacketReject`
with `ReasonConnectionIDCollision` and don't insert.

`handleRequest` calls `sc.DecryptRequest(raw)` then `protocol.ParseRequest`,
and dispatches by opcode. The OpPut/OpGet handlers are refactors of the
existing `handlePushReq` / `handlePullReq`:

- **OpPut**: base-name sanitization, busy check, overwrite check, send an
  OpPut-style RESPONSE OK, construct `receiver.IncomingSession`, spawn the
  transfer goroutine, set `sc.SetState(ConnTransferring)`.
- **OpGet**: manifest lookup, busy check, send GET RESPONSE OK with
  FileSize/FileHash/InitialRate, spawn sender goroutine with
  `MuxConn`/`MuxAddr`/`EncKey`/`IVBase` wired to `sc`.
- **OpList**: read-lock manifest, build tab-separated body, send as
  RESPONSE OK — no busy check needed.
- **OpDelete**: base-name sanitization, write-lock manifest, `os.Remove`,
  delete from manifest, RESPONSE OK. No busy check (manifest is independent
  of in-flight transfers).

**Important:** when a transfer completes or aborts, the goroutine must
`sc.SetState(ConnIdle)`, clear `sc.TransferCh`, and `busy.Store(0)` so the
connection survives for the next request. This is the key behavior change
vs. v5, where teardown destroyed the session.

### 4.5 Progress/TUI — no code changes

`RunSendTUI` / `RunRecvTUI` consume `*sender.Sender` / `*receiver.Receiver`
instances. As long as `sender.Config`/`receiver.Config` still produce those
types via `.New(cfg)`, the TUI is unaffected.

---

## Phase 5: Remove Dead Code

Do after the build passes and the CLI round-trips `put`/`get`/`ls`/`rm`:

- [ ] Delete `cmd/hpuft/send.go`
- [ ] Delete `cmd/hpuft/recv.go`
- [ ] Delete `cmd/hpuft/push.go`
- [ ] Delete `cmd/hpuft/list.go`
- [ ] `protocol/payload.go`: remove `SessionReqPayload`, `PullReqPayload`,
      `PushReqPayload`, `PushAcceptPayload`, `PullAcceptPayload`,
      `SessionAcceptPayload`, `ResumeReqPayload`, `ResumeAcceptPayload`, and
      their `Marshal*`/`Unmarshal*` pairs. Constants `SessionReqFixedSize`,
      `SessionReqEncFixedSize`, `PushReqFixedSize`, `PushReqEncFixedSize`,
      `PushAcceptEncFixedSize`, `ResumeReqFixedSize`, `ResumeReqEncFixedSize`,
      `ResumeAcceptFixedSize`, `ResumeAcceptEncFixedSize`, `SessionAcceptSize`.
- [ ] `protocol/types.go`: remove `FlagEncrypted`, remove any
      `MaxPayloadUnencrypted`, remove `RejectEncryptionUnsupported`,
      `RejectResumeHashMismatch`, and the old `PacketType` constants.
- [ ] `protocol/crypto.go`: drop `MaxEncryptedPayload` or mark as alias.
- [ ] `sender/sender.go`: `Config.Encrypt`, `Config.PeerPubKey`,
      `Config.PushFlow`, the SESSION_REQ build block, cases 3 & 4.
- [ ] `receiver/receiver.go`: `Config.Encrypt`, the SESSION_REQ wait path, any
      `SESSION_ACCEPT` sender logic.
- [ ] Remove or rename `SessionConfig.StaleIDReservation` in `types.go` if it
      stays; nothing references session reuse in v6.

---

## Phase 6: Test Updates

### 6.1 `protocol/header_test.go`
- Swap `SessionID: ...` → `ConnectionID: ...` in every test case literal.
- Swap `PacketSessionReq` → `PacketHello` (or drop, since the test is just
  round-tripping a header, the type value is arbitrary).
- Everything else round-trips unchanged.

### 6.2 `protocol/payload_test.go`
- Delete `TestSessionReqRoundTrip` and `TestSessionReqTooShort`.
- Keep the HEARTBEAT tests as-is (that payload is unchanged).
- Add: round-trip tests for each OpCode builder (`BuildPutRequest`,
  `BuildGetRequest`, `BuildListRequest`, `BuildDeleteRequest`,
  `BuildGetResponseOK`, `BuildResponseError`, `ParseRequest`, `ParseResponse`).

### 6.3 NEW `protocol/connection_test.go`
- **Handshake over loopback**: two goroutines on the same UDP socket (or two
  sockets on ephemeral ports); server calls `AcceptConnection`, client calls
  `DialConnection`; assert both derive the same `SessionKey`.
- **HELLO retry**: drop first HELLO, verify retry succeeds.
- **REJECT on collision**: pre-populate a conn table, dial with the same ID,
  expect a reject error.
- **Nonce domain separation**: `BuildNonce(iv,5,true) != BuildNonce(iv,5,false)`
  bitwise.
- **Request round-trip crypto**: `EncryptRequest` → `DecryptRequest` with the
  same `Connection` returns the original plaintext; with a different key fails
  GCM auth.

### 6.4 `integration/transfer_test.go` — REWRITE

Drop the raw `sender.Send()` / `receiver.Run()` loopback pair (their v5
signatures bundled the key-exchange into `Send()`/`Run()`; in v6 there's no
standalone-mode receiver to point at). Instead, spin up a `serve` goroutine and
drive it end-to-end:

```go
func TestServeLoopbackPutGet(t *testing.T) {
    dir := t.TempDir()
    // Generate a random 1MB file at dir/src.bin
    // Start runServe equivalent on a goroutine bound to :0 with dir/
    // Client: Dial → SendRequest(BuildPutRequest) → sender.Send()
    // Client: Dial (fresh conn) → SendRequest(BuildGetRequest) → receiver.Run()
    // Hash compare src vs. received
}
```

Also add:
- `TestConnectionKeepsAliveAfterTransfer`: one Dial, two PUT calls, assert the
  same `Connection` survives.
- `TestIdleTimeout`: Dial, sleep past `ConnIdleTimeout`, expect server to have
  evicted the connection (next request → `ReasonConnectionIDCollision`-ish
  behavior or drop).
- `TestConnectionSurvivesFailedTransfer`: PUT that aborts mid-stream; the
  connection should still accept a new LS or RM request.

### 6.5 `cmd/hpuft/test.go` — REWRITE integration harness

The current harness launches `recv`, then `send` directly (optionally via
`proxy`). In v6 there's no `recv`, so replace the topology:

- Launch `serve` bound to a temp dir with ephemeral port.
- Optionally launch `proxy` between the client and `serve` to inject loss.
- For each test file + loss%: run `put` (through the proxy if present), then
  `get` to a separate output dir, hash-compare against the source.
- Drop the `-listen`/`-out` receiver args; drop `-nodelay` (not a v6 flag —
  pacing is implicit in the new sender config).

Keep the results table and summary untouched.

### 6.6 `tests/test_hpuft.py` — UPDATE

Mapping:

| v5 call                                 | v6 call                                   |
|-----------------------------------------|-------------------------------------------|
| `hpuft send  -file F -addr A`           | **removed** — no direct send in v6        |
| `hpuft recv  -listen L -out O`          | **removed** — no direct recv in v6        |
| `hpuft push  -addr A -file F [-encrypt]`| `hpuft put  -addr A -file F`              |
| `hpuft list  -addr A`                   | `hpuft ls   -addr A`                      |
| `hpuft get   -addr A -file N -out O`    | `hpuft get  -addr A -file N -out O`       |
| (no equivalent)                         | `hpuft rm   -addr A -file N` (**new**)    |

Concrete edits in `test_hpuft.py`:
- Delete `test_send_recv(...)` and its call in `main()`.
- Rename `test_serve_push_list_get` → `test_serve_put_ls_get_rm`.
- Replace every `"push"` → `"put"`, `"list"` → `"ls"`.
- Drop every `-encrypt` flag (v6 is unconditional encryption).
- Add an `rm` test: put → ls (confirm present) → rm → ls (confirm gone) → get
  (confirm FILE_NOT_FOUND).

### 6.7 `test_basic.bat` — UPDATE

- `%EXE% push` → `%EXE% put`
- `%EXE% get` unchanged flag surface, keep as-is
- Drop the `-encrypt` paths (single encrypted mode now). Either remove the
  encrypted half of the script, or leave one copy as a regression sanity.

---

## What Does NOT Change

Do not touch during this refactor:

- `sender/sender.go` internals apart from (a) the `Config` field rename,
  (b) deletion of the SESSION_REQ / SESSION_ACCEPT branches, (c) the
  `BuildNonce` argument addition, (d) the `FlagEncrypted` removal.
- `sender/congestion.go`, `sender/congestion_test.go`, `sender/cc_sim_test.go`
  — loss-driven CC, phased growth, EWMA, auto-ceiling.
- `sender/calibration.go`, `sender/calibration_test.go` — burst logic,
  dispersion.
- `sender/fec_sender.go` — RS encode, block grouping, parity ratio.
- `sender/sliding_window.go` — ring buffer, IsFull/Advance.
- `receiver/receiver.go` internals apart from the same four edit classes as
  sender.
- `receiver/buffer.go` — ring buffer, contiguous tracking.
- `receiver/heartbeat.go` — payload construction, loss rate, NACK array.
- `receiver/fec_receiver.go` — RS decode, block pool, recovery.
- `receiver/writer.go` — disk flush, fsync.
- `receiver/checkpoint.go` — sidecar format, resume (the on-disk resume
  format is unaffected even though the `RESUME_REQ` packet type is gone —
  resume within a connection is simply a fresh PUT request with offset info
  inside the body; leave that to a future v6.1 if desired).
- `protocol/fec.go`, `protocol/gf256.go` — GF(2⁸) math.
- `protocol/header.go` marshal/unmarshal function signatures — only the
  `Header` struct field rename changes.
- `protocol/heartbeat` payload — unchanged.
- `cmd/hpuft/progress.go` — progress bar, repair state.
- `cmd/hpuft/tui.go` — terminal UI.
- `cmd/hpuft/proxy.go` — lossy UDP proxy.
- `cmd/hpuft/servers.go` — master-tracker query CLI (unrelated to session
  state).
- `tests/run_tests.bat`, `tests/run_tests.sh` — they just wrap the Python
  suite; no verb-specific logic to update.

---

## Recommended Execution Order (for agents working from this doc)

Build must pass at every step, so interleave small verifications:

1. **Phase 1.1–1.3** (constants, header field rename) — compile. Many type
   errors will surface; fix them mechanically.
2. **Phase 1.4** (drop `FlagEncrypted`) — compile.
3. **Phase 2.1** (`BuildNonce` signature + HKDF info bump) — compile. Tests
   will still fail because payloads are mid-flight.
4. **Phase 2.3** (payload builders: add new, keep old side-by-side
   temporarily so sender/receiver still compile during transition). Build.
5. **Phase 3** (sender/receiver edits). Build.
6. **Phase 2.2** (new `connection.go`). Build + run `connection_test.go`.
7. **Phase 4.1–4.3** (new CLI verbs: `put`, `get`, `ls`, `rm`, `connect`).
   These can call into a still-v5 serve for incremental testing only if we
   *don't* delete the old serve until step 8 — otherwise do steps 7 and 8
   together.
8. **Phase 4.4** (serve refactor). End-to-end loopback should pass now.
9. **Phase 5** (delete dead code). Build should still pass.
10. **Phase 6** (tests). Update/rewrite tests, run the suite.
