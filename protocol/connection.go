package protocol

import (
	"crypto/cipher"
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

// ConnState is the lifecycle state of a Connection.
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

func (c *Connection) State() ConnState      { return ConnState(c.state.Load()) }
func (c *Connection) SetState(s ConnState)  { c.state.Store(int32(s)) }
func (c *Connection) NextRequestID() uint64 { return c.nextRequestID.Add(1) }

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
	copy(payload[PubKeySize:], []byte(clientID)) // remainder is zero-padded

	hdr := Header{
		Type:         PacketHello,
		ConnectionID: connectionID,
		PayloadLen:   uint16(len(payload)),
	}
	raw, err := MarshalPacket(&Packet{Header: hdr, Payload: payload})
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
				if isTimeout(err) {
					break // retry HELLO
				}
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
					RemoteAddr: from, // server may respond from different src if NATted
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
// builds the server side of the connection. It returns the Connection and the
// raw WELCOME packet bytes the caller must send back to the client.
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

// --- Ping / Pong ---

// SendPing sends a PING packet to the remote peer to keep the connection alive.
func (c *Connection) SendPing() error {
	hdr := Header{Type: PacketPing, ConnectionID: c.ID}
	raw, err := MarshalPacket(&Packet{Header: hdr})
	if err != nil {
		return err
	}
	_, err = c.Conn.WriteToUDP(raw, c.RemoteAddr)
	return err
}

// SendPong replies with a PONG packet.
func (c *Connection) SendPong() error {
	hdr := Header{Type: PacketPong, ConnectionID: c.ID}
	raw, err := MarshalPacket(&Packet{Header: hdr})
	if err != nil {
		return err
	}
	_, err = c.Conn.WriteToUDP(raw, c.RemoteAddr)
	return err
}

// KeepAlive sends a PING at the given interval until stop is closed.
// Intended to be run as a goroutine: go conn.KeepAlive(10*time.Second, stop).
func (c *Connection) KeepAlive(interval time.Duration, stop <-chan struct{}) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			_ = c.SendPing()
		}
	}
}

// --- Request / Response crypto ---

// EncryptRequest produces a full wire packet for a REQUEST, encrypting the
// plaintext op-payload with the request-layer nonce and using the header as
// AAD. Returns the raw packet bytes (header | ciphertext | tag).
func (c *Connection) EncryptRequest(reqID uint64, plaintext []byte) ([]byte, error) {
	return c.encryptFramed(PacketRequest, reqID, plaintext)
}

// EncryptResponse produces a full wire packet for a RESPONSE.
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

// DecryptRequest authenticates and decrypts the payload of an incoming REQUEST.
// Caller passes the full raw packet bytes.
func (c *Connection) DecryptRequest(raw []byte) ([]byte, error) {
	return c.decryptFramed(raw)
}

// DecryptResponse authenticates and decrypts the payload of an incoming RESPONSE.
func (c *Connection) DecryptResponse(raw []byte) ([]byte, error) {
	return c.decryptFramed(raw)
}

func (c *Connection) decryptFramed(raw []byte) ([]byte, error) {
	if len(raw) < HeaderSize+GCMTagSize {
		return nil, errors.New("framed packet too short")
	}
	hdr, err := UnmarshalHeader(raw)
	if err != nil {
		return nil, err
	}
	nonce := BuildNonce(c.IVBase, hdr.SequenceNum, true)
	return DecryptPacket(c.AEAD, raw, nonce)
}

// --- Request / Response RPC over UDP ---

// SendRequest is the client-side one-shot RPC: encrypt, send, wait for a
// matching RESPONSE. Retransmits up to RequestMaxRetries on timeout. The serve
// loop on the peer deduplicates by (connectionID, requestID) so retransmission
// is idempotent.
//
// Returns the decrypted response payload (status byte + body).
func (c *Connection) SendRequest(plaintext []byte) ([]byte, error) {
	reqID := c.NextRequestID()
	raw, err := c.EncryptRequest(reqID, plaintext)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, MTUHardCap)
	for attempt := 0; attempt < RequestMaxRetries; attempt++ {
		if _, err := c.Conn.WriteToUDP(raw, c.RemoteAddr); err != nil {
			return nil, fmt.Errorf("send request: %w", err)
		}
		c.Conn.SetReadDeadline(time.Now().Add(RequestRetryTimeout))
		for {
			n, _, err := c.Conn.ReadFromUDP(buf)
			if err != nil {
				if isTimeout(err) {
					break // retry
				}
				c.Conn.SetReadDeadline(time.Time{})
				return nil, err
			}
			hdr, herr := UnmarshalHeader(buf[:n])
			if herr != nil || hdr.ConnectionID != c.ID {
				continue
			}
			if hdr.Type == PacketPong {
				continue // ignore keepalive
			}
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

// Close transitions the connection to ConnClosing, sends a courtesy
// REJECT(ClientDisconnect) so the peer can release state immediately, and
// zeros the session key material.
func (c *Connection) Close() {
	c.SetState(ConnClosing)
	hdr := Header{Type: PacketReject, ConnectionID: c.ID}
	pkt := Packet{Header: hdr, Payload: []byte{byte(ReasonClientDisconnect)}}
	if raw, err := MarshalPacket(&pkt); err == nil {
		c.Conn.WriteToUDP(raw, c.RemoteAddr) //nolint:errcheck
	}
	// Zero session key material on close.
	for i := range c.SessionKey {
		c.SessionKey[i] = 0
	}
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
