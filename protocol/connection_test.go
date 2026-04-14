package protocol

import (
	"bytes"
	"fmt"
	"net"
	"testing"
	"time"
)

// makeTestConn creates a Connection with derived keys for unit tests that
// don't need a real UDP socket (Conn is nil — crypto operations only).
func makeTestConn(t *testing.T, connID uint32) *Connection {
	t.Helper()
	clientPriv, err := GenerateEphemeralKey()
	if err != nil {
		t.Fatalf("GenerateEphemeralKey client: %v", err)
	}
	serverPriv, err := GenerateEphemeralKey()
	if err != nil {
		t.Fatalf("GenerateEphemeralKey server: %v", err)
	}
	key, ivBase, err := DeriveSessionKey(clientPriv, serverPriv.PublicKey().Bytes(), connID)
	if err != nil {
		t.Fatalf("DeriveSessionKey: %v", err)
	}
	aead, err := NewSessionCipher(key)
	if err != nil {
		t.Fatalf("NewSessionCipher: %v", err)
	}
	c := &Connection{
		ID:         connID,
		SessionKey: key,
		IVBase:     ivBase,
		AEAD:       aead,
	}
	c.SetState(ConnIdle)
	return c
}

// TestNonceDomainSeparation verifies that requestLayer=true and false produce
// distinct nonces for the same ivBase and counter, with the expected bit set.
func TestNonceDomainSeparation(t *testing.T) {
	var ivBase [8]byte
	for i := range ivBase {
		ivBase[i] = byte(i + 1)
	}
	const counter = uint64(99)

	data := BuildNonce(ivBase, counter, false)
	ctrl := BuildNonce(ivBase, counter, true)

	if data == ctrl {
		t.Fatal("data-plane and request-layer nonces are identical for the same counter")
	}
	if !bytes.Equal(data[:8], ctrl[:8]) {
		t.Error("iv_base portion (bytes 0-7) should be identical in both nonces")
	}
	if data[8]&0x80 != 0 {
		t.Error("data nonce must not have bit 31 set (requestLayer=false)")
	}
	if ctrl[8]&0x80 == 0 {
		t.Error("control nonce must have bit 31 set (requestLayer=true)")
	}
}

// TestRequestCryptoRoundTrip verifies EncryptRequest→DecryptRequest and
// EncryptResponse→DecryptResponse are inverses using the same connection.
func TestRequestCryptoRoundTrip(t *testing.T) {
	c := makeTestConn(t, 0xDEAD1234)

	reqID := c.NextRequestID()
	plaintext := []byte("hello request payload")

	enc, err := c.EncryptRequest(reqID, plaintext)
	if err != nil {
		t.Fatalf("EncryptRequest: %v", err)
	}
	got, err := c.DecryptRequest(enc)
	if err != nil {
		t.Fatalf("DecryptRequest: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("DecryptRequest: got %q, want %q", got, plaintext)
	}

	respBody := []byte("hello response payload")
	encResp, err := c.EncryptResponse(reqID, respBody)
	if err != nil {
		t.Fatalf("EncryptResponse: %v", err)
	}
	gotResp, err := c.DecryptResponse(encResp)
	if err != nil {
		t.Fatalf("DecryptResponse: %v", err)
	}
	if !bytes.Equal(gotResp, respBody) {
		t.Fatalf("DecryptResponse: got %q, want %q", gotResp, respBody)
	}
}

// TestRequestTampering confirms that flipping a ciphertext byte causes
// DecryptRequest to return an authentication error.
func TestRequestTampering(t *testing.T) {
	c := makeTestConn(t, 0xBEEF0001)
	reqID := c.NextRequestID()
	enc, err := c.EncryptRequest(reqID, []byte("tamper me"))
	if err != nil {
		t.Fatalf("EncryptRequest: %v", err)
	}
	enc[HeaderSize] ^= 0xFF // flip a bit in the ciphertext
	if _, err := c.DecryptRequest(enc); err == nil {
		t.Fatal("expected authentication error after tampering, got nil")
	}
}

// TestHandshakeLoopback performs a full DialConnection/AcceptConnection
// loopback on real 127.0.0.1 UDP sockets and verifies both sides derive the
// same session key and can cross-encrypt/decrypt.
func TestHandshakeLoopback(t *testing.T) {
	srvConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP server: %v", err)
	}
	defer srvConn.Close()
	srvAddr := srvConn.LocalAddr().(*net.UDPAddr)

	cliConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP client: %v", err)
	}
	defer cliConn.Close()

	type srvResult struct {
		conn *Connection
		err  error
	}
	srvCh := make(chan srvResult, 1)

	// Server goroutine: receive one HELLO, derive key, emit WELCOME.
	go func() {
		buf := make([]byte, MTUHardCap)
		srvConn.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, from, err := srvConn.ReadFromUDP(buf)
		if err != nil {
			srvCh <- srvResult{err: fmt.Errorf("server read: %w", err)}
			return
		}
		p, err := UnmarshalPacket(buf[:n])
		if err != nil || p.Header.Type != PacketHello {
			srvCh <- srvResult{err: fmt.Errorf("expected HELLO, got type %v (err=%v)", p.Header.Type, err)}
			return
		}
		sc, welcomeRaw, err := AcceptConnection(srvConn, from, p.Header.ConnectionID, p.Payload)
		if err != nil {
			srvCh <- srvResult{err: fmt.Errorf("AcceptConnection: %w", err)}
			return
		}
		if _, err := srvConn.WriteToUDP(welcomeRaw, from); err != nil {
			srvCh <- srvResult{err: fmt.Errorf("send WELCOME: %w", err)}
			return
		}
		srvCh <- srvResult{conn: sc}
	}()

	cliPC, err := DialConnection(cliConn, srvAddr, "test-client")
	if err != nil {
		t.Fatalf("DialConnection: %v", err)
	}

	srvRes := <-srvCh
	if srvRes.err != nil {
		t.Fatalf("server handshake: %v", srvRes.err)
	}
	srvPC := srvRes.conn

	// Both sides must have derived identical session material.
	if srvPC.SessionKey != cliPC.SessionKey {
		t.Fatalf("session key mismatch:\n  server=%x\n  client=%x", srvPC.SessionKey, cliPC.SessionKey)
	}
	if srvPC.IVBase != cliPC.IVBase {
		t.Fatalf("iv_base mismatch:\n  server=%x\n  client=%x", srvPC.IVBase, cliPC.IVBase)
	}

	// Cross-decrypt: client encrypts a request, server decrypts it.
	reqID := cliPC.NextRequestID()
	msg := []byte("cross-connection crypto check")
	enc, err := cliPC.EncryptRequest(reqID, msg)
	if err != nil {
		t.Fatalf("client EncryptRequest: %v", err)
	}
	got, err := srvPC.DecryptRequest(enc)
	if err != nil {
		t.Fatalf("server DecryptRequest: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("cross-decrypt: got %q want %q", got, msg)
	}
}

// TestRejectOnCollision verifies that DialConnection propagates an error when
// the server responds with PacketReject.
func TestRejectOnCollision(t *testing.T) {
	srvConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP server: %v", err)
	}
	defer srvConn.Close()
	srvAddr := srvConn.LocalAddr().(*net.UDPAddr)

	// Server goroutine that always rejects the first packet.
	go func() {
		buf := make([]byte, MTUHardCap)
		srvConn.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, from, err := srvConn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		p, err := UnmarshalPacket(buf[:n])
		if err != nil {
			return
		}
		hdr := Header{
			Type:         PacketReject,
			ConnectionID: p.Header.ConnectionID,
			PayloadLen:   1,
		}
		raw, _ := MarshalPacket(&Packet{Header: hdr, Payload: []byte{byte(ReasonConnectionIDCollision)}})
		srvConn.WriteToUDP(raw, from) //nolint:errcheck
	}()

	cliConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP client: %v", err)
	}
	defer cliConn.Close()

	_, err = DialConnection(cliConn, srvAddr, "collide")
	if err == nil {
		t.Fatal("expected error from REJECT response, got nil")
	}
}
