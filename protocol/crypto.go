package protocol

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

const (
	// GCMTagSize is the length of the AES-GCM authentication tag appended to
	// each encrypted DATA or PARITY packet after the ciphertext.
	GCMTagSize = 16

	// PubKeySize is the length of an X25519 public key on the wire.
	PubKeySize = 32
)

// GenerateEphemeralKey generates a fresh X25519 keypair for one session.
// The caller must zero the private key after session teardown.
func GenerateEphemeralKey() (*ecdh.PrivateKey, error) {
	return ecdh.X25519().GenerateKey(rand.Reader)
}

// DeriveSessionKey performs X25519 ECDH + HKDF-SHA256 to produce a 16-byte
// AES-128 session key and an 8-byte iv_base. sessionID is used as the HKDF
// salt so that the same keypair yields a distinct key for every session.
//
// HKDF parameters:
//
//	hash  = SHA-256
//	ikm   = X25519 shared secret (32 bytes)
//	salt  = sessionID (4 bytes big-endian)
//	info  = "hp-udp-aes128-v6"
//	L     = 24 bytes  (bytes 0-15 = AES-128 key, bytes 16-23 = iv_base)
func DeriveSessionKey(priv *ecdh.PrivateKey, theirPubBytes []byte, sessionID uint32) ([16]byte, [8]byte, error) {
	peerPub, err := ecdh.X25519().NewPublicKey(theirPubBytes)
	if err != nil {
		return [16]byte{}, [8]byte{}, fmt.Errorf("bad peer public key: %w", err)
	}
	shared, err := priv.ECDH(peerPub)
	if err != nil {
		return [16]byte{}, [8]byte{}, fmt.Errorf("ecdh: %w", err)
	}

	var salt [4]byte
	binary.BigEndian.PutUint32(salt[:], sessionID)

	okm, err := hkdf.Key(sha256.New, shared, salt[:], "hp-udp-aes128-v6", 24)
	if err != nil {
		return [16]byte{}, [8]byte{}, fmt.Errorf("hkdf: %w", err)
	}
	var key [16]byte
	var ivBase [8]byte
	copy(key[:], okm[:16])
	copy(ivBase[:], okm[16:24])
	return key, ivBase, nil
}

// NewSessionCipher creates an AES-128-GCM AEAD from a derived session key.
// The AEAD instance is stateless and reused across packets; the nonce is
// passed per-call by EncryptPacket / DecryptPacket.
func NewSessionCipher(key [16]byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// BuildNonce constructs the 12-byte packet nonce per spec §4.5C.
//
//	Bytes 0–7:  iv_base (8 bytes, HKDF-derived, session-scoped)
//	Bytes 8–11: low 31 bits of counter, big-endian; bit 31 set iff requestLayer
//
// The top bit separates the request/response control plane (requestLayer=true)
// from the data plane (requestLayer=false), giving each domain 2³¹ (~2.1B)
// unique nonces. A single transfer of more than ~2.9 TB at 1352-byte payloads
// would exhaust the data-layer nonce space; rekey mid-stream if needed.
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

// EncryptPacket encrypts the DATA or PARITY payload of a pre-assembled wire
// packet. raw must be [header(32)][plaintext(N)]; the returned slice is
// [header(32)][ciphertext(N)][tag(16)].
//
// PayloadLen in the output header is set to N+GCMTagSize so the receiver can
// locate the full ciphertext+tag region using the header alone. The AAD is the
// updated 32-byte header (with the corrected PayloadLen), so both sides derive
// the same AAD without any out-of-band knowledge.
func EncryptPacket(aead cipher.AEAD, raw []byte, nonce [12]byte) []byte {
	plaintext := raw[HeaderSize:]
	// Copy the header and update PayloadLen to include the tag before sealing,
	// so the AAD matches what the receiver will see in the wire header.
	var hdr [HeaderSize]byte
	copy(hdr[:], raw[:HeaderSize])
	binary.BigEndian.PutUint16(hdr[21:23], uint16(len(plaintext)+GCMTagSize))
	sealed := aead.Seal(nil, nonce[:], plaintext, hdr[:]) // AAD = updated header; len(sealed) = N+16
	out := make([]byte, HeaderSize+len(sealed))
	copy(out[:HeaderSize], hdr[:])
	copy(out[HeaderSize:], sealed)
	return out
}

// DecryptPacket decrypts and authenticates a received encrypted packet.
// raw must be [header(32)][ciphertext(N)][tag(16)].
// Returns the plaintext payload, or an error if authentication fails.
func DecryptPacket(aead cipher.AEAD, raw []byte, nonce [12]byte) ([]byte, error) {
	if len(raw) < HeaderSize+GCMTagSize {
		return nil, fmt.Errorf("encrypted packet too short: %d bytes", len(raw))
	}
	header := raw[:HeaderSize]
	ciphertextWithTag := raw[HeaderSize:]
	pt, err := aead.Open(nil, nonce[:], ciphertextWithTag, header)
	if err != nil {
		return nil, fmt.Errorf("gcm auth failed: %w", err)
	}
	return pt, nil
}
