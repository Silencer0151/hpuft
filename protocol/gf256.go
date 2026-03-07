package protocol

// GF(2^8) Galois Field arithmetic using the irreducible polynomial
// x^8 + x^4 + x^3 + x^2 + 1 (0x11d). This is the same polynomial
// used by AES and is the standard choice for Reed-Solomon implementations.

const (
	gfFieldSize = 256
	gfPoly      = 0x11d // x^8 + x^4 + x^3 + x^2 + 1
)

var (
	// gfExp maps log values to field elements: gfExp[i] = alpha^i
	gfExp [512]byte // doubled for convenience in multiplication

	// gfLog maps field elements to log values: gfLog[gfExp[i]] = i
	// gfLog[0] is undefined (log of zero doesn't exist)
	gfLog [256]byte
)

func init() {
	// Build log/exp tables using generator alpha = 2
	x := 1
	for i := 0; i < 255; i++ {
		gfExp[i] = byte(x)
		gfExp[i+255] = byte(x) // duplicate for wrap-around convenience
		gfLog[x] = byte(i)
		x <<= 1
		if x >= gfFieldSize {
			x ^= gfPoly
		}
	}
	// gfLog[0] left as 0, but must never be used (caller guards against zero)
}

// gfAdd returns a + b in GF(2^8). Addition in GF(2^8) is XOR.
func gfAdd(a, b byte) byte {
	return a ^ b
}

// gfSub returns a - b in GF(2^8). Subtraction equals addition in GF(2^8).
func gfSub(a, b byte) byte {
	return a ^ b
}

// gfMul returns a * b in GF(2^8).
func gfMul(a, b byte) byte {
	if a == 0 || b == 0 {
		return 0
	}
	return gfExp[int(gfLog[a])+int(gfLog[b])]
}

// gfDiv returns a / b in GF(2^8). Panics if b is zero.
func gfDiv(a, b byte) byte {
	if b == 0 {
		panic("gf256: division by zero")
	}
	if a == 0 {
		return 0
	}
	return gfExp[int(gfLog[a])-int(gfLog[b])+255]
}

// gfInv returns the multiplicative inverse of a in GF(2^8). Panics if a is zero.
func gfInv(a byte) byte {
	if a == 0 {
		panic("gf256: inverse of zero")
	}
	return gfExp[255-int(gfLog[a])]
}

// gfPow returns a^n in GF(2^8).
func gfPow(a byte, n int) byte {
	if n == 0 {
		return 1
	}
	if a == 0 {
		return 0
	}
	logA := int(gfLog[a])
	result := (logA * n) % 255
	if result < 0 {
		result += 255
	}
	return gfExp[result]
}
