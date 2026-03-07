package protocol

import (
	"errors"
	"fmt"
)

var (
	ErrTooFewShards  = errors.New("fec: not enough shards to reconstruct")
	ErrInvalidShards = errors.New("fec: shard count does not match encoder configuration")
	ErrShardSize     = errors.New("fec: all shards must be equal length")
)

// RSEncoder performs Reed-Solomon erasure coding over GF(2^8).
//
// Given k data shards, it produces m parity shards such that any k of the
// total k+m shards are sufficient to reconstruct the original data.
type RSEncoder struct {
	dataShards   int
	parityShards int
	totalShards  int
	encodeMatrix []byte // (k+m) × k matrix, row-major
	parityMatrix []byte // m × k submatrix (bottom rows of encodeMatrix)
}

// NewRSEncoder creates a Reed-Solomon encoder for the given data and parity shard counts.
func NewRSEncoder(dataShards, parityShards int) (*RSEncoder, error) {
	if dataShards <= 0 || parityShards <= 0 {
		return nil, fmt.Errorf("fec: dataShards (%d) and parityShards (%d) must be positive", dataShards, parityShards)
	}
	total := dataShards + parityShards
	if total > 255 {
		return nil, fmt.Errorf("fec: total shards (%d) exceeds GF(2^8) limit of 255", total)
	}

	enc := &RSEncoder{
		dataShards:   dataShards,
		parityShards: parityShards,
		totalShards:  total,
	}

	// Build the encoding matrix using a Vandermonde-derived approach.
	// 1) Create a Vandermonde matrix (total × dataShards)
	// 2) Invert its top k×k submatrix
	// 3) Multiply the full matrix by that inverse
	// Result: top k rows become identity, bottom m rows are the parity generator.
	vm := vandermonde(total, dataShards)
	topInv := matInvert(vm[:dataShards*dataShards], dataShards)
	enc.encodeMatrix = matMul(vm, topInv, total, dataShards, dataShards)
	enc.parityMatrix = enc.encodeMatrix[dataShards*dataShards:]

	return enc, nil
}

// Encode generates parity shards from the given data shards.
// All data shards must be the same length. Returns m parity shards.
func (e *RSEncoder) Encode(data [][]byte) ([][]byte, error) {
	if len(data) != e.dataShards {
		return nil, fmt.Errorf("%w: got %d data shards, want %d", ErrInvalidShards, len(data), e.dataShards)
	}

	shardLen := len(data[0])
	for i, d := range data {
		if len(d) != shardLen {
			return nil, fmt.Errorf("%w: shard %d has length %d, shard 0 has length %d", ErrShardSize, i, len(d), shardLen)
		}
	}

	// Allocate parity shards
	parity := make([][]byte, e.parityShards)
	for i := range parity {
		parity[i] = make([]byte, shardLen)
	}

	// Matrix multiply: parity[i] = sum(parityMatrix[i][j] * data[j]) for each byte position
	for i := 0; i < e.parityShards; i++ {
		for j := 0; j < e.dataShards; j++ {
			coeff := e.parityMatrix[i*e.dataShards+j]
			if coeff == 0 {
				continue
			}
			if coeff == 1 {
				// Optimization: multiplication by 1 is just XOR
				for b := 0; b < shardLen; b++ {
					parity[i][b] ^= data[j][b]
				}
			} else {
				for b := 0; b < shardLen; b++ {
					parity[i][b] ^= gfMul(coeff, data[j][b])
				}
			}
		}
	}

	return parity, nil
}

// Reconstruct recovers missing data shards given the surviving shards.
//
// shards is a slice of length (dataShards + parityShards). Present shards
// contain their data; missing shards are nil. At least dataShards non-nil
// shards must be present. On success, all nil data shards (indices 0..k-1)
// are filled in. Parity shards are not reconstructed.
func (e *RSEncoder) Reconstruct(shards [][]byte) error {
	if len(shards) != e.totalShards {
		return fmt.Errorf("%w: got %d shards, want %d", ErrInvalidShards, len(shards), e.totalShards)
	}

	// Find shard length from any present shard
	shardLen := 0
	for _, s := range shards {
		if s != nil {
			shardLen = len(s)
			break
		}
	}
	if shardLen == 0 {
		return ErrTooFewShards
	}

	// Identify which shards are present and which data shards need recovery
	presentIdx := make([]int, 0, e.totalShards)
	missingDataIdx := make([]int, 0, e.dataShards)

	for i, s := range shards {
		if s != nil {
			presentIdx = append(presentIdx, i)
		} else if i < e.dataShards {
			missingDataIdx = append(missingDataIdx, i)
		}
	}

	if len(presentIdx) < e.dataShards {
		return fmt.Errorf("%w: have %d shards, need %d", ErrTooFewShards, len(presentIdx), e.dataShards)
	}

	// If no data shards are missing, nothing to do
	if len(missingDataIdx) == 0 {
		return nil
	}

	// Select the first k present shards and build the corresponding submatrix
	useIdx := presentIdx[:e.dataShards]

	// Extract the k rows from the encoding matrix corresponding to the surviving shards
	subMatrix := make([]byte, e.dataShards*e.dataShards)
	for i, idx := range useIdx {
		copy(subMatrix[i*e.dataShards:(i+1)*e.dataShards], e.encodeMatrix[idx*e.dataShards:(idx+1)*e.dataShards])
	}

	// Invert the submatrix
	decodeMatrix := matInvert(subMatrix, e.dataShards)

	// Gather the surviving shard data in the order we selected
	subShards := make([][]byte, e.dataShards)
	for i, idx := range useIdx {
		subShards[i] = shards[idx]
	}

	// Reconstruct each missing data shard
	for _, missingIdx := range missingDataIdx {
		recovered := make([]byte, shardLen)
		// row of decodeMatrix corresponding to this data shard
		row := decodeMatrix[missingIdx*e.dataShards : (missingIdx+1)*e.dataShards]

		for j := 0; j < e.dataShards; j++ {
			coeff := row[j]
			if coeff == 0 {
				continue
			}
			if coeff == 1 {
				for b := 0; b < shardLen; b++ {
					recovered[b] ^= subShards[j][b]
				}
			} else {
				for b := 0; b < shardLen; b++ {
					recovered[b] ^= gfMul(coeff, subShards[j][b])
				}
			}
		}

		shards[missingIdx] = recovered
	}

	return nil
}

// --- Matrix operations over GF(2^8) ---

// vandermonde creates a rows×cols Vandermonde matrix.
// Element (i, j) = i^j in GF(2^8).
func vandermonde(rows, cols int) []byte {
	m := make([]byte, rows*cols)
	for i := 0; i < rows; i++ {
		for j := 0; j < cols; j++ {
			m[i*cols+j] = gfPow(byte(i), j)
		}
	}
	return m
}

// matMul multiplies matrix a (r1×c1) by matrix b (c1×c2), returning r1×c2.
func matMul(a, b []byte, r1, c1, c2 int) []byte {
	result := make([]byte, r1*c2)
	for i := 0; i < r1; i++ {
		for j := 0; j < c2; j++ {
			var val byte
			for k := 0; k < c1; k++ {
				val = gfAdd(val, gfMul(a[i*c1+k], b[k*c2+j]))
			}
			result[i*c2+j] = val
		}
	}
	return result
}

// matInvert inverts a size×size matrix using Gauss-Jordan elimination in GF(2^8).
// Returns the inverted matrix. Panics if the matrix is singular.
func matInvert(m []byte, size int) []byte {
	// Build [m | I] augmented matrix
	aug := make([]byte, size*size*2)
	for i := 0; i < size; i++ {
		copy(aug[i*size*2:i*size*2+size], m[i*size:(i+1)*size])
		aug[i*size*2+size+i] = 1 // identity on the right
	}

	cols := size * 2

	// Forward elimination with partial pivoting
	for col := 0; col < size; col++ {
		// Find pivot
		pivotRow := -1
		for row := col; row < size; row++ {
			if aug[row*cols+col] != 0 {
				pivotRow = row
				break
			}
		}
		if pivotRow == -1 {
			panic("fec: singular matrix in matInvert")
		}

		// Swap rows if needed
		if pivotRow != col {
			for c := 0; c < cols; c++ {
				aug[col*cols+c], aug[pivotRow*cols+c] = aug[pivotRow*cols+c], aug[col*cols+c]
			}
		}

		// Scale pivot row so pivot element becomes 1
		pivotVal := aug[col*cols+col]
		if pivotVal != 1 {
			pivotInv := gfInv(pivotVal)
			for c := 0; c < cols; c++ {
				aug[col*cols+c] = gfMul(aug[col*cols+c], pivotInv)
			}
		}

		// Eliminate all other rows in this column
		for row := 0; row < size; row++ {
			if row == col {
				continue
			}
			factor := aug[row*cols+col]
			if factor == 0 {
				continue
			}
			for c := 0; c < cols; c++ {
				aug[row*cols+c] = gfAdd(aug[row*cols+c], gfMul(factor, aug[col*cols+c]))
			}
		}
	}

	// Extract the right half (the inverse)
	inv := make([]byte, size*size)
	for i := 0; i < size; i++ {
		copy(inv[i*size:(i+1)*size], aug[i*cols+size:i*cols+size+size])
	}
	return inv
}
