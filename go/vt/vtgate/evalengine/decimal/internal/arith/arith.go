// Package arith provides performance-sensitive arithmetic
// operations.
package arith

import (
	"math/big"
	"math/bits"
)

const (
	intSize = 32 << (^uint(0) >> 63) // 32 or 64
	MaxInt  = 1<<(intSize-1) - 1
)

// Words returns a little-endian slice of big.Words representing
// the uint64.
func Words(x uint64) []big.Word {
	if bits.UintSize == 32 {
		return []big.Word{big.Word(x), big.Word(x >> 32)}
	}
	return []big.Word{big.Word(x)}
}

// Add sets z to x + y and returns z.
//
// x is assumed to be unsigned.
func Add(z, x *big.Int, y uint64) *big.Int {
	zw := z.Bits()
	switch xw := x.Bits(); {
	default:
		zw = add(zw, xw, big.Word(y))
	case len(xw) == 0:
		zw = setW(zw, big.Word(y))
	case y == 0:
		zw = set(zw, xw)
	}
	return z.SetBits(zw)
}

// Sub sets z to x - y and returns z.
//
// x is assumed to be unsigned.
func Sub(z, x *big.Int, y uint64) *big.Int {
	zw := z.Bits()
	switch xw := x.Bits(); {
	default:
		zw = sub(zw, xw, big.Word(y))
	case y == 0:
		zw = set(zw, xw)
	case len(xw) == 0:
		panic("underflow")
	}
	return z.SetBits(zw)
}

// Mul sets z to x * y and returns z.
//
// x is assumed to be unsigned.
func Mul(z, x *big.Int, y uint64) *big.Int {
	if y == 0 || x.Sign() == 0 {
		return z.SetUint64(0)
	}
	return z.SetBits(mulAddWW(z.Bits(), x.Bits(), big.Word(y), 0))
}

// MulPow10 computes x * 10**n and a bool indicating whether the
// multiplcation was successful.
func MulPow10(x uint64, n uint64) (uint64, bool) {
	p, ok := Pow10(n)
	if !ok {
		// 0 * 10^n = 0.
		return 0, x == 0
	}
	hi, lo := bits.Mul64(x, p)
	return lo, hi == 0
}

// MulBigPow10 sets z to x * 10**n and returns z.
func MulBigPow10(z, x *big.Int, n uint64) *big.Int {
	switch {
	case x.Sign() == 0:
		return z.SetUint64(0)
	case n == 0:
		return z.Set(x)
	default:
		return z.Mul(x, BigPow10(n))
	}
}

// Set sets z to the 128-bit integer represented by z1 and z0.
func Set(z *big.Int, z1, z0 uint64) *big.Int {
	ww := makeWord(z.Bits(), 128/bits.UintSize)
	if bits.UintSize == 32 {
		ww[3] = big.Word(z1 >> 32)
		ww[2] = big.Word(z1)
		ww[1] = big.Word(z0 >> 32)
		ww[0] = big.Word(z0)
	} else {
		ww[1] = big.Word(z1)
		ww[0] = big.Word(z0)
	}
	return z.SetBits(ww)
}

// The following is (mostly) copied from math/big/arith.go, licensed under the
// BSD 3-clause license: https://github.com/golang/go/blob/master/LICENSE

const (
	_S = _W / 8 // word size in bytes

	_W = bits.UintSize // word size in bits
	_B = 1 << _W       // digit base
	_M = _B - 1        // digit mask

	_W2 = _W / 2   // half word size in bits
	_B2 = 1 << _W2 // half digit base
	_M2 = _B2 - 1  // half digit mask
)

func makeWord(z []big.Word, n int) []big.Word {
	if n <= cap(z) {
		return z[:n]
	}
	const e = 4
	return make([]big.Word, n, n+e)
}

func norm(z []big.Word) []big.Word {
	i := len(z)
	for i > 0 && z[i-1] == 0 {
		i--
	}
	return z[0:i]
}

func mulWW(x, y big.Word) (z1, z0 big.Word) {
	zz1, zz0 := bits.Mul(uint(x), uint(y))
	return big.Word(zz1), big.Word(zz0)
}

func addWW(x, y, c big.Word) (z1, z0 big.Word) {
	zz1, zz0 := bits.Add(uint(x), uint(y), uint(c))
	return big.Word(zz0), big.Word(zz1)
}

func subWW(x, y, c big.Word) (z1, z0 big.Word) {
	zz1, zz0 := bits.Sub(uint(x), uint(y), uint(c))
	return big.Word(zz0), big.Word(zz1)
}

func mulAddWW(z, x []big.Word, y, r big.Word) []big.Word {
	m := len(x)
	z = makeWord(z, m+1)
	z[m] = mulAddVWW(z[0:m], x, y, r)
	return norm(z)
}

func mulAddVWW(z, x []big.Word, y, r big.Word) (c big.Word) {
	c = r
	for i := range z {
		c, z[i] = mulAddWWW(x[i], y, c)
	}
	return c
}

func mulAddWWW(x, y, c big.Word) (z1, z0 big.Word) {
	z1, zz0 := mulWW(x, y)
	if z0 = zz0 + c; z0 < zz0 {
		z1++
	}
	return z1, z0
}

func set(z, x []big.Word) []big.Word {
	z = makeWord(z, len(x))
	copy(z, x)
	return z
}

func setW(z []big.Word, x big.Word) []big.Word {
	z = makeWord(z, 1)
	z[0] = x
	return z
}

// add sets z to x + y and returns z.
func add(z, x []big.Word, y big.Word) []big.Word {
	m := len(x)
	const n = 1

	// m > 0 && y > 0

	z = makeWord(z, m+1)
	var c big.Word
	// addVV(z[0:m], x, y) but WW since len(y) == 1
	c, z[0] = addWW(x[0], y, 0)
	if m > n {
		c = addVW(z[n:m], x[n:], c)
	}
	z[m] = c
	return norm(z)
}

// sub sets z to x - y and returns z.
func sub(z, x []big.Word, y big.Word) []big.Word {
	m := len(x)
	const n = 1

	// m > 0 && y > 0

	z = makeWord(z, m)
	// subVV(z[0:m], x, y) but WW since len(y) == 1
	var c big.Word
	c, z[0] = subWW(x[0], y, 0)
	if m > n {
		c = subVW(z[n:], x[n:], c)
	}
	if c != 0 {
		panic("underflow")
	}
	return norm(z)
}

// addVW sets z to x + y and returns the carry.
func addVW(z, x []big.Word, y big.Word) (c big.Word) {
	c = y
	for i, xi := range x[:len(z)] {
		zi := xi + c
		z[i] = zi
		c = xi &^ zi >> (bits.UintSize - 1)
	}
	return c
}

// subVW sets z to x - y and returns the carry.
func subVW(z, x []big.Word, y big.Word) (c big.Word) {
	c = y
	for i, xi := range x[:len(z)] {
		zi := xi - c
		z[i] = zi
		c = zi &^ xi >> (bits.UintSize - 1)
	}
	return c
}
