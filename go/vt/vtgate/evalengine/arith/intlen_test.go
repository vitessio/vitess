package arith

import (
	"crypto/rand"
	"io"
	"math"
	"math/big"
	mrand "math/rand"
	"testing"
)

func TestLength(t *testing.T) {
	tests := [...]struct {
		i uint64
		l int
	}{
		{i: 0, l: 1},
		{i: 1, l: 1},
		{i: 10, l: 2},
		{i: 100, l: 3},
		{i: 1000, l: 4},
		{i: 10000, l: 5},
		{i: 100000, l: 6},
		{i: 1000000, l: 7},
		{i: 10000000, l: 8},
		{i: 100000000, l: 9},
		{i: 1000000000, l: 10},
		{i: 10000000000, l: 11},
		{i: 100000000000, l: 12},
		{i: 1000000000000, l: 13},
		{i: 10000000000000, l: 14},
		{i: 100000000000000, l: 15},
		{i: 1000000000000000, l: 16},
		{i: 10000000000000000, l: 17},
		{i: 100000000000000000, l: 18},
		{i: 1000000000000000000, l: 19},
		{i: 10000000000000000000, l: 20},
	}
	for _, v := range tests {
		if l := Length(v.i); l != v.l {
			t.Fatalf("#%d: wanted %d, got %d", v.i, v.l, l)
		}
	}
}

func TestBigLength(t *testing.T) {
	tests := [...]struct {
		i *big.Int
		l int
	}{
		{i: new(big.Int).SetUint64(0), l: 1},
		{i: new(big.Int).SetUint64(1), l: 1},
		{i: new(big.Int).SetUint64(10), l: 2},
		{i: new(big.Int).SetUint64(100), l: 3},
		{i: new(big.Int).SetUint64(1000), l: 4},
		{i: new(big.Int).SetUint64(10000), l: 5},
		{i: new(big.Int).SetUint64(100000), l: 6},
		{i: new(big.Int).SetUint64(1000000), l: 7},
		{i: new(big.Int).SetUint64(10000000), l: 8},
		{i: new(big.Int).SetUint64(100000000), l: 9},
		{i: new(big.Int).SetUint64(1000000000), l: 10},
		{i: new(big.Int).SetUint64(10000000000), l: 11},
		{i: new(big.Int).SetUint64(100000000000), l: 12},
		{i: new(big.Int).SetUint64(1000000000000), l: 13},
		{i: new(big.Int).SetUint64(10000000000000), l: 14},
		{i: new(big.Int).SetUint64(100000000000000), l: 15},
		{i: new(big.Int).SetUint64(1000000000000000), l: 16},
		{i: new(big.Int).SetUint64(10000000000000000), l: 17},
		{i: new(big.Int).SetUint64(100000000000000000), l: 18},
		{i: new(big.Int).SetUint64(1000000000000000000), l: 19},
		{i: new(big.Int).SetUint64(10000000000000000000), l: 20},
		{i: BigPow10(25), l: 26},
		{i: BigPow10(50), l: 51},
		{i: BigPow10(150), l: 151},
		{i: BigPow10(2500), l: 2501},
	}
	for i, v := range tests {
		if l := BigLength(v.i); l != v.l {
			t.Fatalf("#%d: wanted %d, got %d", i, v.l, l)
		}
	}

	// Test a really long one.
	x := new(big.Int).Exp(big.NewInt(10), big.NewInt(1e5), nil)
	n := len(x.String())
	if l := BigLength(x); l != n {
		t.Fatalf("exp(10, 1e5): wanted %d, got %d", n, l)
	}

	if testing.Short() {
		t.Skip("skipping testing enormous big.Int bit-length in short mode")
	}

	// Randomly chosen length so its bit-length is a smidge above overflowCutoff
	// to speed up this looong test.
	nat := make([]big.Word, 222932222)
	nat[0] = 0xDEADBEEF
	for bp := 1; bp < len(nat); bp *= 2 {
		copy(nat[bp:], nat[:bp])
	}
	x.SetBits(nat)

	// Used by math/big.nat to determine the size of the output buffer.
	n = int(float64(x.BitLen())/math.Log2(10)) + 1

	// We're allowed to be +1 larger, but not smaller.
	if l := BigLength(x); l-n > 1 {
		t.Fatalf("really freaking big: wanted %d, got %d", n, l)
	}
}

var lengths = func() []*big.Int {
	const (
		N  = 1000
		S  = 64 * 3 // 1 << 18
		T  = ((N * S) + 7) / 8
		Sb = 8
	)

	var buf [T + 1]byte
	if _, err := io.ReadFull(rand.Reader, buf[:]); err != nil {
		panic(err)
	}

	var a [N]*big.Int
	for j := 0; j < T; j += (S + 7) / 8 {
		chunk := buf[j : j+((S+7)/8)]
		chunk[0] &= uint8(int(1<<Sb) - 1)
		chunk[0] |= 3 << (Sb - 2)
		a[j/((S+7)/8)] = new(big.Int).SetBytes(chunk)
	}
	return a[:]
}()

var rnd = mrand.New(mrand.NewSource(0))

var smallLengths = func() (a [5000]uint64) {
	for i := range a {
		a[i] = rnd.Uint64()
	}
	return a
}()

var gl int

func BenchmarkLength(b *testing.B) {
	var ll int
	for i := 0; i < b.N; i++ {
		ll = Length(smallLengths[i%len(smallLengths)])
	}
	gl = ll
}

func BenchmarkBigLength(b *testing.B) {
	var ll int
	for i := 0; i < b.N; i++ {
		ll = BigLength(lengths[i%len(lengths)])
	}
	gl = ll
}
