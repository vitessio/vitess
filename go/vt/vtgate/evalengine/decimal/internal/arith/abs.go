package arith

import (
	"math/big"
	"math/bits"
)

// Abs returns the absolute value of x.
func Abs(x int64) uint64 {
	m := x >> 63
	return uint64((x ^ m) - m)
}

// Cmp compares x and y and returns
//
//    -1 if x  < 0
//     0 if x == 0
//    +1 if x  > 0
//
func Cmp(x, y uint64) int {
	if x != y {
		if x > y {
			return +1
		}
		return -1
	}
	return 0
}

// CmpShift compares x and y*shift.
func CmpShift(x, y, shift uint64) int {
	y1, y0 := bits.Mul64(y, shift)
	if y1 != 0 {
		return -1
	}
	return Cmp(x, y0)
}

// CmpBits compares x and y.
func CmpBits(x, y []big.Word) (r int) {
	// Copied from math/big.nat.go
	m := len(x)
	n := len(y)
	if m != n || m == 0 {
		switch {
		case m < n:
			r = -1
		case m > n:
			r = 1
		}
		return
	}

	i := m - 1
	for i > 0 && x[i] == y[i] {
		i--
	}

	switch {
	case x[i] < y[i]:
		r = -1
	case x[i] > y[i]:
		r = 1
	}
	return
}
