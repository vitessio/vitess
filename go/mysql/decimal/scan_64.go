//go:build !386 && !arm && !mips && !mipsle

package decimal

import "math/big"

const (
	b1 = big.Word(10)
	bn = big.Word(1e19)
	n  = 19
	s  = 5
)
