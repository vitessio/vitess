//go:build 386 || arm || mips || mipsle

package decimal

import "math/big"

const (
	b1 = big.Word(10)
	bn = big.Word(1e9)
	n  = 9
	s  = 10
)
