// Package c provides internal constants.
package c

import (
	"math"
	"math/big"
)

const Inflated uint64 = math.MaxUint64

var (
	OneInt        = big.NewInt(1)
	TwoInt        = big.NewInt(2)
	FiveInt       = big.NewInt(5)
	TenInt        = big.NewInt(10)
	OneMillionInt = big.NewInt(1000000)

	TenFloat = big.NewFloat(10)

	MaxInt64 = big.NewInt(math.MaxInt64)
	MinInt64 = big.NewInt(math.MinInt64)

	MaxInt32 = big.NewInt(math.MaxInt32)
	MinInt32 = big.NewInt(math.MinInt32)
)
