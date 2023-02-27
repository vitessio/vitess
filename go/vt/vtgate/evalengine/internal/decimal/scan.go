/*
Copyright 2022 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package decimal

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/bits"
	"strconv"
	"strings"
)

var errOverflow = errors.New("overflow")

func parseDecimal64(s []byte) (Decimal, error) {
	const cutoff = math.MaxUint64/10 + 1
	var n uint64
	var dot = -1

	for i, c := range s {
		var d byte
		switch {
		case c == '.':
			if dot > -1 {
				return Decimal{}, fmt.Errorf("too many .s")
			}
			dot = i
			continue
		case '0' <= c && c <= '9':
			d = c - '0'
		default:
			return Decimal{}, fmt.Errorf("unexpected character %q", c)
		}

		if n >= cutoff {
			// n*base overflows
			return Decimal{}, errOverflow
		}
		n *= 10
		n1 := n + uint64(d)
		if n1 < n {
			return Decimal{}, errOverflow
		}
		n = n1
	}

	var exp int32
	if dot != -1 {
		exp = -int32(len(s) - dot - 1)
	}
	return Decimal{
		value: new(big.Int).SetUint64(n),
		exp:   exp,
	}, nil
}

func NewFromMySQL(s []byte) (Decimal, error) {
	var original = s
	var neg bool

	if len(s) > 0 {
		switch s[0] {
		case '+':
			s = s[1:]
		case '-':
			neg = true
			s = s[1:]
		}
	}

	if len(s) == 0 {
		return Decimal{}, fmt.Errorf("can't convert %q to decimal: too short", original)
	}

	if len(s) <= 18 {
		dec, err := parseDecimal64(s)
		if err == nil {
			if neg {
				dec.value.Neg(dec.value)
			}
			return dec, nil
		}
		if err != errOverflow {
			return Decimal{}, fmt.Errorf("can't convert %s to decimal: %v", original, err)
		}
	}

	var fractional, integral []byte
	if pIndex := bytes.IndexByte(s, '.'); pIndex >= 0 {
		if bytes.IndexByte(s[pIndex+1:], '.') != -1 {
			return Decimal{}, fmt.Errorf("can't convert %s to decimal: too many .s", original)
		}
		if pIndex+1 < len(s) {
			integral = s[:pIndex]
			fractional = s[pIndex+1:]
		} else {
			integral = s[:pIndex]
		}
	} else {
		integral = s
	}

	// Check if the size of this bigint would fit in the limits
	// that MySQL has by default. To do that, we must convert the
	// length of our integral and fractional part to "mysql digits"
	myintg := myBigDigits(int32(len(integral)))
	myfrac := myBigDigits(int32(len(fractional)))
	if myintg > MyMaxBigDigits {
		return largestForm(MyMaxPrecision, 0, neg), nil
	}
	if myintg+myfrac > MyMaxBigDigits {
		fractional = fractional[:int((MyMaxBigDigits-myintg)*9)]
	}
	value, err := parseLargeDecimal(integral, fractional)
	if err != nil {
		return Decimal{}, err
	}
	if neg {
		value.Neg(value)
	}
	return Decimal{value: value, exp: -int32(len(fractional))}, nil
}

// NewFromString returns a new Decimal from a string representation.
// Trailing zeroes are not trimmed.
//
// Example:
//
//	d, err := NewFromString("-123.45")
//	d2, err := NewFromString(".0001")
//	d3, err := NewFromString("1.47000")
func NewFromString(value string) (Decimal, error) {
	originalInput := value
	var intString string
	var exp int64

	// Check if number is using scientific notation
	eIndex := strings.IndexAny(value, "Ee")
	if eIndex != -1 {
		expInt, err := strconv.ParseInt(value[eIndex+1:], 10, 32)
		if err != nil {
			if e, ok := err.(*strconv.NumError); ok && e.Err == strconv.ErrRange {
				return Decimal{}, fmt.Errorf("can't convert %s to decimal: fractional part too long", value)
			}
			return Decimal{}, fmt.Errorf("can't convert %s to decimal: exponent is not numeric", value)
		}
		value = value[:eIndex]
		exp = expInt
	}

	pIndex := -1
	vLen := len(value)
	for i := 0; i < vLen; i++ {
		if value[i] == '.' {
			if pIndex > -1 {
				return Decimal{}, fmt.Errorf("can't convert %s to decimal: too many .s", value)
			}
			pIndex = i
		}
	}

	if pIndex == -1 {
		// There is no decimal point, we can just parse the original string as
		// an int
		intString = value
	} else {
		if pIndex+1 < vLen {
			intString = value[:pIndex] + value[pIndex+1:]
		} else {
			intString = value[:pIndex]
		}
		expInt := -len(value[pIndex+1:])
		exp += int64(expInt)
	}

	var dValue *big.Int
	// strconv.ParseInt is faster than new(big.Int).SetString so this is just a shortcut for strings we know won't overflow
	if len(intString) <= 18 {
		parsed64, err := strconv.ParseInt(intString, 10, 64)
		if err != nil {
			return Decimal{}, fmt.Errorf("can't convert %s to decimal", value)
		}
		dValue = big.NewInt(parsed64)
	} else {
		dValue = new(big.Int)
		_, ok := dValue.SetString(intString, 10)
		if !ok {
			return Decimal{}, fmt.Errorf("can't convert %s to decimal", value)
		}
	}

	if exp < math.MinInt32 || exp > math.MaxInt32 {
		// NOTE(vadim): I doubt a string could realistically be this long
		return Decimal{}, fmt.Errorf("can't convert %s to decimal: fractional part too long", originalInput)
	}

	return Decimal{
		value: dValue,
		exp:   int32(exp),
	}, nil
}

// RequireFromString returns a new Decimal from a string representation
// or panics if NewFromString would have returned an error.
//
// Example:
//
//	d := RequireFromString("-123.45")
//	d2 := RequireFromString(".0001")
func RequireFromString(value string) Decimal {
	dec, err := NewFromString(value)
	if err != nil {
		panic(err)
	}
	return dec
}

func mulWW(x, y big.Word) (z1, z0 big.Word) {
	zz1, zz0 := bits.Mul(uint(x), uint(y))
	return big.Word(zz1), big.Word(zz0)
}

func mulAddWWW(x, y, c big.Word) (z1, z0 big.Word) {
	z1, zz0 := mulWW(x, y)
	if z0 = zz0 + c; z0 < zz0 {
		z1++
	}
	return z1, z0
}

func mulAddVWW(z, x []big.Word, y, r big.Word) (c big.Word) {
	c = r
	for i := range z {
		c, z[i] = mulAddWWW(x[i], y, c)
	}
	return c
}

func mulAddWW(z, x []big.Word, y, r big.Word) []big.Word {
	m := len(x)
	z = z[:m+1]
	z[m] = mulAddVWW(z[0:m], x, y, r)
	return z
}

func pow(x big.Word, n int) (p big.Word) {
	// n == sum of bi * 2**i, for 0 <= i < imax, and bi is 0 or 1
	// thus x**n == product of x**(2**i) for all i where bi == 1
	// (Russian Peasant Method for exponentiation)
	p = 1
	for n > 0 {
		if n&1 != 0 {
			p *= x
		}
		x *= x
		n >>= 1
	}
	return
}

func parseLargeDecimal(integral, fractional []byte) (*big.Int, error) {
	const (
		b1 = big.Word(10)
		bn = big.Word(1e19)
		n  = 19
	)
	var (
		di = big.Word(0) // 0 <= di < b1**i < bn
		i  = 0           // 0 <= i < n
		// 5 is the largest possible size for a MySQL decimal; anything
		// that doesn't fit in 5 words won't make it to this func
		z = make([]big.Word, 0, 5)
	)

	parseChunk := func(partial []byte) error {
		for _, ch := range partial {
			var d1 big.Word
			switch {
			case ch == '.':
				continue
			case '0' <= ch && ch <= '9':
				d1 = big.Word(ch - '0')
			default:
				return fmt.Errorf("unexpected character %q", ch)
			}

			// collect d1 in di
			di = di*b1 + d1
			i++

			// if di is "full", add it to the result
			if i == n {
				z = mulAddWW(z, z, bn, di)
				di = 0
				i = 0
			}
		}
		return nil
	}

	if err := parseChunk(integral); err != nil {
		return nil, err
	}
	if err := parseChunk(fractional); err != nil {
		return nil, err
	}
	if i > 0 {
		z = mulAddWW(z, z, pow(b1, i), di)
	}
	return new(big.Int).SetBits(z), nil
}
