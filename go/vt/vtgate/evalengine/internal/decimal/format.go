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
	"fmt"
	"math/big"
	"math/bits"
)

func appendZeroes(buf []byte, n int) []byte {
	const zeroes = "0000000000000000"
	for n >= len(zeroes) {
		buf = append(buf, zeroes...)
		n -= len(zeroes)
	}
	return append(buf, zeroes[:n]...)
}

func trimTrailingZeroes(buf []byte) []byte {
	var trim int
	for trim = len(buf); trim > 0; trim-- {
		if buf[trim-1] != '0' {
			break
		}
	}
	if buf[trim-1] == '.' {
		trim--
	}
	return buf[:trim]
}

// formatSlow formats the decimal to its maximum accuracy without rounding.
// This is an unoptimized implementation left here for testing against the
// optimized implementations.
func (d *Decimal) formatSlow(trim bool) []byte {
	var (
		buf         []byte
		exp         = int(d.exp)
		integral, _ = d.value.MarshalText()
	)

	if exp >= 0 {
		buf = make([]byte, 0, len(integral)+exp)
	} else if len(integral) > -exp {
		buf = make([]byte, 0, len(integral)+1)
	} else {
		buf = make([]byte, 0, len(integral)-exp+2)
	}

	if exp >= 0 {
		buf = append(buf, integral...)
		if d.value.Sign() != 0 {
			buf = appendZeroes(buf, exp)
		}
		return buf
	}

	if integral[0] == '-' {
		integral = integral[1:]
		buf = append(buf, '-')
	}

	if len(integral) > -exp {
		buf = append(buf, integral[:len(integral)+exp]...)
		buf = append(buf, '.')
		buf = append(buf, integral[len(integral)+exp:]...)
	} else {
		num0s := -exp - len(integral)
		buf = append(buf, '0', '.')
		buf = appendZeroes(buf, num0s)
		buf = append(buf, integral...)
	}
	if trim {
		buf = trimTrailingZeroes(buf)
	}
	return buf
}

var zeroByte = []byte{'0'}

const smallsString = "00010203040506070809" +
	"10111213141516171819" +
	"20212223242526272829" +
	"30313233343536373839" +
	"40414243444546474849" +
	"50515253545556575859" +
	"60616263646566676869" +
	"70717273747576777879" +
	"80818283848586878889" +
	"90919293949596979899"

const maxUint64FormatSize = 20

// formatMantissa formats the mantissa of this decimal into its base10 representation.
// The given buf must be at least 20 characters long to ensure single-word
// mantissas can be formatted in place. If this decimal has a mantissa composed
// by multiple words, the given buf is ignored and the formatted mantissa is
// returned as a new allocation from the `big` package in the stdlib.
func (d *Decimal) formatMantissa(buf []byte) []byte {
	var (
		us    uint
		words = d.value.Bits()
		i     = len(buf)
	)

	switch len(words) {
	case 0:
		return zeroByte
	case 1:
		us = uint(words[0])
	default:
		// MarshalText cannot fail
		buf, _ = d.value.MarshalText()
		if buf[0] == '-' {
			buf = buf[1:]
		}
		return buf
	}

	for us >= 100 {
		is := us % 100 * 2
		us /= 100
		i -= 2
		buf[i+1] = smallsString[is+1]
		buf[i+0] = smallsString[is+0]
	}

	// us < 100
	is := us * 2
	i--
	buf[i] = smallsString[is+1]
	if us >= 10 {
		i--
		buf[i] = smallsString[is]
	}
	return buf[i:]
}

// formatFast formats this decimal number into its base10 representation.
// If round is true, the number will be rounded to the given precision,
// which must be >= 0
// If trim is true, trailing zeroes after the decimal period will be stripped
func (d *Decimal) formatFast(prec int, round bool, trim bool) []byte {
	var (
		buf      []byte
		exp      int
		sign     int
		short    [maxUint64FormatSize]byte
		integral = d.formatMantissa(short[:])
	)

	if prec < 0 {
		panic("decimal: formatFast with prec < 0")
	}

	if round {
		// prec is the amount of decimal places after the period we want;
		// However, to perform string-based rounding in our integer, we need
		// prec to be the total amount of significant digits in the mantissa
		// (i.e. the number of integral digits + the number of decimals)
		// Let's adjust prec accordingly based on the exponent for the number
		// and iprec, which is the precision of our mantissa
		iprec := len(integral)
		if d.exp > 0 {
			prec += int(d.exp) + iprec
		} else {
			if adj := int(d.exp) + iprec; adj > -prec {
				prec += adj
			} else {
				prec = -prec
			}
		}
		if prec > 0 {
			// if prec > 0, perform string-based rounding on the integral to
			integral = roundString(integral, prec)
			exp = int(d.exp) + iprec - len(integral)
			sign = d.value.Sign()
		} else if prec < 0 {
			integral = nil
			prec = -prec
			exp = -prec
		} else {
			integral = zeroByte
		}
	} else {
		exp = int(d.exp)
		sign = d.value.Sign()
		prec = len(integral)
	}

	// alloc allocates the destination buf to the right size, and prepends a
	// negative sign for negative numbers
	alloc := func(length int) []byte {
		buf := make([]byte, 0, length+1)
		if sign < 0 {
			buf = append(buf, '-')
		}
		return buf
	}

	// exp > 0, so integral is truly integral but scaled up; there's no period
	if exp > 0 {
		buf = alloc(len(integral) + exp)
		buf = append(buf, integral...)
		if sign != 0 {
			buf = appendZeroes(buf, exp)
		}
		return buf
	}

	const zeroRadix = "0."
	switch radix := len(integral) + exp; {
	// log10(integral) == scale, so place "0." immediately before integral: 0.123456
	case radix == 0:
		buf = alloc(len(zeroRadix) + len(integral))
		buf = append(buf, zeroRadix...)
		buf = append(buf, integral...)

	// log10(integral) > scale, so the period is somewhere inside integral: 123.456
	case radix > 0:
		buf = alloc(len(integral) + 1)
		buf = append(buf, integral[:radix]...)
		if radix < len(integral) {
			buf = append(buf, '.')
			buf = append(buf, integral[radix:]...)
		} else {
			trim = false
		}
	// log10(integral) < scale, so put "0." and fill with zeroes until integral: 0.00000123456
	default:
		end := len(integral)
		if prec < end {
			end = prec
		}
		buf = alloc(len(zeroRadix) - radix + end)
		buf = append(buf, zeroRadix...)
		buf = appendZeroes(buf, -radix)
		buf = append(buf, integral[:end]...)
	}
	if trim {
		buf = trimTrailingZeroes(buf)
	}
	return buf
}

// allZeros returns true if every character in b is '0'.
func allZeros(b []byte) bool {
	for _, c := range b {
		if c != '0' {
			return false
		}
	}
	return true
}

// roundString rounds the plain numeric string (e.g., "1234") b.
func roundString(b []byte, prec int) []byte {
	if prec >= len(b) {
		return appendZeroes(b, prec-len(b))
	}

	// Trim zeros until prec. This is useful when we can round exactly by simply
	// chopping zeros off the end of the number.
	if allZeros(b[prec:]) {
		return b[:prec]
	}

	b = b[:prec+1]
	i := prec - 1

	// Do the rounding away from zero and check if we overflowed;
	// if so we'll have to carry
	if b[i+1] >= '5' {
		b[i]++
	}
	if b[i] != '9'+1 {
		return b[:prec]
	}
	b[i] = '0'
	for i--; i >= 0; i-- {
		if b[i] != '9' {
			b[i]++
			break
		}
		b[i] = '0'
	}

	// Carried all the way over to the first column, so slide the buffer down
	// instead of reallocating.
	if b[0] == '0' {
		copy(b[1:], b)
		b[0] = '1'
		// We might end up with an extra digit of precision. E.g., given the
		// decimal 9.9 with a requested precision of 1, we'd convert 99 -> 10.
		// Let the calling code handle that case.
		prec++
	}
	return b[:prec]
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
		di     = big.Word(0) // 0 <= di < b1**i < bn
		i      = 0           // 0 <= i < n
		z      = make([]big.Word, 0, 5)
		chunks = [2][]byte{integral, fractional}
	)

	for _, partial := range chunks {
		for _, ch := range partial {
			var d1 big.Word
			switch {
			case ch == '.':
				continue
			case '0' <= ch && ch <= '9':
				d1 = big.Word(ch - '0')
			default:
				return nil, fmt.Errorf("unexpected character %q", ch)
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
	}
	if i > 0 {
		z = mulAddWW(z, z, pow(b1, i), di)
	}
	return new(big.Int).SetBits(z), nil
}
