/*
Copyright (c) 2009 The Go Authors. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
   * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

// Copied from the Go runtime, package strconv

package fastparse

// decimal to binary floating point conversion.
// Algorithm:
//   1) Store input in multiprecision decimal.
//   2) Multiply/divide decimal by powers of two until in range [0.5, 1)
//   3) Multiply by 2^precision and round to get mantissa.

import (
	"math"
	"strconv"
)

type floatInfo struct {
	mantbits uint
	expbits  uint
	bias     int
}

func lower(c byte) byte {
	return c | ('x' - 'X')
}

var float32info = floatInfo{23, 8, -127}
var float64info = floatInfo{52, 11, -1023}

// commonPrefixLenIgnoreCase returns the length of the common
// prefix of s and prefix, with the character case of s ignored.
// The prefix argument must be all lower-case.
func commonPrefixLenIgnoreCase(s, prefix string) int {
	n := len(prefix)
	if n > len(s) {
		n = len(s)
	}
	for i := range n {
		c := s[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != prefix[i] {
			return i
		}
	}
	return n
}

// special returns the floating-point value for the special,
// possibly signed floating-point representations inf, infinity,
// and NaN. The result is ok if a prefix of s contains one
// of these representations and n is the length of that prefix.
// The character case is ignored.
func special(s string) (f float64, n int, ok bool) {
	if len(s) == 0 {
		return 0, 0, false
	}
	sign := 1
	nsign := 0
	switch s[0] {
	case '+', '-':
		if s[0] == '-' {
			sign = -1
		}
		nsign = 1
		s = s[1:]
		fallthrough
	case 'i', 'I':
		n := commonPrefixLenIgnoreCase(s, "infinity")
		// Anything longer than "inf" is ok, but if we
		// don't have "infinity", only consume "inf".
		if 3 < n && n < 8 {
			n = 3
		}
		if n == 3 || n == 8 {
			return math.Inf(sign), nsign + n, true
		}
	case 'n', 'N':
		if commonPrefixLenIgnoreCase(s, "nan") == 3 {
			return math.NaN(), 3, true
		}
	}
	return 0, 0, false
}

// readFloat reads a decimal or hexadecimal mantissa and exponent from a float
// string representation in s; the number may be followed by other characters.
// readFloat reports the number of bytes consumed (i), and whether the number
// is valid (ok).
func readFloat(s string) (mantissa uint64, exp int, neg, trunc bool, i int, ok bool) {
	const base = 10

	// optional sign
	if i >= len(s) {
		return
	}
	switch s[i] {
	case '+':
		i++
	case '-':
		neg = true
		i++
	}

	// digits
	maxMantDigits := 19 // 10^19 fits in uint64
	expChar := byte('e')
	sawdot := false
	sawdigits := false
	nd := 0
	ndMant := 0
	dp := 0
loop:
	for ; i < len(s); i++ {
		switch c := s[i]; true {
		case c == '.':
			if sawdot {
				break loop
			}
			sawdot = true
			dp = nd
			continue

		case '0' <= c && c <= '9':
			sawdigits = true
			if c == '0' && nd == 0 { // ignore leading zeros
				dp--
				continue
			}
			nd++
			if ndMant < maxMantDigits {
				mantissa *= base
				mantissa += uint64(c - '0')
				ndMant++
			} else if c != '0' {
				trunc = true
			}
			continue
		}
		break
	}
	if !sawdigits {
		return
	}
	if !sawdot {
		dp = nd
	}

	// optional exponent moves decimal point.
	// if we read a very large, very long number,
	// just be sure to move the decimal point by
	// a lot (say, 100000).  it doesn't matter if it's
	// not the exact number.
	if i < len(s) && lower(s[i]) == expChar {
		i++
		if i >= len(s) {
			return
		}
		esign := 1
		switch s[i] {
		case '+':
			i++
		case '-':
			i++
			esign = -1
		}
		if i >= len(s) || s[i] < '0' || s[i] > '9' {
			return
		}
		e := 0
		for ; i < len(s) && ('0' <= s[i] && s[i] <= '9'); i++ {
			if e < 10000 {
				e = e*10 + int(s[i]) - '0'
			}
		}
		dp += e * esign
	}

	if mantissa != 0 {
		exp = dp - ndMant
	}

	ok = true
	return
}

// Exact powers of 10.
var float64pow10 = []float64{
	1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
	1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19,
	1e20, 1e21, 1e22,
}
var float32pow10 = []float32{1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10}

// If possible to convert decimal representation to 64-bit float f exactly,
// entirely in floating-point math, do so, avoiding the expense of decimalToFloatBits.
// Three common cases:
//
//	value is exact integer
//	value is exact integer * exact power of ten
//	value is exact integer / exact power of ten
//
// These all produce potentially inexact but correctly rounded answers.
func atof64exact(mantissa uint64, exp int, neg bool) (f float64, ok bool) {
	if mantissa>>float64info.mantbits != 0 {
		return
	}
	f = float64(mantissa)
	if neg {
		f = -f
	}
	switch {
	case exp == 0:
		// an integer.
		return f, true
	// Exact integers are <= 10^15.
	// Exact powers of ten are <= 10^22.
	case exp > 0 && exp <= 15+22: // int * 10^k
		// If exponent is big but number of digits is not,
		// can move a few zeros into the integer part.
		if exp > 22 {
			f *= float64pow10[exp-22]
			exp = 22
		}
		if f > 1e15 || f < -1e15 {
			// the exponent was really too large.
			return
		}
		return f * float64pow10[exp], true
	case exp < 0 && exp >= -22: // int / 10^k
		return f / float64pow10[-exp], true
	}
	return
}

func Atof64(s string) (f float64, n int, err error) {
	if val, n, ok := special(s); ok {
		return val, n, nil
	}

	mantissa, exp, neg, trunc, n, ok := readFloat(s)
	if !ok {
		return 0, n, strconv.ErrSyntax
	}

	if !trunc {
		if f, ok := atof64exact(mantissa, exp, neg); ok {
			return f, n, nil
		}
	}
	f, ok = eiselLemire64(mantissa, exp, neg)
	if ok {
		if !trunc {
			return f, n, nil
		}
		// Even if the mantissa was truncated, we may
		// have found the correct result. Confirm by
		// converting the upper mantissa bound.
		fUp, ok := eiselLemire64(mantissa+1, exp, neg)
		if ok && f == fUp {
			return f, n, nil
		}
	}

	// Eisel-Lemire is incapable of parsing this float accurately; we need to
	// fall back to the slow path. Fortunately, we now know how many actual
	// characters the float has (it's the value of `n`), so we can simply fall
	// back to the standard library, which will do the slow parsing and will
	// not return an error for trailing characters after the float, because
	// we have truncated it.
	f, err = strconv.ParseFloat(s[:n], 64)
	return f, n, err
}
