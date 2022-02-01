// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ftoa

import "math/bits"

// binary to decimal conversion using the Ryū algorithm.
//
// See Ulf Adams, "Ryū: Fast Float-to-String Conversion" (doi:10.1145/3192366.3192369)
//
// Fixed precision formatting is a variant of the original paper's
// algorithm, where a single multiplication by 10^k is required,
// sharing the same rounding guarantees.

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

// ryuFtoaShortest formats mant*2^exp with prec decimal digits.
func ryuFtoaShortest(d *decimalSlice, mant uint64, exp int) {
	if mant == 0 {
		d.nd, d.dp = 0, 0
		return
	}
	// If input is an exact integer with fewer bits than the mantissa,
	// the previous and next integer are not admissible representations.
	if exp <= 0 && bits.TrailingZeros64(mant) >= -exp {
		mant >>= uint(-exp)
		ryuDigits(d, mant, mant, mant, true, false)
		return
	}
	ml, mc, mu, e2 := computeBounds(mant, exp)
	if e2 == 0 {
		ryuDigits(d, ml, mc, mu, true, false)
		return
	}
	// Find 10^q *larger* than 2^-e2
	q := mulByLog2Log10(-e2) + 1

	// We are going to multiply by 10^q using 128-bit arithmetic.
	// The exponent is the same for all 3 numbers.
	var dl, dc, du uint64
	var dl0, dc0, du0 bool
	dl, _, dl0 = mult128bitPow10(ml, e2, q)
	dc, _, dc0 = mult128bitPow10(mc, e2, q)
	du, e2, du0 = mult128bitPow10(mu, e2, q)
	if e2 >= 0 {
		panic("not enough significant bits after mult128bitPow10")
	}
	// Is it an exact computation?
	if q > 55 {
		// Large positive powers of ten are not exact
		dl0, dc0, du0 = false, false, false
	}
	if q < 0 && q >= -24 {
		// Division by a power of ten may be exact.
		// (note that 5^25 is a 59-bit number so division by 5^25 is never exact).
		if divisibleByPower5(ml, -q) {
			dl0 = true
		}
		if divisibleByPower5(mc, -q) {
			dc0 = true
		}
		if divisibleByPower5(mu, -q) {
			du0 = true
		}
	}
	// Express the results (dl, dc, du)*2^e2 as integers.
	// Extra bits must be removed and rounding hints computed.
	extra := uint(-e2)
	extraMask := uint64(1<<extra - 1)
	// Now compute the floored, integral base 10 mantissas.
	dl, fracl := dl>>extra, dl&extraMask
	dc, fracc := dc>>extra, dc&extraMask
	du, fracu := du>>extra, du&extraMask
	// Is it allowed to use 'du' as a result?
	// It is always allowed when it is truncated, but also
	// if it is exact and the original binary mantissa is even
	// When disallowed, we can substract 1.
	uok := !du0 || fracu > 0
	if du0 && fracu == 0 {
		uok = mant&1 == 0
	}
	if !uok {
		du--
	}
	// Is 'dc' the correctly rounded base 10 mantissa?
	// The correct rounding might be dc+1
	cup := false // don't round up.
	if dc0 {
		// If we computed an exact product, the half integer
		// should round to next (even) integer if 'dc' is odd.
		cup = fracc > 1<<(extra-1) ||
			(fracc == 1<<(extra-1) && dc&1 == 1)
	} else {
		// otherwise, the result is a lower truncation of the ideal
		// result.
		cup = fracc>>(extra-1) == 1
	}
	// Is 'dl' an allowed representation?
	// Only if it is an exact value, and if the original binary mantissa
	// was even.
	lok := dl0 && fracl == 0 && (mant&1 == 0)
	if !lok {
		dl++
	}
	// We need to remember whether the trimmed digits of 'dc' are zero.
	c0 := dc0 && fracc == 0
	// render digits
	ryuDigits(d, dl, dc, du, c0, cup)
	d.dp -= q
}

// mulByLog2Log10 returns math.Floor(x * log(2)/log(10)) for an integer x in
// the range -1600 <= x && x <= +1600.
//
// The range restriction lets us work in faster integer arithmetic instead of
// slower floating point arithmetic. Correctness is verified by unit tests.
func mulByLog2Log10(x int) int {
	// log(2)/log(10) ≈ 0.30102999566 ≈ 78913 / 2^18
	return (x * 78913) >> 18
}

// mulByLog10Log2 returns math.Floor(x * log(10)/log(2)) for an integer x in
// the range -500 <= x && x <= +500.
//
// The range restriction lets us work in faster integer arithmetic instead of
// slower floating point arithmetic. Correctness is verified by unit tests.
func mulByLog10Log2(x int) int {
	// log(10)/log(2) ≈ 3.32192809489 ≈ 108853 / 2^15
	return (x * 108853) >> 15
}

// computeBounds returns a floating-point vector (l, c, u)×2^e2
// where the mantissas are 55-bit (or 26-bit) integers, describing the interval
// represented by the input float64 or float32.
func computeBounds(mant uint64, exp int) (lower, central, upper uint64, e2 int) {
	if mant != 1<<float64Mantbits || exp == float64Bias+1-int(float64Mantbits) {
		// regular case (or denormals)
		lower, central, upper = 2*mant-1, 2*mant, 2*mant+1
		e2 = exp - 1
		return
	} else {
		// border of an exponent
		lower, central, upper = 4*mant-1, 4*mant, 4*mant+2
		e2 = exp - 2
		return
	}
}

func ryuDigits(d *decimalSlice, lower, central, upper uint64,
	c0, cup bool) {
	lhi, llo := divmod1e9(lower)
	chi, clo := divmod1e9(central)
	uhi, ulo := divmod1e9(upper)
	if uhi == 0 {
		// only low digits (for denormals)
		ryuDigits32(d, llo, clo, ulo, c0, cup, 8)
	} else if lhi < uhi {
		// truncate 9 digits at once.
		if llo != 0 {
			lhi++
		}
		c0 = c0 && clo == 0
		cup = (clo > 5e8) || (clo == 5e8 && cup)
		ryuDigits32(d, lhi, chi, uhi, c0, cup, 8)
		d.dp += 9
	} else {
		d.nd = 0
		// emit high part
		n := uint(9)
		for v := chi; v > 0; {
			v1, v2 := v/10, v%10
			v = v1
			n--
			d.d[n] = byte(v2 + '0')
		}
		d.d = d.d[n:]
		d.nd = int(9 - n)
		// emit low part
		ryuDigits32(d, llo, clo, ulo,
			c0, cup, d.nd+8)
	}
	// trim trailing zeros
	for d.nd > 0 && d.d[d.nd-1] == '0' {
		d.nd--
	}
	// trim initial zeros
	for d.nd > 0 && d.d[0] == '0' {
		d.nd--
		d.dp--
		d.d = d.d[1:]
	}
}

// ryuDigits32 emits decimal digits for a number less than 1e9.
func ryuDigits32(d *decimalSlice, lower, central, upper uint32,
	c0, cup bool, endindex int) {
	if upper == 0 {
		d.dp = endindex + 1
		return
	}
	trimmed := 0
	// Remember last trimmed digit to check for round-up.
	// c0 will be used to remember zeroness of following digits.
	cNextDigit := 0
	for upper > 0 {
		// Repeatedly compute:
		// l = Ceil(lower / 10^k)
		// c = Round(central / 10^k)
		// u = Floor(upper / 10^k)
		// and stop when c goes out of the (l, u) interval.
		l := (lower + 9) / 10
		c, cdigit := central/10, central%10
		u := upper / 10
		if l > u {
			// don't trim the last digit as it is forbidden to go below l
			// other, trim and exit now.
			break
		}
		// Check that we didn't cross the lower boundary.
		// The case where l < u but c == l-1 is essentially impossible,
		// but may happen if:
		//    lower   = ..11
		//    central = ..19
		//    upper   = ..31
		// and means that 'central' is very close but less than
		// an integer ending with many zeros, and usually
		// the "round-up" logic hides the problem.
		if l == c+1 && c < u {
			c++
			cdigit = 0
			cup = false
		}
		trimmed++
		// Remember trimmed digits of c
		c0 = c0 && cNextDigit == 0
		cNextDigit = int(cdigit)
		lower, central, upper = l, c, u
	}
	// should we round up?
	if trimmed > 0 {
		cup = cNextDigit > 5 ||
			(cNextDigit == 5 && !c0) ||
			(cNextDigit == 5 && c0 && central&1 == 1)
	}
	if central < upper && cup {
		central++
	}
	// We know where the number ends, fill directly
	endindex -= trimmed
	v := central
	n := endindex
	for n > d.nd {
		v1, v2 := v/100, v%100
		d.d[n] = smallsString[2*v2+1]
		d.d[n-1] = smallsString[2*v2+0]
		n -= 2
		v = v1
	}
	if n == d.nd {
		d.d[n] = byte(v + '0')
	}
	d.nd = endindex + 1
	d.dp = d.nd + trimmed
}

// mult128bitPow10 takes a floating-point input with a 55-bit
// mantissa and multiplies it with 10^q. The resulting mantissa
// is m*P >> 119 where P is a 128-bit element of the detailedPowersOfTen tables.
// It is typically 63 or 64-bit wide.
// The returned boolean is true is all trimmed bits were zero.
//
// That is:
//     m*2^e2 * round(10^q) = resM * 2^resE + ε
//     exact = ε == 0
func mult128bitPow10(m uint64, e2, q int) (resM uint64, resE int, exact bool) {
	if q == 0 {
		// P == 1<<127
		return m << 8, e2 - 8, true
	}
	if q < detailedPowersOfTenMinExp10 || detailedPowersOfTenMaxExp10 < q {
		// This never happens due to the range of float32/float64 exponent
		panic("mult128bitPow10: power of 10 is out of range")
	}
	pow := detailedPowersOfTen[q-detailedPowersOfTenMinExp10]
	if q < 0 {
		// Inverse powers of ten must be rounded up.
		pow[0] += 1
	}
	e2 += mulByLog10Log2(q) - 127 + 119

	// long multiplication
	l1, l0 := bits.Mul64(m, pow[0])
	h1, h0 := bits.Mul64(m, pow[1])
	mid, carry := bits.Add64(l1, h0, 0)
	h1 += carry
	return h1<<9 | mid>>55, e2, mid<<9 == 0 && l0 == 0
}

func divisibleByPower5(m uint64, k int) bool {
	if m == 0 {
		return true
	}
	for i := 0; i < k; i++ {
		if m%5 != 0 {
			return false
		}
		m /= 5
	}
	return true
}
