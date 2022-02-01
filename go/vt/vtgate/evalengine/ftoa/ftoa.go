// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ftoa

import (
	"math"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// FormatFloat prints a 64 bit float as a string using the same formatting options as MySQL would.
// The code for this method and all its dependencies has been copied from the Go standard library,
// in the `strconv/ftoa.go` package.
// These is a comprehensive list of all the changes performed to this implementation compared to
// the one in the standard library, as to match MySQL's behavior
// - only support printing 64 bit floats
// - only support 'e', 'f' and 'g' as formatting specifiers
// - always print the decimals using the smallest possible precision as to not lose any information
// - consider 15 as the minimum exponent size to switch from 'f' to 'e'
// - do not print a '+' after the 'e' for positive exponents when formatting as 'e'
func FormatFloat(dst []byte, val float64, fmt byte) []byte {
	bits := math.Float64bits(val)
	neg := bits>>(float64Expbits+float64Mantbits) != 0
	exp := int(bits>>float64Mantbits) & (1<<float64Expbits - 1)
	mant := bits & (uint64(1)<<float64Mantbits - 1)

	switch exp {
	case 1<<float64Expbits - 1:
		// Inf, NaN
		var s string
		switch {
		case mant != 0:
			s = "NaN"
		case neg:
			s = "-Inf"
		default:
			s = "+Inf"
		}
		return append(dst, s...)

	case 0:
		// denormalized
		exp++

	default:
		// add implicit top bit
		mant |= uint64(1) << float64Mantbits
	}
	exp += float64Bias

	var prec int
	var digs decimalSlice
	// Negative precision means "only as much as needed to be exact."
	// Use Ryu algorithm.
	var buf [32]byte
	digs.d = buf[:]
	ryuFtoaShortest(&digs, mant, exp-int(float64Mantbits))
	// Precision for shortest representation mode.
	switch fmt {
	case 'e', 'E':
		prec = max(digs.nd-1, 0)
	case 'f':
		prec = max(digs.nd-digs.dp, 0)
	case 'g', 'G':
		prec = digs.nd
	}
	return formatDigits(dst, neg, digs, prec, fmt)
}

func formatDigits(dst []byte, neg bool, digs decimalSlice, prec int, fmt byte) []byte {
	switch fmt {
	case 'e', 'E':
		return fmtE(dst, neg, digs, prec, fmt)
	case 'f':
		return fmtF(dst, neg, digs, prec)
	case 'g', 'G':
		// trailing fractional zeros in 'e' form will be trimmed.
		eprec := mysqlLargeExponent
		exp := digs.dp - 1
		if exp < -4 || exp >= eprec {
			if prec > digs.nd {
				prec = digs.nd
			}
			return fmtE(dst, neg, digs, prec-1, fmt+'e'-'g')
		}
		if prec > digs.dp {
			prec = digs.nd
		}
		return fmtF(dst, neg, digs, max(prec-digs.dp, 0))
	default:
		panic("unknown format")
	}
}

type decimalSlice struct {
	d      []byte
	nd, dp int
	neg    bool
}

// %e: -d.ddddde±dd
func fmtE(dst []byte, neg bool, d decimalSlice, prec int, fmt byte) []byte {
	// sign
	if neg {
		dst = append(dst, '-')
	}

	// first digit
	ch := byte('0')
	if d.nd != 0 {
		ch = d.d[0]
	}
	dst = append(dst, ch)

	// .moredigits
	if prec > 0 {
		dst = append(dst, '.')
		i := 1
		m := min(d.nd, prec+1)
		if i < m {
			dst = append(dst, d.d[i:m]...)
			i = m
		}
		for ; i <= prec; i++ {
			dst = append(dst, '0')
		}
	}

	// e±
	dst = append(dst, fmt)
	exp := d.dp - 1
	if d.nd == 0 { // special case: 0 has exponent 0
		exp = 0
	}
	if exp < 0 {
		dst = append(dst, '-')
		exp = -exp
	}

	// dd or ddd
	switch {
	case exp < 10:
		dst = append(dst, '0', byte(exp)+'0')
	case exp < 100:
		dst = append(dst, byte(exp/10)+'0', byte(exp%10)+'0')
	default:
		dst = append(dst, byte(exp/100)+'0', byte(exp/10)%10+'0', byte(exp%10)+'0')
	}

	return dst
}

// %f: -ddddddd.ddddd
func fmtF(dst []byte, neg bool, d decimalSlice, prec int) []byte {
	// sign
	if neg {
		dst = append(dst, '-')
	}

	// integer, padded with zeros as needed.
	if d.dp > 0 {
		m := min(d.nd, d.dp)
		dst = append(dst, d.d[:m]...)
		for ; m < d.dp; m++ {
			dst = append(dst, '0')
		}
	} else {
		dst = append(dst, '0')
	}

	// fraction
	if prec > 0 {
		dst = append(dst, '.')
		for i := 0; i < prec; i++ {
			ch := byte('0')
			if j := d.dp + i; 0 <= j && j < d.nd {
				ch = d.d[j]
			}
			dst = append(dst, ch)
		}
	}

	return dst
}
