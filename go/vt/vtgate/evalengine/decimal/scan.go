package decimal

import (
	"fmt"
	"io"
	"math"

	"vitess.io/vitess/go/vt/vtgate/evalengine/decimal/internal/arith"
	"vitess.io/vitess/go/vt/vtgate/evalengine/decimal/internal/c"
)

func (z *Big) scan(r io.ByteScanner) error {
	if debug {
		defer func() { z.validate() }()
	}

	// http://speleotrove.com/decimal/daconvs.html#refnumsyn
	//
	//   sign           ::=  '+' | '-'
	//   digit          ::=  '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' |
	//                       '8' | '9'
	//   indicator      ::=  'e' | 'E'
	//   digits         ::=  digit [digit]...
	//   decimal-part   ::=  digits '.' [digits] | ['.'] digits
	//   exponent-part  ::=  indicator [sign] digits
	//   infinity       ::=  'Infinity' | 'Inf'
	//   nan            ::=  'NaN' [digits] | 'sNaN' [digits]
	//   numeric-value  ::=  decimal-part [exponent-part] | infinity
	//   numeric-string ::=  [sign] numeric-value | [sign] nan
	//
	// We deviate a little by being a tad bit more forgiving. For instance,
	// we allow case-insensitive nan and infinity values.

	// Sign
	neg, err := scanSign(r)
	if err != nil {
		return err
	}

	z.form, err = z.scanForm(r)
	if err != nil {
		switch err {
		case ConversionSyntax:
			z.form = qnan
			z.Context.Conditions |= ConversionSyntax
		default:
			return err
		}
		return nil
	}

	if z.isSpecial() {
		if neg {
			z.form |= signbit
		}
		return nil
	}

	// Mantissa (as a unsigned integer)
	if err := z.scanMant(r); err != nil {
		switch err {
		case io.EOF:
			z.form = qnan
			return io.ErrUnexpectedEOF
		case ConversionSyntax:
			z.form = qnan
			z.Context.Conditions |= ConversionSyntax
		default:
			return err
		}
		return nil
	}

	// Exponent
	if err := z.scanExponent(r); err != nil && err != io.EOF {
		switch err {
		case Underflow:
			z.xflow(MinScale, false, neg)
		case Overflow:
			z.xflow(MinScale, true, neg)
		case ConversionSyntax:
			z.form = qnan
			z.Context.Conditions |= ConversionSyntax
		default:
			return err
		}
		return nil
	}

	// Adjust for negative values.
	if neg {
		z.form |= signbit
	}
	return nil
}

func scanSign(r io.ByteScanner) (bool, error) {
	ch, err := r.ReadByte()
	if err != nil {
		return false, err
	}
	switch ch {
	case '+':
		return false, nil
	case '-':
		return true, nil
	default:
		return false, r.UnreadByte()
	}
}

func (z *Big) scanForm(r io.ByteScanner) (form, error) {
	ch, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	if (ch >= '0' && ch <= '9') || ch == '.' {
		return finite, r.UnreadByte()
	}

	signal := false
	switch ch {
	case 'i', 'I':
		return z.scanInfinity(r)
	case 'q', 'Q':
		// OK
	case 's', 'S':
		signal = true
	case 'n', 'N':
		r.UnreadByte()
	default:
		return 0, ConversionSyntax
	}

	const (
		s = "nan"
	)
	for i := 0; i < len(s); i++ {
		ch, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				return 0, io.ErrUnexpectedEOF
			}
			return 0, err
		}
		if ch != s[i] && ch != s[i]-('a'-'A') {
			return 0, ConversionSyntax
		}
	}

	// Parse payload
	for {
		ch, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		if ch < '0' || ch > '9' {
			return 0, ConversionSyntax
		}
		d := ch - '0'
		if d >= 10 {
			return 0, ConversionSyntax
		}
		const cutoff = math.MaxUint64/10 + 1
		if z.compact >= cutoff {
			return 0, ConversionSyntax
		}
		z.compact *= 10

		if z.compact+uint64(d) < z.compact {
			return 0, ConversionSyntax
		}
		z.compact += uint64(d)
	}

	if signal {
		return snan, nil
	}
	return qnan, nil
}

func (z *Big) scanInfinity(r io.ByteScanner) (form, error) {
	const (
		s = "infinity"
	)
	for i := 1; i < len(s); i++ {
		ch, err := r.ReadByte()
		if err != nil {
			if err != io.EOF {
				return 0, err
			}
			if i == len("inf") {
				return inf, nil
			}
			return 0, io.ErrUnexpectedEOF
		}
		if ch != s[i] && ch != s[i]-('a'-'A') {
			return 0, ConversionSyntax
		}
	}
	return inf, nil
}

func (z *Big) scanMant(r io.ByteScanner) (err error) {
	buf := make([]byte, 0, 20)
	seenDot := false
	dot := 0
	big := false

Loop:
	for {
		ch, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				break Loop
			}
			return err
		}

		// Common case.
		if ch >= '0' && ch <= '9' {
			buf = append(buf, ch)

			if !big {
				d := ch - '0'
				if d >= 10 {
					return ConversionSyntax
				}
				const cutoff = math.MaxUint64/10 + 1
				if z.compact >= cutoff {
					big = true
					continue
				}
				z.compact *= 10

				if z.compact+uint64(d) < z.compact {
					big = true
					continue
				}
				z.compact += uint64(d)
			}
			continue
		}

		switch ch {
		case '.':
			if seenDot {
				// Found two dots.
				return ConversionSyntax
			}
			seenDot = true
			dot = len(buf)
		case 'e', 'E':
			// Hit the exponent: we're done here.
			if err := r.UnreadByte(); err != nil {
				return err
			}
			break Loop
		default:
			return ConversionSyntax
		}
	}

	if big || z.compact == c.Inflated {
		z.unscaled.SetString(string(buf), 10)
		z.compact = c.Inflated
		z.precision = arith.BigLength(&z.unscaled)
	} else {
		z.precision = arith.Length(z.compact)
	}

	if seenDot {
		z.exp = -(len(buf) - dot)
	}
	return nil
}

func (z *Big) scanExponent(r io.ByteScanner) error {
	ch, err := r.ReadByte()
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}

	switch ch {
	case 'e', 'E':
		// OK
	default:
		return ConversionSyntax
	}

	ch, err = r.ReadByte()
	if err != nil {
		return err
	}
	var neg bool
	switch ch {
	case '+':
		// OK
	case '-':
		neg = true
	default:
		r.UnreadByte()
	}

	max := uint64(arith.MaxInt)
	if neg {
		max++ // -math.MinInt
	}

	var exp uint64
	for {
		ch, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if ch < '0' || ch > '9' {
			return ConversionSyntax
		}
		d := ch - '0'
		if d >= 10 {
			return ConversionSyntax
		}
		const cutoff = math.MaxUint64/10 + 1
		if exp >= cutoff {
			return ConversionSyntax
		}
		exp *= 10

		v := exp + uint64(d)
		if v < exp || v > max {
			if neg {
				return Underflow
			}
			return Overflow
		}
		exp = v
	}

	if neg {
		z.exp -= int(exp)
	} else {
		z.exp += int(exp)
	}
	return nil
}

// byteReader implementation borrowed from math/big/intconv.go

// byteReader is a local wrapper around fmt.ScanState; it implements the
// io.ByteReader interface.
type byteReader struct {
	fmt.ScanState
}

func (r byteReader) ReadByte() (byte, error) {
	ch, size, err := r.ReadRune()
	if size != 1 && err == nil {
		err = fmt.Errorf("invalid rune %#U", ch)
	}
	return byte(ch), err
}

func (r byteReader) UnreadByte() error {
	return r.UnreadRune()
}
