package decimal

import (
	"bytes"
	"fmt"
	"io"
	"math/big"
	"strconv"
)

// allZeros returns true if every character in b is '0'.
func allZeros(b []byte) bool {
	for _, c := range b {
		if c != '0' {
			return false
		}
	}
	return true
}

var zero = []byte{'0'}

// roundString rounds the plain numeric string (e.g., "1234") b.
func roundString(b []byte, mode RoundingMode, pos bool, prec int) []byte {
	if prec >= len(b) {
		return append(b, bytes.Repeat(zero, prec-len(b))...)
	}

	// Trim zeros until prec. This is useful when we can round exactly by simply
	// chopping zeros off the end of the number.
	if allZeros(b[prec:]) {
		return b[:prec]
	}

	b = b[:prec+1]
	i := prec - 1

	// Blindly increment b[i] and handle possible carries later.
	switch mode {
	case AwayFromZero:
		b[i]++
	case ToZero:
		// OK
	case ToPositiveInf:
		if pos {
			b[i]++
		}
	case ToNegativeInf:
		if !pos {
			b[i]++
		}
	case ToNearestEven:
		if b[i+1] > '5' || b[i+1] == '5' && b[i]%2 != 0 {
			b[i]++
		}
	case ToNearestAway:
		if b[i+1] >= '5' {
			b[i]++
		}
	case ToNearestTowardZero:
		if b[i+1] > '5' {
			b[i]++
		}
	}

	if b[i] != '9'+1 {
		return b[:prec]
	}

	// We had to carry.
	b[i] = '0'

	for i--; i >= 0; i-- {
		if b[i] != '9' {
			b[i]++
			break
		}
		b[i] = '0'
	}

	// Carried all the way over to the first column, so slide the buffer down
	// and instead of reallocating.
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

// formatCompact formats the compact decimal, x, as an unsigned integer.
func formatCompact(x uint64) []byte {
	var b [20]byte
	return strconv.AppendUint(b[0:0], uint64(x), 10)
}

// formatUnscaled formats the unscaled (non-compact) decimal, unscaled, as an
// unsigned integer.
func formatUnscaled(unscaled *big.Int) []byte {
	// math/big.MarshalText never returns an error, only nil, so there's no need
	// to check for an error. Use MarshalText instead of Append because it limits
	// us to one allocation.
	b, _ := unscaled.MarshalText()
	if b[0] == '-' {
		b = b[1:]
	}
	return b
}

// noWidth indicates the width of a formatted number wasn't set.
const noWidth = -1

type format byte

const (
	normal format = iota // either sci or plain, depending on x
	plain                // forced plain
	sci                  // forced sci
)

//go:generate stringer -type=format

type formatter struct {
	w interface {
		io.Writer
		io.ByteWriter
		WriteString(string) (int, error)
	}
	sign  byte  // leading '+' or ' ' flag
	prec  int   // total precision
	width int   // min width
	n     int64 // cumulative number of bytes written to w
}

func (f *formatter) WriteByte(c byte) error {
	f.n++
	return f.w.WriteByte(c)
}

func (f *formatter) WriteString(s string) (int, error) {
	m, err := f.w.WriteString(s)
	f.n += int64(m)
	return m, err
}

func (f *formatter) Write(p []byte) (n int, err error) {
	n, err = f.w.Write(p)
	f.n += int64(n)
	return n, err
}

var sciE = [2]byte{GDA: 'E', Go: 'e'}

func (f *formatter) format(x *Big, format format, e byte, rounding RoundingMode) {
	if x == nil {
		f.WriteString("<nil>")
		return
	}

	o := x.Context.OperatingMode
	if x.isSpecial() {
		switch o {
		case GDA:
			f.WriteString(x.form.String())
			if x.IsNaN(0) && x.compact != 0 {
				f.WriteString(strconv.FormatUint(x.compact, 10))
			}
		case Go:
			if x.IsNaN(0) {
				f.WriteString("NaN")
			} else if x.IsInf(+1) {
				f.WriteString("+Inf")
			} else {
				f.WriteString("-Inf")
			}
		}
		return
	}

	if x.isZero() && o == Go {
		// Go mode prints zeros different than GDA.
		if f.width == noWidth {
			f.WriteByte('0')
		} else {
			f.WriteString("0.")
			io.CopyN(f, zeroReader{}, int64(f.width))
		}
		return
	}

	neg := x.Signbit()
	if neg {
		f.WriteByte('-')
	} else if f.sign != 0 {
		f.WriteByte(f.sign)
	}

	var (
		b   []byte
		exp int
	)
	if f.prec > 0 {
		if x.isCompact() {
			b = formatCompact(x.compact)
		} else {
			b = formatUnscaled(&x.unscaled)
		}
		orig := len(b)
		b = roundString(b, rounding, !neg, f.prec)
		exp = int(x.exp) + orig - len(b)
	} else if f.prec < 0 {
		f.prec = -f.prec
		exp = -f.prec
	} else {
		b = []byte{'0'}
	}

	// "Next, the adjusted exponent is calculated; this is the exponent, plus
	// the number of characters in the converted coefficient, less one. That
	// is, exponent+(clength-1), where clength is the length of the coefficient
	// in decimal digits.
	adj := exp + (len(b) - 1)
	if format != sci {
		if exp <= 0 && (format == plain || adj >= -6) {
			// "If the exponent is less than or equal to zero and the adjusted
			// exponent is greater than or equal to -6 the number will be
			// converted to a character form without using exponential notation."
			//
			// - http://speleotrove.com/decimal/daconvs.html#reftostr
			f.formatPlain(b, exp)
			return
		}

		// No decimal places, write b and fill with zeros.
		if format == plain && exp > 0 {
			f.Write(b)
			io.CopyN(f, zeroReader{}, int64(exp))
			return
		}
	}
	f.formatSci(b, adj, e)
}

// formatSci returns the scientific version of b.
func (f *formatter) formatSci(b []byte, adj int, e byte) {
	f.WriteByte(b[0])

	if len(b) > 1 {
		f.WriteByte('.')
		f.Write(b[1:])
	}

	// If negative, the call to strconv.Itoa will add the minus sign for us.
	f.WriteByte(e)
	if adj > 0 {
		f.WriteByte('+')
	}
	f.WriteString(strconv.Itoa(adj))
}

// formatPlain returns the plain string version of b.
func (f *formatter) formatPlain(b []byte, exp int) {
	const zeroRadix = "0."

	switch radix := len(b) + exp; {
	// log10(b) == scale, so immediately before b: 0.123456
	case radix == 0:
		f.WriteString(zeroRadix)
		f.Write(b)

	// log10(b) > scale, so somewhere inside b: 123.456
	case radix > 0:
		f.Write(b[:radix])
		if radix < len(b) {
			f.WriteByte('.')
			f.Write(b[radix:])
		}

	// log10(b) < scale, so before p "0s" and before b: 0.00000123456
	default:
		f.WriteString(zeroRadix)
		io.CopyN(f, zeroReader{}, -int64(radix))

		end := len(b)
		if f.prec < end {
			end = f.prec
		}
		f.Write(b[:end])
	}
}

// TODO(eric): can we merge zeroReader and spaceReader into a "singleReader" or
// something and still maintain the same performance?

// zeroReader is an io.Reader that, when read from, only provides the character
// '0'.
type zeroReader struct{}

// Read implements io.Reader.
func (z zeroReader) Read(p []byte) (n int, err error) {
	// zeroLiterals is 16 '0' bytes. It's used to speed up zeroReader's Read
	// method.
	const zeroLiterals = "0000000000000000"
	for n < len(p) {
		m := copy(p[n:], zeroLiterals)
		if m == 0 {
			break
		}
		n += m
	}
	return n, nil
}

// spaceReader is an io.Reader that, when read from, only provides the
// character ' '.
type spaceReader struct{}

// Read implements io.Reader.
func (s spaceReader) Read(p []byte) (n int, err error) {
	// spaceLiterals is 16 ' ' bytes. It's used to speed up spaceReader's Read
	// method.
	const spaceLiterals = "                "
	for n < len(p) {
		m := copy(p[n:], spaceLiterals)
		if m == 0 {
			break
		}
		n += m
	}
	return n, nil
}

// stateWrapper is a wrapper around an io.Writer to add WriteByte and
// WriteString methods.
type stateWrapper struct{ fmt.State }

func (w stateWrapper) WriteByte(c byte) error {
	_, err := w.Write([]byte{c})
	return err
}

func (w stateWrapper) WriteString(s string) (int, error) {
	return io.WriteString(w.State, s)
}
