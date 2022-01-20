package decimal

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
)

// decomposer composes or decomposes a decimal value to and from individual parts.
// There are four separate parts: a boolean negative flag, a form byte with three possible states
// (finite=0, infinite=1, NaN=2),  a base-2 big-endian integer
// coefficient (also known as a significand) as a []byte, and an int32 exponent.
// These are composed into a final value as "decimal = (neg) (form=finite) coefficient * 10 ^ exponent".
// A zero length coefficient is a zero value.
// If the form is not finite the coefficient and scale should be ignored.
// The negative parameter may be set to true for any form, although implementations are not required
// to respect the negative parameter in the non-finite form.
//
// Implementations may choose to signal a negative zero or negative NaN, but implementations
// that do not support these may also ignore the negative zero or negative NaN without error.
// If an implementation does not support Infinity it may be converted into a NaN without error.
// If a value is set that is larger then what is supported by an implementation is attempted to
// be set, an error must be returned.
// Implementations must return an error if a NaN or Infinity is attempted to be set while neither
// are supported.
type decomposer interface {
	// Decompose returns the internal decimal state into parts.
	// If the provided buf has sufficient capacity, buf may be returned as the coefficient with
	// the value set and length set as appropriate.
	Decompose(buf []byte) (form byte, negative bool, coefficient []byte, exponent int32)

	// Compose sets the internal decimal value from parts. If the value cannot be
	// represented then an error should be returned.
	// The coefficent should not be modified. Successive calls to compose with
	// the same arguments should result in the same decimal value.
	Compose(form byte, negative bool, coefficient []byte, exponent int32) error
}

// Decompose returns the internal decimal state into parts.
// If the provided buf has sufficient capacity, buf may be returned as the coefficient with
// the value set and length set as appropriate.
func (z *Big) Decompose(buf []byte) (form byte, negative bool, coefficient []byte, exponent int32) {
	negative = z.Sign() < 0
	switch {
	case z.IsInf(0):
		form = 1
		return
	case z.IsNaN(0):
		form = 2
		return
	}
	if !z.IsFinite() {
		panic("expected number to be finite")
	}
	if z.exp > math.MaxInt32 {
		panic("exponent exceeds max size")
	}
	exponent = int32(z.exp)

	if z.isCompact() {
		if cap(buf) >= 8 {
			coefficient = buf[:8]
		} else {
			coefficient = make([]byte, 8)
		}
		binary.BigEndian.PutUint64(coefficient, z.compact)
	} else {
		coefficient = z.unscaled.Bytes() // This returns a big-endian slice.
	}
	return
}

// Compose sets the internal decimal value from parts. If the value cannot be
// represented then an error should be returned.
func (z *Big) Compose(form byte, negative bool, coefficient []byte, exponent int32) error {
	switch form {
	default:
		return fmt.Errorf("unknown form: %v", form)
	case 0:
		// Finite form below.
	case 1:
		z.SetInf(negative)
		return nil
	case 2:
		z.SetNaN(false)
		return nil
	}
	bigc := &big.Int{}
	bigc.SetBytes(coefficient)
	z.SetBigMantScale(bigc, -int(exponent))
	if negative {
		z.Neg(z)
	}
	return nil
}
