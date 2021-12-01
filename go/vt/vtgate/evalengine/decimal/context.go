package decimal

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/vtgate/evalengine/decimal/internal/c"
)

// Precision and scale limits.
const (
	MaxScale           = c.MaxScale       // largest allowed scale.
	MinScale           = -MaxScale        // smallest allowed scale.
	MaxPrecision       = MaxScale         // largest allowed Context precision.
	MinPrecision       = 1                // smallest allowed Context precision.
	UnlimitedPrecision = MaxPrecision + 1 // no precision, but may error.
	DefaultPrecision   = 16               // default precision for literals.
)

// Context is a per-decimal contextual object that governs
// specific operations.
type Context struct {
	// MaxScale overrides the MaxScale constant so long as it's
	// in the range (0, MaxScale].
	MaxScale int

	// MinScale overrides the MaxScale constant so long as it's
	// in the range [MinScale, 0).
	MinScale int

	// Precision is the Context's precision; that is, the maximum
	// number of significant digits that may result from any
	// arithmetic operation.  Excluding any package-defined
	// constants (e.g., "UnlimitedPrecision"), if precision is
	// not in the range [1, MaxPrecision] operations might result
	// in an error. A precision of 0 will be interpreted as
	// DefaultPrecision. For example,
	//
	//   precision ==  4 // 4
	//   precision == -4 // error
	//   precision ==  0 // DefaultPrecision
	//   precision == 12 // 12
	//
	Precision int

	// Traps are a set of exceptional conditions that should
	// result in an error.
	Traps Condition

	// Conditions are a set of the most recent exceptional
	// conditions to occur during an operation.
	Conditions Condition

	// RoundingMode determines how a decimal is rounded.
	RoundingMode RoundingMode

	// OperatingMode which dictates how the decimal operates under certain
	// conditions. See OperatingMode for more information.
	OperatingMode OperatingMode
}

// dup returns the Context, but with a non-zero Precision.
func (c Context) dup() Context {
	ctx := c
	ctx.Precision = c.precision()
	return ctx
}

func (c Context) precision() int {
	if c.Precision != 0 {
		return c.Precision
	}
	return DefaultPrecision
}

func (c Context) emax() int {
	if c.MaxScale != 0 {
		return c.MaxScale
	}
	return MaxScale
}

func (c Context) emin() int {
	if c.MinScale != 0 {
		return c.MinScale
	}
	return MinScale
}

// Err returns non-nil if there are any trapped exceptional
// conditions.
func (c Context) Err() error {
	if m := c.Conditions & c.Traps; m != 0 {
		return m
	}
	return nil
}

// WithContext is shorthand to create a Big decimal from a Context.
func WithContext(c Context) *Big {
	z := new(Big)
	z.Context = c
	return z
}

// WithPrecision is shorthand to create a Big decimal with a given precision.
func WithPrecision(p int) *Big {
	z := new(Big)
	switch {
	case p > 0 && p <= UnlimitedPrecision:
		z.Context.Precision = p
	case p == 0:
		z.Context.Precision = DefaultPrecision
	default:
		z.setNaN(InvalidContext, qnan, invctxpgtu)
	}
	return z
}

// The following are called ContextXX instead of DecimalXX
// to reserve the DecimalXX namespace for future decimal types.

// The following Contexts are based on IEEE 754R. Each Context's
// RoundingMode is ToNearestEven, OperatingMode is GDA, and traps
// are set to every exception other than Inexact, Rounded, and
// Subnormal.
var (
	// Context32 is the IEEE 754R Decimal32 format.
	Context32 = Context{
		Precision:     7,
		RoundingMode:  ToNearestEven,
		OperatingMode: GDA,
		Traps:         ^(Inexact | Rounded | Subnormal),
		MaxScale:      96,
		MinScale:      -95,
	}

	// Context64 is the IEEE 754R Decimal64 format.
	Context64 = Context{
		Precision:     16,
		RoundingMode:  ToNearestEven,
		OperatingMode: GDA,
		Traps:         ^(Inexact | Rounded | Subnormal),
		MaxScale:      384,
		MinScale:      -383,
	}

	// Context128 is the IEEE 754R Decimal128 format.
	Context128 = Context{
		Precision:     34,
		RoundingMode:  ToNearestEven,
		OperatingMode: GDA,
		Traps:         ^(Inexact | Rounded | Subnormal),
		MaxScale:      6144,
		MinScale:      -6143,
	}

	// ContextUnlimited provides unlimited precision decimals.
	ContextUnlimited = Context{
		Precision:     UnlimitedPrecision,
		RoundingMode:  ToNearestEven,
		OperatingMode: GDA,
		Traps:         ^(Inexact | Rounded | Subnormal),
		MaxScale:      MaxScale,
		MinScale:      MinScale,
	}

	maxCtx = Context{
		Precision:     MaxPrecision,
		RoundingMode:  ToNearestEven,
		OperatingMode: GDA,
		Traps:         ^(Inexact | Rounded | Subnormal),
		MaxScale:      MaxScale,
		MinScale:      MinScale,
	}
)

// RoundingMode determines how a decimal will be rounded.
type RoundingMode uint8

// The following rounding modes are supported.
const (
	ToNearestEven       RoundingMode = iota // == IEEE 754-2008 roundTiesToEven
	ToNearestAway                           // == IEEE 754-2008 roundTiesToAway
	ToZero                                  // == IEEE 754-2008 roundTowardZero
	AwayFromZero                            // no IEEE 754-2008 equivalent
	ToNegativeInf                           // == IEEE 754-2008 roundTowardNegative
	ToPositiveInf                           // == IEEE 754-2008 roundTowardPositive
	ToNearestTowardZero                     // no IEEE 754-2008 equivalent

	unnecessary // placeholder for x / y with UnlimitedPrecision.
)

//go:generate stringer -type RoundingMode

func (m RoundingMode) needsInc(odd bool, r int, pos bool) bool {
	switch m {
	case AwayFromZero:
		return true // always up
	case ToZero:
		return false // always down
	case ToPositiveInf:
		return pos // up if positive
	case ToNegativeInf:
		return !pos // down if negative

	//  r <  0: closer to higher
	//  r == 0: halfway
	//  r >  0: closer to lower
	case ToNearestEven:
		if r != 0 {
			return r > 0
		}
		return odd
	case ToNearestAway:
		return r >= 0
	case ToNearestTowardZero:
		return r > 0
	default:
		return false
	}
}

// OperatingMode dictates how the decimal approaches specific non-numeric
// operations like conversions to strings and panicking on NaNs.
type OperatingMode uint8

const (
	// GDA strictly adheres to the General Decimal Arithmetic Specification
	// Version 1.70. In particular:
	//
	//  - at does not panic
	//  - all arithmetic operations will be rounded down to the proper precision
	//    if necessary
	//  - it utilizes traps to set both Context.Err and Context.Conditions
	//  - its string forms of qNaN, sNaN, +Inf, and -Inf are "NaN", "sNaN",
	//    "Infinity", and "-Infinity", respectively
	//
	GDA OperatingMode = iota
	// Go adheres to typical Go idioms. In particular:
	//
	//  - it panics on NaN values
	//  - has lossless (i.e., without rounding) addition, subtraction, and
	//    multiplication
	//  - traps are ignored; it does not set Context.Err or Context.Conditions
	//  - its string forms of qNaN, sNaN, +Inf, and -Inf are "NaN", "NaN",
	//     "+Inf", and "-Inf", respectively
	//
	Go
)

//go:generate stringer -type OperatingMode

// Condition is a bitmask value raised after or during specific operations. For
// example, dividing by zero is undefined so a DivisionByZero Condition flag
// will be set in the decimal's Context.
type Condition uint32

const (
	// Clamped occurs if the scale has been modified to fit the constraints of
	// the decimal representation.
	Clamped Condition = 1 << iota
	// ConversionSyntax occurs when a string is converted to a decimal and does
	// not have a valid syntax.
	ConversionSyntax
	// DivisionByZero occurs when division is attempted with a finite,
	// non-zero dividend and a divisor with a value of zero.
	DivisionByZero
	// DivisionImpossible occurs when the result of integer division would
	// contain too many digits (i.e. be longer than the specified precision).
	DivisionImpossible
	// DivisionUndefined occurs when division is attempted with in which both
	// the divided and divisor are zero.
	DivisionUndefined
	// Inexact occurs when the result of an operation (e.g. division) is not
	// exact, or when the Overflow/Underflow Conditions occur.
	Inexact
	// InsufficientStorage occurs when the system doesn't have enough storage
	// (i.e. memory) to store the
	InsufficientStorage
	// InvalidContext occurs when an invalid context was detected during an
	// operation. This might occur if, for example, an invalid RoundingMode was
	// passed to a Context.
	InvalidContext
	// InvalidOperation occurs when:
	//
	// 	- an operand to an operation is a signaling NaN
	// 	- an attempt is made to add or subtract infinities of opposite signs
	// 	- an attempt is made to multiply zero by an infinity of either sign
	// 	- an attempt is made to divide an infinity by an infinity
	// 	- the divisor for a remainder operation is zero
	// 	- the dividend for a remainder operation is an infinity
	// 	- either operand of the quantize operation is an infinity, or the result
	// 	  of a quantize operation would require greater precision than is
	// 	  available
	// 	- the operand of the ln or the log10 operation is less than zero
	// 	- the operand of the square-root operation has a sign of 1 and a
	// 	  non-zero coefficient
	// 	- both operands of the power operation are zero, or if the left-hand
	// 	  operand is less than zero and the right-hand operand does not have an
	// 	  integral value or is an infinity
	//
	InvalidOperation
	// Overflow occurs when the adjusted scale, after rounding, would be
	// greater than MaxScale. (Inexact and Rounded will also be raised.)
	Overflow
	// Rounded occurs when the result of an operation is rounded, or if an
	// Overflow/Underflow occurs.
	Rounded
	// Subnormal ocurs when the result of a conversion or operation is subnormal
	// (i.e. the adjusted scale is less than MinScale before any rounding).
	Subnormal
	// Underflow occurs when the result is inexact and the adjusted scale would
	// be smaller (more negative) than MinScale.
	Underflow
)

func (c Condition) Error() string { return c.String() }

func (c Condition) String() string {
	if c == 0 {
		return ""
	}

	var b strings.Builder
	for i := Condition(1); c != 0; i <<= 1 {
		if c&i == 0 {
			continue
		}
		switch c ^= i; i {
		case Clamped:
			b.WriteString("clamped, ")
		case ConversionSyntax:
			b.WriteString("conversion syntax, ")
		case DivisionByZero:
			b.WriteString("division by zero, ")
		case DivisionImpossible:
			b.WriteString("division impossible, ")
		case DivisionUndefined:
			b.WriteString("division undefined, ")
		case Inexact:
			b.WriteString("inexact, ")
		case InsufficientStorage:
			b.WriteString("insufficient storage, ")
		case InvalidContext:
			b.WriteString("invalid context, ")
		case InvalidOperation:
			b.WriteString("invalid operation, ")
		case Overflow:
			b.WriteString("overflow, ")
		case Rounded:
			b.WriteString("rounded, ")
		case Subnormal:
			b.WriteString("subnormal, ")
		case Underflow:
			b.WriteString("underflow, ")
		default:
			fmt.Fprintf(&b, "unknown(%d), ", i)
		}
	}
	// Omit trailing comma and space.
	return b.String()[:b.Len()-2]
}

var _ error = Condition(0)
