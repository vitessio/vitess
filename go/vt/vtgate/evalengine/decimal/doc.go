// Package decimal provides a high-performance, arbitrary precision,
// floating-point decimal library.
//
// Overview
//
// This package provides floating-point decimal numbers, useful for financial
// programming or calculations where a larger, more accurate representation of
// a number is required.
//
// In addition to basic arithmetic operations (addition, subtraction,
// multiplication, and division) this package offers various mathematical
// functions, including the exponential function, various logarithms, and the
// ability to compute continued fractions.
//
// While lean, this package is full of features. It implements interfaces like
// ``fmt.Formatter'' and intuitively utilizes verbs and flags as described in
// the ``fmt'' package. (Also included: ``fmt.Scanner'', ``fmt.Stringer'',
// ``encoding.TextUnmarshaler'', and ``encoding.TextMarshaler''.)
//
// It allows users to specific explicit contexts for arithmetic operations, but
// doesn't require it. It provides access to NaN payloads and is more lenient
// when parsing a decimal from a string than the GDA specification requires.
//
// API interfaces have been changed slightly to work more seamlessly with
// existing Go programs. For example, many ``Quantize'' implementations require
// a decimal as both the receiver and argument which isn't very user friendly.
// Instead, this library accepts a simple ``int'' which can be derived from an
// existing decimal if required.
//
// It contains two modes of operation designed to make transitioning to various
// GDA "quirks" (like always rounding lossless operations) easier.
//
//     GDA: strictly adhere to the GDA specification (default)
//     Go: utilize Go idioms, more flexibility
//
// Goals
//
// There are three primary goals of this library:
//
//     1. Correctness
//
// By adhering to the General Decimal Arithmetic specification, this package
// has a well-defined structure for its arithmetic operations.
//
//     2. Performance
//
// Decimal libraries are inherently slow; this library works diligently to
// minimize memory allocations and utilize efficient algorithms. Performance
// regularly benchmarks as fast or faster than many other popular decimal
// libraries.
//
//     3. Ease of use
//
// Libraries should be intuitive and work out of the box without having to
// configure too many settings; however, precise settings should still be
// available.
//
// Usage
//
// The following type is supported:
//
//     Big decimal numbers
//
// The zero value for a Big corresponds with 0, meaning all the following are
// valid:
//
//     var x Big
//     y := new(Big)
//     z := &Big{}
//
// Method naming is the same as math/big's, meaning:
//
//     func (z *T) SetV(v V) *T          // z = v
//     func (z *T) Unary(x *T) *T        // z = unary x
//     func (z *T) Binary(x, y *T) *T    // z = x binary y
//     func (x *T) Pred() P              // p = pred(x)
//
// In general, its conventions mirror math/big's. It is suggested to read the
// math/big package comments to gain an understanding of this package's
// conventions.
//
// Arguments to Binary and Unary methods are allowed to alias, so the following
// is valid:
//
//     x := New(1, 0)
//     x.Add(x, x) // x == 2
//
//     y := New(1, 0)
//     y.FMA(y, x, y) // y == 3
//
// Unless otherwise specified, the only argument that will be modified is the
// result (``z''). This means the following is valid and race-free:
//
//    x := New(1, 0)
//    var g1, g2 Big
//
//    go func() { g1.Add(x, x) }()
//    go func() { g2.Add(x, x) }()
//
// But this is not:
//
//    x := New(1, 0)
//    var g Big
//
//    go func() { g.Add(x, x) }() // BAD! RACE CONDITION!
//    go func() { g.Add(x, x) }() // BAD! RACE CONDITION!
//
package decimal
