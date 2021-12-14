package decimal

import (
	"math"
	"math/big"
	"math/bits"

	"vitess.io/vitess/go/vt/vtgate/evalengine/decimal/internal/arith"
	cst "vitess.io/vitess/go/vt/vtgate/evalengine/decimal/internal/c"
)

// Abs sets z to the absolute value of x and returns z.
func (c Context) Abs(z, x *Big) *Big {
	if debug {
		x.validate()
	}
	if !z.invalidContext(z.Context) && !z.checkNaNs(x, x, absvalue) {
		c.finish(z.copyAbs(x))
	}
	return z
}

// Add sets z to x + y and returns z.
func (c Context) Add(z, x, y *Big) *Big {
	if debug {
		x.validate()
		y.validate()
	}
	if z.invalidContext(c) {
		return z
	}

	if x.IsFinite() && y.IsFinite() {
		z.form = c.add(z, x, x.form, y, y.form)
		return c.finish(z)
	}

	// NaN + NaN
	// NaN + y
	// x + NaN
	if z.checkNaNs(x, y, addition) {
		return z
	}

	if x.form&inf != 0 {
		if y.form&inf != 0 && x.form^y.form == signbit {
			// +Inf + -Inf
			// -Inf + +Inf
			return z.setNaN(InvalidOperation, qnan, addinfinf)
		}
		// ±Inf + y
		// +Inf + +Inf
		// -Inf + -Inf
		return c.Set(z, x)
	}
	// x + ±Inf
	return c.Set(z, y)
}

// add sets z to x + y, where x and y are both finite.
//
// The (*Big).form fields are ignored and must be provided as
// separate arguments in order to facilitate Context.Sub.
func (c Context) add(z, x *Big, xform form, y *Big, yform form) form {
	// addCompact, addMixed, and addBig all require X be the
	// "shifted" number, which means X must have the greater
	// exponent.
	hi, lo := x, y
	hisign, losign := xform, yform
	if hi.exp < lo.exp {
		hi, lo = lo, hi
		hisign, losign = losign, hisign
	}

	if sign, ok := c.tryTinyAdd(z, hi, hisign, lo, losign); ok {
		return sign
	}

	var sign form
	if hi.isCompact() {
		if lo.isCompact() {
			sign = c.addCompact(z, hi.compact, hisign, lo.compact, losign, uint64(hi.exp-lo.exp))
		} else {
			sign = c.addMixed(z, &lo.unscaled, losign, lo.exp, hi.compact, hisign, hi.exp)
		}
	} else if lo.isCompact() {
		sign = c.addMixed(z, &hi.unscaled, hisign, hi.exp, lo.compact, losign, lo.exp)
	} else {
		sign = c.addBig(z, &hi.unscaled, hisign, &lo.unscaled, losign, uint64(hi.exp-lo.exp))
	}
	z.exp = lo.exp
	return sign
}

// tryTinyAdd attempts to set z to X + Y, but only if the
// addition requires such a large shift that the result of the
// addition would be the same if Y were replaced with a smaller
// value.
//
// For example, given
//
//    X = 5 * 10^0      // 5
//    Y = 3 * 10^-99999 // 3e-99999
//
// X would have to be shifted (multiplied) by
//
//    shift = 10 ^ (0 - (-99999)) =
//            10 ^ (0 + 99999)    =
//            10^99999
//
// which is a *large* number.
//
// If the desired precision for the addition is 16, the end
// result will be
// rounded down to
//
//    5.0000000000000000
//
// making the shift entirely useless.
//
// Instead, Y can be replaced with a smaller number that rounds
// down to the same result and avoids large shifts.
//
// tryTinyAdd reports whether the "tiny" addition was performed.
func (c Context) tryTinyAdd(z *Big, X *Big, Xsign form, Y *Big, Ysign form) (form, bool) {
	if X.isZero() {
		return 0, false
	}

	exp := X.exp - 1
	if xp, zp := X.Precision(), c.precision(); xp <= zp {
		exp += xp - zp - 1
	}

	if Y.adjusted() >= exp {
		return 0, false
	}

	var tiny uint64
	if Y.compact != 0 {
		tiny = 1
	}

	var sign form
	if X.isCompact() {
		sign = c.addCompact(z, X.compact, Xsign, tiny, Ysign, uint64(X.exp-exp))
	} else {
		sign = c.addMixed(z, &X.unscaled, Xsign, X.exp, tiny, Ysign, exp)
	}
	z.exp = exp
	return sign, true
}

// addCompact sets z to X + Y where
//
//    X = X0 * 10^shift
//
// and returns the resulting signbit.
func (c Context) addCompact(z *Big, X0 uint64, Xsign form, Y uint64, Ysign form, shift uint64) form {
	// Test whether X0 * 10^shift fits inside a uint64. If not,
	// fall back to big.Ints.
	X, ok := arith.MulPow10(X0, shift)
	if !ok {
		X0 := z.unscaled.SetUint64(X0)
		X := arith.MulBigPow10(X0, X0, shift)
		// Because hi was promoted to a big.Int, it by definition
		// is larger than lo.
		//
		// Therefore, the resulting signbit is the same as hi's
		// signbit.
		//
		// Furthermore, we do not need to check if the result of
		// the operation is zero.
		if Xsign == Ysign {
			z.precision = arith.BigLength(arith.Add(&z.unscaled, X, Y))
			z.compact = cst.Inflated
		} else {
			arith.Sub(&z.unscaled, X, Y)
			z.norm()
		}
		return Xsign
	}

	// If the signs are the same, then X + Y = ℤ≠0.
	if Ysign == Xsign {
		if sum, c := bits.Add64(X, Y, 0); c == 0 {
			z.compact = sum
			if sum == cst.Inflated {
				z.unscaled.SetUint64(cst.Inflated)
			}
			z.precision = arith.Length(z.compact)
		} else {
			arith.Set(&z.unscaled, c, sum)
			z.precision = 20
			z.compact = cst.Inflated
		}
		return Xsign
	}

	sign := Xsign
	// X + (-Y) == X - Y == -(Y - X)
	// (-X) + Y == Y - X == -(X - Y)
	diff, b := bits.Sub64(X, Y, 0)
	if b != 0 {
		sign ^= signbit
		diff = Y - X
	}

	if diff != 0 {
		z.compact = diff
		z.precision = arith.Length(z.compact)
		return sign
	}

	sign = 0
	// On a zero result:
	//
	//    Otherwise, the sign of a zero result is 0 unless either
	//    both operands were negative or the signs of the
	//    operands were different and the rounding is
	//    round-floor.
	//
	// http://speleotrove.com/decimal/daops.html#refaddsub
	if c.RoundingMode == ToNegativeInf {
		sign = Xsign ^ Ysign // either 0 or 1
	}
	sign |= Xsign & Ysign

	z.compact = 0
	z.precision = 1
	return sign
}

// addMixed sets z to X + Y where
//
//    X = X * 10^shift
//
// and returns the resulting signbit.
func (c Context) addMixed(z *Big, X *big.Int, Xform form, xs int, Y uint64, Yform form, ys int) form {
	if xs < ys {
		shift := uint64(ys - xs)
		Y0, ok := arith.MulPow10(Y, shift)
		if !ok {
			yb := alias(&z.unscaled, X).SetUint64(Y)
			yb = arith.MulBigPow10(yb, yb, shift)
			return c.addBig(z, X, Xform, yb, Yform, 0)
		}
		Y = Y0
	} else if xs > ys {
		X = arith.MulBigPow10(&z.unscaled, X, uint64(xs-ys))
	}

	if Xform == Yform {
		arith.Add(&z.unscaled, X, Y)
		z.precision = arith.BigLength(&z.unscaled)
		z.compact = cst.Inflated
	} else {
		// X > Y
		arith.Sub(&z.unscaled, X, Y)
		z.norm()
	}
	return Xform
}

// addBig sets z to X + Y where
//
//    X = X0 * 10^shift
//
// and returns the resulting signbit.
func (c Context) addBig(z *Big, X *big.Int, Xsign form, Y *big.Int, Ysign form, shift uint64) form {
	// Guard the call so we don't allocate (via alias) if we don't need to.
	if shift != 0 {
		X = arith.MulBigPow10(alias(&z.unscaled, Y), X, shift)
	}

	if Xsign == Ysign {
		z.unscaled.Add(X, Y)
		z.compact = cst.Inflated
		z.precision = arith.BigLength(&z.unscaled)
		return Xsign
	}

	sign := Xsign
	// X + (-Y) == X - Y == -(Y - X)
	// (-X) + Y == Y - X == -(X - Y)
	if X.Cmp(Y) >= 0 {
		z.unscaled.Sub(X, Y)
	} else {
		sign ^= signbit
		z.unscaled.Sub(Y, X)
	}

	if z.unscaled.Sign() == 0 {
		z.compact = 0
		z.precision = 1
		sign = 0
		if c.RoundingMode == ToNegativeInf {
			sign = Xsign ^ Ysign // either 0 or 1
		}
		return sign | Xsign&Ysign
	}

	z.norm()
	return sign
}

// Acos returns the arccosine, in radians, of x.
//
// Range:
//     Input: -1 <= x <= 1
//     Output: 0 <= Acos(x) <= pi
//
// Special cases:
//     Acos(NaN)  = NaN
//     Acos(±Inf) = NaN
//     Acos(x)    = NaN if x < -1 or x > 1
//     Acos(-1)   = pi
//     Acos(1)    = 0
func (c Context) Acos(z, x *Big) *Big {
	if debug {
		x.validate()
	}
	if z.invalidContext(c) {
		return z
	}
	if z.checkNaNs(x, x, acos) {
		return z
	}

	cmp1 := cmpAbsInt(x, 1)
	if x.IsInf(0) || cmp1 > 0 {
		z.Context.Conditions |= InvalidOperation
		return z.SetNaN(false)
	}

	if cmp1 == 0 {
		if x.Signbit() {
			return c.pi(z)
		}
		return z.SetUint64(0)
	}

	ctx := c.dup()
	ctx.Precision += defaultExtraPrecision

	// Acos(x) = pi/2 - arcsin(x)

	pi2 := ctx.pi2(getDec(ctx))
	ctx.Sub(z, pi2, ctx.Asin(z, x))
	putDec(pi2)
	return c.finish(z)
}

// Asin returns the arcsine, in radians, of x.
//
// Range:
//     Input: -1 <= x <= 1
//     Output: -pi/2 <= Asin(x) <= pi/2
//
// Special cases:
//		Asin(NaN)  = NaN
//		Asin(±Inf) = NaN
//		Asin(x)    = NaN if x < -1 or x > 1
//		Asin(±1)   = ±pi/2
func (c Context) Asin(z, x *Big) *Big {
	if debug {
		x.validate()
	}
	if z.invalidContext(c) {
		return z
	}
	if z.checkNaNs(x, x, asin) {
		return z
	}

	cmp1 := cmpAbsInt(x, 1)
	if x.IsInf(0) || cmp1 > 0 {
		z.Context.Conditions |= InvalidOperation
		return z.SetNaN(false)
	}

	if cmp1 == 0 {
		c.pi2(z)
		if x.Signbit() {
			z.SetSignbit(true)
		}
		return z
	}

	ctx := c.dup()
	ctx.Precision += defaultExtraPrecision

	// Asin(x) = 2 * atan(x / (1 + sqrt(1 - x*x)))

	if z == x {
		x = getDec(c).Copy(x)
		defer putDec(x)
	}
	c.Mul(z, x, x)           //                            x*x
	ctx.Sub(z, one.get(), z) //                        1 - x*x
	ctx.Sqrt(z, z)           //                   sqrt(1 - x*x)
	ctx.Add(z, one.get(), z) //               1 + sqrt(1 - x*x)
	ctx.Quo(z, x, z)         //          x / (1 + sqrt(1 - x*x))
	ctx.Atan(z, z)           //     atan(x / (1 + sqrt(1 - x*x)))
	ctx.Mul(z, z, two.get()) // 2 * atan(x / (1 + sqrt(1 - x*x)))

	return c.finish(z)
}

// Atan returns the arctangent, in radians, of x.
//
// Range:
//     Input: all real numbers
//     Output: -pi/2 <= Atan(x) <= pi/2
//
// Special cases:
//		Atan(NaN)  = NaN
//		Atan(±Inf) = ±x * pi/2
func (c Context) Atan(z, x *Big) *Big {
	if debug {
		x.validate()
	}
	if z.invalidContext(c) {
		return z
	}
	if z.checkNaNs(x, x, atan) {
		return z
	}

	if x.IsInf(0) {
		c.pi2(z)
		z.form |= x.form & signbit
		return z
	}

	ctx := c.dup()
	ctx.Precision += defaultExtraPrecision

	// when x <-1 we use -atan(-x) instead
	if cmpInt(x, -1) < 0 {
		z.CopyNeg(x)       //       -x
		ctx.Atan(z, z)     //  atan(-x)
		return c.Neg(z, z) // -atan(-x)
	}

	y, ySq, ySqPlus1, segment, halved := ctx.prepAtan(z, x) // z == y, maybe.
	result := BinarySplitDynamic(c,
		func(_ uint64) *Big { return y },
		ctx.getAtanP(ySq),
		func(_ uint64) *Big { return one.get() },
		ctx.getAtanQ(ySqPlus1),
	)

	// undo the double angle part
	tmp := ctx.Pow(ySq, two.get(), z.SetMantScale(int64(halved), 0)) // clobber ySq
	ctx.Mul(z, result, tmp)

	// to handle the argument reduction step
	// check which segment the value was from
	// seg 0: 0 < value <= sqrt(3)/3  // then result = result
	// seg 1: sqrt(3)/3 < value <= 1  // then result = pi/6 + result
	// set 2: 1 < value               // then result = pi/2 - result
	switch segment {
	case 1:
		piOver6 := c.pi(tmp) // clobber _2p
		ctx.Quo(piOver6, piOver6, six.get())
		ctx.Add(z, piOver6, z)
	case 2:
		ctx.Sub(z, ctx.pi2(tmp), z) // clobber _2p
	}
	return c.finish(z)
}

var sqrt3_3 = New(577350, 6) // sqrt(3) / 3

func (c Context) prepAtan(z, x *Big) (y, ySq, ySqPlus1 *Big, segment, halved int) {
	c.Precision += defaultExtraPrecision

	if x == z {
		x = getDec(c).Copy(x)
		defer putDec(x)
	}

	// since smaller values converge faster
	// we'll use argument reduction
	// if |x| > 1 then set x = 1/x
	segment = 0
	switch {
	case cmpAbsInt(x, 1) > 0:
		segment = 2
		c.Quo(z, one.get(), x)
	case x.CmpAbs(sqrt3_3) > 0:
		// if |x| > sqrt(3)/3 (approximated to 0.577350)
		segment = 1

		// then set x = (sqrt(3)*x-1)/(sqrt(3)+x)
		sqrt3 := c.sqrt3(new(Big))
		c.Mul(z, sqrt3, x)     // sqrt(3) * x
		c.Sub(z, z, one.get()) // sqrt(3)*x - 1
		c.Add(sqrt3, sqrt3, x) // sqrt(3) + x
		c.Quo(z, z, sqrt3)     // (sqrt(3)*x - 1) / (sqrt(3) + x)
	default:
		z.Copy(x)
	}

	// next we'll use argument halving technic
	// atan(y) = 2 atan(y/(1+sqrt(1+y^2)))
	// we'll repeat this up to a point
	// we have competing operations at some
	// point the sqrt causes a problem
	// note (http://fredrikj.net/blog/2014/12/faster-high-ctx-Atangents/)
	// suggests that r= 8 times is a good value for
	// precision with 1000s of digits to millions
	// however it was easy to determine there is a
	// better sliding window to use instead
	// which is what we use as it turns out
	// when the ctx is large, a larger r value
	// compared to 8 is every effective
	xf, _ := z.Float64()
	xf = math.Abs(xf)
	// the formula simple but works a bit better then a fixed value (8)
	r := math.Max(0, math.Ceil(0.31554321636851*math.Pow(float64(c.Precision), 0.654095561044508)))
	var p float64

	// maxPrec is the largest precision value we can use bit shifts instead of
	// math.Pow which is more expensive.
	const maxPrec = 3286
	if c.Precision <= maxPrec {
		p = 1 / float64(uint64(1)<<uint64(r))
	} else {
		p = math.Pow(2, -r)
	}
	halved = int(math.Ceil(math.Log(xf/p) / math.Ln2))

	// if the value is already less than 1/(2^r) then halfed
	// will be negative and we don't need to apply
	// the double angle formula because it would hurt performance
	// so we'll set halfed to zero
	if halved < 0 {
		halved = 0
	}

	sq := getDec(c)
	for i := 0; i < halved; i++ {
		c.FMA(sq, z, z, one.get())
		c.Sqrt(sq, sq)
		c.Add(sq, sq, one.get())
		c.Quo(z, z, sq)
	}
	putDec(sq)

	var x2 Big
	c.Mul(&x2, z, z)
	var x2p1 Big
	c.Add(&x2p1, &x2, one.get())
	return z, &x2, &x2p1, segment, halved
}

func (c Context) getAtanP(x2 *Big) SplitFunc {
	var p Big
	return func(n uint64) *Big {
		// P(n) = 2n for all n > 0
		if n == 0 {
			return one.get()
		}
		if n < math.MaxUint64/2 {
			p.SetUint64(n * 2)
		} else {
			c.Mul(&p, p.SetUint64(n), two.get())
		}
		return c.Mul(&p, &p, x2)
	}
}

func (c Context) getAtanQ(x2p1 *Big) SplitFunc {
	var q Big
	return func(n uint64) *Big {
		// B(n) = (2n+1) for all n >= 0

		// atanMax is the largest number we can use to compute (2n + 1) without
		// overflow.
		const atanMax = (math.MaxUint64 - 1) / 2
		if n < atanMax {
			q.SetUint64((2 * n) + 1)
		} else {
			c.FMA(&q, q.SetUint64(n), two.get(), one.get())
		}
		return c.Mul(&q, &q, x2p1)
	}
}

// Atan2 calculates arctan of y/x and uses the signs of y and x to determine
// the valid quadrant
//
// Range:
//     y input: all real numbers
//     x input: all real numbers
//     Output: -pi < Atan2(y, x) <= pi
//
// Special cases:
//     Atan2(NaN, NaN)      = NaN
//     Atan2(y, NaN)        = NaN
//     Atan2(NaN, x)        = NaN
//     Atan2(±0, x >=0)     = ±0
//     Atan2(±0, x <= -0)   = ±pi
//     Atan2(y > 0, 0)      = +pi/2
//     Atan2(y < 0, 0)      = -pi/2
//     Atan2(±Inf, +Inf)    = ±pi/4
//     Atan2(±Inf, -Inf)    = ±3pi/4
//     Atan2(y, +Inf)       = 0
//     Atan2(y > 0, -Inf)   = +pi
//     Atan2(y < 0, -Inf)   = -pi
//     Atan2(±Inf, x)       = ±pi/2
//     Atan2(y, x > 0)      = Atan(y/x)
//     Atan2(y >= 0, x < 0) = Atan(y/x) + pi
//     Atan2(y < 0, x < 0)  = Atan(y/x) - pi
func (c Context) Atan2(z, y, x *Big) *Big {
	if debug {
		x.validate()
	}
	if z.invalidContext(c) {
		return z
	}
	if z.checkNaNs(x, x, atan) {
		return z
	}

	ctx := c.dup()
	ctx.Precision += defaultExtraPrecision

	switch {
	// Special values.
	case x.isSpecial(), x.isZero(), y.isSpecial(), y.isZero():
		// Save the signbit in case z == y.
		sb := y.form & signbit
		ctx.atan2Specials(z, x, y)
		z.form |= sb
	default:
		ctx.Atan(z, ctx.Quo(z, y, x))
		if x.Sign() < 0 {
			pi := ctx.pi(getDec(ctx))
			if z.Sign() <= 0 {
				ctx.Add(z, z, pi)
			} else {
				ctx.Sub(z, z, pi)
			}
			putDec(pi)
		}
	}
	return c.finish(z)
}

// atan2Specials handles atan2 special cases.
//
// Rounding and sign handling are performed by Atan2.
func (c Context) atan2Specials(z, x, y *Big) *Big {
	xs := x.Sign()
	if y.Sign() == 0 {
		if xs >= 0 && !x.Signbit() {
			// Atan2(0, x >= +0)  = ±0
			return z.setZero(0, 0)
		}
		// Atan2(0, x <= -0) = ±pi
		return c.pi(z)
	}
	if xs == 0 {
		// Atan2(y, 0) = ±pi/2
		return c.pi2(z)
	}
	if x.IsInf(0) {
		if x.IsInf(+1) {
			if y.IsInf(0) {
				// Atan2(±Inf, +Inf) = ±pi/4
				return c.Quo(z, c.pi(z), four.get())
			}
			// Atan2(y, +Inf) = ±0
			return z.SetUint64(0)
		}
		if y.IsInf(0) {
			// Atan2(±Inf, -Inf) = ±3 * pi/4
			c.Quo(z, c.pi(z), four.get())
			return c.Mul(z, z, three.get())
		}
		// Atan2(y, -Inf) = ±pi
		return c.pi(z)
	}
	if y.IsInf(0) {
		// Atan2(±Inf, x) = ±pi/2
		return c.pi2(z)
	}
	panic("unreachable")
}

// Cos returns the cosine, in radians, of x.
//
// Range:
//     Input: all real numbers
//     Output: -1 <= Cos(x) <= 1
//
// Special cases:
//		Cos(NaN)  = NaN
//		Cos(±Inf) = NaN
func (c Context) Cos(z, x *Big) *Big {
	if debug {
		x.validate()
	}
	if z.invalidContext(c) {
		return z
	}
	if z.checkNaNs(x, x, sin) {
		return z
	}

	if x.IsInf(0) {
		z.Context.Conditions |= InvalidOperation
		return z.SetNaN(false)
	}

	ctx := c.dup()
	ctx.Precision += defaultExtraPrecision

	negXSq, halved, ok := ctx.prepCosine(z, x)
	if !ok {
		z.Context.Conditions |= InvalidOperation
		return z.SetNaN(false)
	}

	ctx.Precision += halved
	z.Copy(BinarySplitDynamic(ctx,
		func(_ uint64) *Big { return one.get() },
		getCosineP(negXSq),
		func(_ uint64) *Big { return one.get() },
		ctx.getCosineQ(),
	))

	// now undo the half angle bit
	for i := 0; i < halved; i++ {
		ctx.Mul(z, z, z)
		ctx.Mul(z, z, two.get())
		ctx.Sub(z, z, one.get())
	}

	return c.finish(z)
}

func (c Context) prepCosine(z, x *Big) (*Big, int, bool) {
	if z == x {
		x = getDec(c).Copy(x)
		defer putDec(x)
	}

	var tmp Big
	var twoPi Big

	// For better results, we need to make sure the value we're
	// working with a value is closer to zero.
	c.Mul(&twoPi, c.pi(&twoPi), two.get()) // 2 * Pi
	if x.CmpAbs(&twoPi) >= 0 {
		// For cos to work correctly the input must be in (-2Pi,
		// 2Pi).
		c.Quo(&tmp, x, &twoPi)
		v, ok := tmp.Int64()
		if !ok {
			return nil, 0, false
		}
		uv := arith.Abs(v)

		// Adjust so we have ceil(v/10) + ctx.Precision, but check for overflows.
		// 1+((v-1)/10) will be wildly incorrect for v == 0, but x/y = 0 iff
		// x = 0 and y != 0. In this case, -2pi <= x >= 2pi, so we're fine.
		prec, carry := bits.Add64(1+((uv-1)/10), uint64(c.Precision), 0)
		if carry != 0 || prec > arith.MaxInt {
			return nil, 0, false
		}
		pctx := Context{Precision: int(prec)}

		if uv <= math.MaxInt64/2 {
			tmp.SetMantScale(v, 0)
		}

		pctx.Mul(&tmp, &twoPi, &tmp)

		// so toRemove = 2*Pi*v so x - toRemove < 2*Pi
		c.Sub(z, x, &tmp)
	} else {
		z.Copy(x)
	}

	// add 1 to the precision for the up eventual squaring.
	c.Precision++

	// now preform the half angle.
	// we'll repeat this up to log(precision)/log(2) and keep track
	// since we'll be dividing x we need to give a bit more precision
	// we'll be repeated applying the double angle formula
	// we could use a higher angle formula but wouldn't buy us anything.
	// Each time we half we'll have to increase the values precision by 1
	// and since we'll dividing at most 11 time that means at most 11 digits
	// but we'll figure out the minimum time we'll apply the double angle
	// formula

	// we'll we reduce until it's x <= p where p= 1/2^8 (approx 0.0039)
	// we figure out the number of time to divide by solving for r
	// in x/p = 2^r  so r = log(x/p)/log(2)

	xf, _ := z.Float64()
	// We only need to do the calculation if xf >= 0.0004. Anything below that
	// and we're <= 0.
	var halved int
	if xf = math.Abs(xf); xf >= 0.0004 {
		// Originally: ceil(log(xf/0.0048828125) / ln2)
		halved = int(math.Ceil(1.4427*math.Log(xf) + 11))
		// The general case is halved > 0, since we only get 0 if xf is very
		// close to 0.0004.
		if halved > 0 {
			// Increase the precision based on the number of divides. Overflow is
			// unlikely, but possible.
			c.Precision += halved
			if c.Precision <= halved {
				return nil, 0, false
			}
			// The maximum value for halved will be 14, given
			//     ceil(1.4427*log(2*pi)+11) = 14
			c.Quo(z, z, tmp.SetUint64(1<<uint64(halved)))
		}
	}

	c.Mul(z, z, z)
	z.CopyNeg(z)
	return z, halved, true
}

func getCosineP(negXSq *Big) func(n uint64) *Big {
	return func(n uint64) *Big {
		if n == 0 {
			return one.get()
		}
		return negXSq
	}
}

func (c Context) getCosineQ() func(n uint64) *Big {
	var q, tmp Big
	return func(n uint64) *Big {
		// (0) = 1, q(n) = 2n(2n-1) for n > 0
		if n == 0 {
			return one.get()
		}

		// most of the time n will be a small number so
		// use the fastest method to calculate 2n(2n-1)
		const cosine4NMaxN = 2147483648
		if n < cosine4NMaxN {
			// ((n*n) << 2) - (n << 1)
			return q.SetUint64((2 * n) * (2*n - 1))
		}
		q.SetUint64(n)
		c.Mul(&tmp, &q, two.get())
		c.Mul(&q, &tmp, &tmp)
		return c.Sub(&q, &q, &tmp)
	}
}

// Ceil sets z to the least integer value greater than or equal
// to x and returns z.
func (c Context) Ceil(z, x *Big) *Big {
	// ceil(x) = -floor(-x)
	return c.Neg(z, c.Floor(z, z.CopyNeg(x)))
}

// E sets z to the mathematical constant e and returns z.
func (c Context) E(z *Big) *Big {
	if c.Precision <= constPrec {
		return c.Set(z, _E.get())
	}

	var (
		sum  = z.SetUint64(2)
		fac  = new(Big).SetUint64(1)
		term = new(Big)
		prev = new(Big)
	)

	ctx := c
	ctx.Precision += 5
	for i := uint64(2); sum.Cmp(prev) != 0; i++ {
		// Use term as our intermediate storage for our
		// factorial. SetUint64 should be marginally faster than
		// ctx.Add(incr, incr, one), but either the costly call
		// to Quo makes it difficult to notice.
		term.SetUint64(i)
		ctx.Mul(fac, fac, term)
		ctx.Quo(term, one.get(), fac)
		prev.Copy(sum)
		ctx.Add(sum, sum, term)
	}
	return ctx.Set(z, sum)
}

// Exp sets z to e**x and returns z.
func (c Context) Exp(z, x *Big) *Big {
	if debug {
		x.validate()
	}
	if z.invalidContext(c) {
		return z
	}
	if z.checkNaNs(x, x, exp) {
		return z
	}

	if x.IsInf(0) {
		if x.IsInf(+1) {
			// e ** +Inf = +Inf
			return z.SetInf(false)
		}
		// e ** -Inf = 0
		return z.SetUint64(0)
	}

	if x.isZero() {
		// e ** 0 = 1
		return z.SetUint64(1)
	}

	z = c.exp(z, x)
	if z.IsFinite() && z.Sign() != 0 && z.Precision() < c.Precision {
		s := c.Precision - z.Precision()
		c.shiftl(z, uint64(s))
		z.exp -= s
	}
	return c.finish(z)
}

func (c Context) exp(z, x *Big) *Big {
	if getmsd(x, 1) == 0 {
		return z.SetMantScale(1, 0)
	}

	t := x.Precision() + x.exp
	if t < 0 {
		t = 0
	}
	const expMax = 19
	if t > expMax {
		z.Context.Conditions |= Inexact | Rounded
		if x.Signbit() {
			z.Context.Conditions |= Subnormal | Underflow | Clamped
			return z.SetMantScale(0, -c.etiny())
		}
		z.Context.Conditions |= Overflow
		return z.SetInf(false)
	}

	// |x| <= 9 * 10**-(prec + 1)
	lim := z
	if lim == x {
		lim = getDec(c)
		defer putDec(lim)
	}
	lim.SetMantScale(9, c.Precision+1)
	if x.CmpAbs(lim) <= 0 {
		z.Context.Conditions |= Rounded | Inexact
		return z.SetMantScale(1, 0)
	}

	if x.IsInt() {
		if v, ok := x.Uint64(); ok && v == 1 {
			// e ** 1 = e
			return c.E(z)
		}
	}

	// Argument reduction:
	//    exp(x) = e**r ** 10**k where x = r * 10**k
	z.Copy(x)
	z.exp -= t

	// TODO(eric): figure out if it's possible to make Horner's
	// method faster than continued fractions for small
	// precisions.
	if false && c.Precision <= 300 {
		c.expSmall(z, t)
	} else {
		c.expLarge(z, t)
	}
	return z
}

// expSmall returns exp(z) using Horner's scheme.
//
// The algorithm is taken from "Variable precision exponential
// function." by T. E. Hull and A. Abrham. 1986.
// https://dl.acm.org/doi/10.1145/6497.6498
func (c Context) expSmall(z *Big, t int) *Big {
	p := c.Precision + t + 2
	if p < 10 {
		p = 10
	}
	c.Precision = p

	pbyr := (arith.Length(uint64(p)) - 1) - (z.adjusted() + 1)
	n := int(math.Ceil((1.435*float64(p) - 1.182) / float64(pbyr)))
	if n < 3 {
		n = 3
	}

	var tmp Big
	var sum Big
	sum.SetMantScale(1, 0) // sum := 1
	for i := n - 1; i >= 1; i-- {
		tmp.SetMantScale(int64(i), 0)
		c.Quo(&tmp, z, &tmp)               // r/i
		c.FMA(&sum, &sum, &tmp, one.get()) // sum = sum*(r/i) + 1
	}
	w, _ := arith.Pow10(uint64(t)) // t in [0, 19]
	return c.powUint64(z, &sum, w)
}

// expLarge returns exp(z) using continued fractions.
func (c Context) expLarge(z *Big, t int) *Big {
	c.Precision += t + 3
	if c.Precision < 10 {
		c.Precision = 10
	}
	g := expg{
		ctx: c,
		z:   z,
		pow: c.Mul(getDec(c), z, z),
		t: Term{
			A: getDec(c),
			B: getDec(c),
		},
	}

	c.Wallis(z, &g)

	putDec(g.pow)
	putDec(g.t.A)
	putDec(g.t.B)

	if t != 0 {
		w, _ := arith.Pow10(uint64(t)) // t <= 19
		c.powUint64(z, z, w)
	}
	return z
}

// expg is a Generator that computes exp(z).
type expg struct {
	ctx Context
	z   *Big   // Input value
	pow *Big   // z*z
	m   uint64 // Term number
	t   Term   // Term storage. Does not need to be manually set.
}

var _ Walliser = (*expg)(nil)

func (e *expg) Context() Context {
	return e.ctx
}

func (e *expg) Next() bool {
	return true
}

func (e *expg) Wallis() (a, a1, b, b1, p, eps *Big) {
	a = new(Big)
	a1 = new(Big)
	b = new(Big)
	b1 = new(Big)
	p = new(Big)
	eps = New(1, e.ctx.Precision)
	return a, a1, b, b1, p, eps
}

func (e *expg) Term() Term {
	// exp(z) can be expressed as the following continued fraction
	//
	//     e^z = 1 +             2z
	//               ------------------------------
	//               2 - z +          z^2
	//                       ----------------------
	//                       6 +        z^2
	//                           ------------------
	//                           10 +     z^2
	//                                -------------
	//                                14 +   z^2
	//                                     --------
	//                                          ...
	//
	// (Khov, p 114)
	//
	// which can be represented as
	//
	//          2z    z^2 / 6    ∞
	//     1 + ----- ---------   K ((a_m^z^2) / 1), z ∈ ℂ
	//          2-z +    1     + m=3
	//
	// where
	//
	//     a_m = 1 / (4 * (2m - 3) * (2m - 1))
	//
	// which can be simplified to
	//
	//     a_m = 1 / (16 * (m-1)^2 - 4)
	//
	// (Cuyt, p 194).
	//
	// References:
	//
	// [Cuyt] - Cuyt, A.; Petersen, V.; Brigette, V.; Waadeland, H.; Jones, W.B.
	// (2008). Handbook of Continued Fractions for Special Functions. Springer
	// Netherlands. https://doi.org/10.1007/978-1-4020-6949-9
	//
	// [Khov] - Merkes, E. P. (1964). The Application of Continued Fractions and
	// Their Generalizations to Problems in Approximation Theory
	// (A. B. Khovanskii). SIAM Review, 6(2), 188–189.
	// https://doi.org/10.1137/1006052

	switch e.m {
	// [0, 1]
	case 0:
		e.t.A.SetUint64(0)
		e.t.B.SetUint64(1)
	// [2z, 2-z]
	case 1:
		e.ctx.Mul(e.t.A, two.get(), e.z)
		e.ctx.Sub(e.t.B, two.get(), e.z)
	// [z^2/6, 1]
	case 2:
		e.ctx.Quo(e.t.A, e.pow, six.get())
		e.t.B.SetUint64(1)
	// [(1/(16((m-1)^2)-4))(z^2), 1]
	default:
		// maxM is the largest m value we can use to compute 4(2m - 3)(2m - 1)
		// using unsigned integers.
		const maxM = 1518500252

		// 4(2m - 3)(2m - 1) ≡ 16(m - 1)^2 - 4
		if e.m <= maxM {
			e.t.A.SetUint64(16*((e.m-1)*(e.m-1)) - 4)
		} else {
			e.t.A.SetUint64(e.m - 1)

			// (m-1)^2
			e.ctx.Mul(e.t.A, e.t.A, e.t.A)

			// 16 * (m-1)^2 - 4 = 16 * (m-1)^2 + (-4)
			e.ctx.FMA(e.t.A, sixteen.get(), e.t.A, negFour.get())
		}

		// 1 / (16 * (m-1)^2 - 4)
		e.ctx.Quo(e.t.A, one.get(), e.t.A)

		// (1 / (16 * (m-1)^2 - 4)) * (z^2)
		e.ctx.Mul(e.t.A, e.t.A, e.pow)

		// e.t.B is set to 1 inside case 2.
	}

	e.m++
	return e.t
}

// Floor sets z to the greatest integer value less than or equal
// to x and returns z.
func (c Context) Floor(z, x *Big) *Big {
	if z.CheckNaNs(x, nil) {
		return z
	}
	c.RoundingMode = ToNegativeInf
	return c.RoundToInt(z.Copy(x))
}

// FMA sets z to (x * y) + u without any intermediate rounding.
func (c Context) FMA(z, x, y, u *Big) *Big {
	if z.invalidContext(c) {
		return z
	}
	// Create a temporary receiver if z == u so we handle the
	// z.FMA(x, y, z) without clobbering z partway through.
	z0 := z
	if z == u {
		z0 = WithContext(c)
	}
	c.mul(z0, x, y)
	if z0.Context.Conditions&InvalidOperation != 0 {
		return z.setShared(z0)
	}
	c.Add(z0, z0, u)
	return z.setShared(z0)
}

// Hypot sets z to Sqrt(p*p + q*q) and returns z.
func (c Context) Hypot(z, p, q *Big) *Big {
	if z.CheckNaNs(p, q) {
		return z
	}

	ctx := c.dup()
	ctx.Precision++

	p0 := getDec(ctx)
	ctx.Mul(p0, p, p)

	if p == q {
		ctx.Sqrt(z, ctx.Add(z, p0, p0))
	} else {
		q0 := getDec(ctx)
		ctx.Mul(q0, q, q)
		ctx.Sqrt(z, ctx.Add(z, p0, q0))
		putDec(q0)
	}
	putDec(p0)

	return c.finish(z)
}

// Log sets z to the natural logarithm of x and returns z.
func (c Context) Log(z, x *Big) *Big {
	if debug {
		x.validate()
	}
	if z.invalidContext(c) {
		return z
	}
	if z.checkNaNs(x, x, log10) {
		return z
	}
	if logSpecials(z, x) {
		return z
	}
	return c.finish(c.log(z, x, false))
}

// Log10 sets z to the common logarithm of x and returns z.
func (c Context) Log10(z, x *Big) *Big {
	if debug {
		x.validate()
	}
	if z.invalidContext(c) {
		return z
	}
	if z.checkNaNs(x, x, log10) {
		return z
	}

	if logSpecials(z, x) {
		return z
	}

	// If x is a power of 10 the result is the exponent and exact.
	var tpow bool
	if x.isCompact() {
		tpow = arith.PowOfTen(x.compact)
	} else {
		tpow = arith.PowOfTenBig(&x.unscaled)
	}
	if tpow {
		z.SetMantScale(int64(x.adjusted()), 0)
		return c.finish(z)
	}
	return c.finish(c.log(z, x, true))
}

// logSpecials checks for special values (Inf, NaN, 0) for
// logarithms.
func logSpecials(z, x *Big) bool {
	if s := x.Sign(); s <= 0 {
		if s == 0 {
			// log 0 = -Inf
			z.SetInf(true)
		} else {
			// log -x is undefined.
			z.Context.Conditions |= InvalidOperation
			z.SetNaN(false)
		}
		return true
	}

	if x.IsInf(+1) {
		// log +Inf = +Inf
		z.SetInf(false)
		return true
	}
	return false
}

// log set z to log(x), or log10(x) if ten.
func (c Context) log(z, x *Big, ten bool) *Big {
	if x.IsInt() {
		if v, ok := x.Uint64(); ok {
			switch v {
			case 1:
				// ln 1 = 0
				return z.SetMantScale(0, 0)
			case 10:
				// Specialized function.
				return c.ln10(z)
			}
		}
	}

	t := int64(x.adjusted())
	if t < 0 {
		t = -t - 1
	}
	t *= 2

	if arith.Length(arith.Abs(t))-1 > c.emax() {
		z.Context.Conditions |= Overflow | Inexact | Rounded
		return z.SetInf(t < 0)
	}

	// Argument reduction:
	// Given
	//    ln(a) = ln(b) + ln(c), where a = b * c
	// Given
	//    x = m * 10**n, where x ∈ ℝ
	// Reduce x (as y) so that
	//    1 <= y < 10
	// And create p so that
	//    x = y * 10**p
	// Compute
	//    log(y) + p*log(10)

	// At ~750 digits of precision, continued fractions become
	// faster.
	small := c.precision() < 750
	if ten || !small {
		c.Precision = c.precision() + arith.Length(uint64(c.precision()+x.Precision())) + 5
	}

	var p int64
	if small {
		p = c.logSmall(z, x)
	} else {
		p = c.logLarge(z, x)
	}

	if p != 0 || ten {
		wctx := c
		wctx.Precision = c.precision() + 1
		t := wctx.ln10Taylor(getDec(c))

		// Avoid doing unnecessary work.
		switch p {
		case 0:
			// OK
		case -1:
			wctx.Sub(z, z, t) // (-1 * t) + z = -t + z = z - t
		case 1:
			wctx.Add(z, t, z) // (+1 * t) + z = t + z
		default:
			tmp := getDec(c)
			tmp.SetMantScale(p, 0)
			wctx.FMA(z, tmp, t, z)
			putDec(tmp)
		}

		// We're calculating log10(x):
		//    log10(x) = log(x) / log(10)
		if ten {
			wctx.Quo(z, z, t)
		}
		putDec(t)
	}

	z.Context.Conditions |= Inexact | Rounded
	return z
}

// logSmall sets z = ln(x) for "small" precisions.
//
// The algorithm comes from libmpdec, which is under the BSD
// 2-clause license.
func (c Context) logSmall(z, x *Big) int64 {
	v := getDec(c).Copy(x)

	d := getmsd(v, 3)
	if d < 10 {
		d *= 10
	}
	if d < 100 {
		d *= 10
	}
	d -= 100

	xp := x.Precision()
	xs := x.exp

	z.SetMantScale(int64(lnapprox[d]), 3)

	var k int
	if d <= 400 {
		v.exp = -(xp - 1)
		k = xs + xp - 1
	} else {
		v.exp = -xp
		k = xs + xp
		z.CopyNeg(z)
	}

	prec := c.precision() + 2

	tmp := getDec(c)

	if k == 0 && (d <= 15 || d >= 800) {
		maxCtx.Sub(tmp, v, one.get())
		if tmp.IsNaN(0) {
			putDec(v)
			putDec(tmp)

			z.Context.Conditions |= Inexact | Rounded
			z.SetNaN(false)
			return 0
		}
		cmp := cmpInt(v, 1)
		if cmp < 0 {
			tmp.exp++
		}
		if tmp.adjusted() < c.etiny() {
			m := int64(1)
			if cmp < 0 {
				m = -1
			}
			z.SetMantScale(m, c.etiny()-1)
		}
		tmp.exp--
		if tmp.adjusted() < 0 {
			prec -= tmp.adjusted()
		}
	}

	s := sched(2, prec)
	for i := len(s) - 1; i >= 0; i-- {
		vctx := c
		vctx.Precision = 2*s[i] + 3
		vctx.RoundingMode = ToZero

		z.form ^= signbit
		vctx.exp(tmp, z)
		z.form ^= signbit

		if tmp.Precision() > vctx.Precision {
			vctx.Round(tmp)
		}
		vctx.Mul(tmp, tmp, v)

		maxCtx.Sub(tmp, tmp, one.get())
		maxCtx.Add(z, z, tmp)
		if !z.IsFinite() {
			break
		}
	}

	wctx := Context{Precision: prec + 1}
	v = wctx.ln10Taylor(v)
	tmp.SetMantScale(int64(k), 0)
	maxCtx.Mul(v, v, tmp)
	maxCtx.Add(z, z, v)

	putDec(v)
	putDec(tmp)
	return 0
}

var lnapprox = []uint16{
	// index 0 - 400: log((i+100)/100) * 1000
	0, 10, 20, 30, 39, 49, 58, 68, 77, 86, 95, 104, 113, 122, 131, 140, 148, 157,
	166, 174, 182, 191, 199, 207, 215, 223, 231, 239, 247, 255, 262, 270, 278,
	285, 293, 300, 308, 315, 322, 329, 336, 344, 351, 358, 365, 372, 378, 385,
	392, 399, 406, 412, 419, 425, 432, 438, 445, 451, 457, 464, 470, 476, 482,
	489, 495, 501, 507, 513, 519, 525, 531, 536, 542, 548, 554, 560, 565, 571,
	577, 582, 588, 593, 599, 604, 610, 615, 621, 626, 631, 637, 642, 647, 652,
	658, 663, 668, 673, 678, 683, 688, 693, 698, 703, 708, 713, 718, 723, 728,
	732, 737, 742, 747, 751, 756, 761, 766, 770, 775, 779, 784, 788, 793, 798,
	802, 806, 811, 815, 820, 824, 829, 833, 837, 842, 846, 850, 854, 859, 863,
	867, 871, 876, 880, 884, 888, 892, 896, 900, 904, 908, 912, 916, 920, 924,
	928, 932, 936, 940, 944, 948, 952, 956, 959, 963, 967, 971, 975, 978, 982,
	986, 990, 993, 997, 1001, 1004, 1008, 1012, 1015, 1019, 1022, 1026, 1030,
	1033, 1037, 1040, 1044, 1047, 1051, 1054, 1058, 1061, 1065, 1068, 1072, 1075,
	1078, 1082, 1085, 1089, 1092, 1095, 1099, 1102, 1105, 1109, 1112, 1115, 1118,
	1122, 1125, 1128, 1131, 1135, 1138, 1141, 1144, 1147, 1151, 1154, 1157, 1160,
	1163, 1166, 1169, 1172, 1176, 1179, 1182, 1185, 1188, 1191, 1194, 1197, 1200,
	1203, 1206, 1209, 1212, 1215, 1218, 1221, 1224, 1227, 1230, 1233, 1235, 1238,
	1241, 1244, 1247, 1250, 1253, 1256, 1258, 1261, 1264, 1267, 1270, 1273, 1275,
	1278, 1281, 1284, 1286, 1289, 1292, 1295, 1297, 1300, 1303, 1306, 1308, 1311,
	1314, 1316, 1319, 1322, 1324, 1327, 1330, 1332, 1335, 1338, 1340, 1343, 1345,
	1348, 1351, 1353, 1356, 1358, 1361, 1364, 1366, 1369, 1371, 1374, 1376, 1379,
	1381, 1384, 1386, 1389, 1391, 1394, 1396, 1399, 1401, 1404, 1406, 1409, 1411,
	1413, 1416, 1418, 1421, 1423, 1426, 1428, 1430, 1433, 1435, 1437, 1440, 1442,
	1445, 1447, 1449, 1452, 1454, 1456, 1459, 1461, 1463, 1466, 1468, 1470, 1472,
	1475, 1477, 1479, 1482, 1484, 1486, 1488, 1491, 1493, 1495, 1497, 1500, 1502,
	1504, 1506, 1509, 1511, 1513, 1515, 1517, 1520, 1522, 1524, 1526, 1528, 1530,
	1533, 1535, 1537, 1539, 1541, 1543, 1545, 1548, 1550, 1552, 1554, 1556, 1558,
	1560, 1562, 1564, 1567, 1569, 1571, 1573, 1575, 1577, 1579, 1581, 1583, 1585,
	1587, 1589, 1591, 1593, 1595, 1597, 1599, 1601, 1603, 1605, 1607, 1609,
	// index 401 - 899: -log((i+100)/1000) * 1000
	691, 689, 687, 685, 683, 681, 679, 677, 675, 673, 671, 669, 668, 666, 664,
	662, 660, 658, 656, 654, 652, 650, 648, 646, 644, 642, 641, 639, 637, 635,
	633, 631, 629, 627, 626, 624, 622, 620, 618, 616, 614, 612, 611, 609, 607,
	605, 603, 602, 600, 598, 596, 594, 592, 591, 589, 587, 585, 583, 582, 580,
	578, 576, 574, 573, 571, 569, 567, 566, 564, 562, 560, 559, 557, 555, 553,
	552, 550, 548, 546, 545, 543, 541, 540, 538, 536, 534, 533, 531, 529, 528,
	526, 524, 523, 521, 519, 518, 516, 514, 512, 511, 509, 508, 506, 504, 502,
	501, 499, 498, 496, 494, 493, 491, 489, 488, 486, 484, 483, 481, 480, 478,
	476, 475, 473, 472, 470, 468, 467, 465, 464, 462, 460, 459, 457, 456, 454,
	453, 451, 449, 448, 446, 445, 443, 442, 440, 438, 437, 435, 434, 432, 431,
	429, 428, 426, 425, 423, 422, 420, 419, 417, 416, 414, 412, 411, 410, 408,
	406, 405, 404, 402, 400, 399, 398, 396, 394, 393, 392, 390, 389, 387, 386,
	384, 383, 381, 380, 378, 377, 375, 374, 372, 371, 370, 368, 367, 365, 364,
	362, 361, 360, 358, 357, 355, 354, 352, 351, 350, 348, 347, 345, 344, 342,
	341, 340, 338, 337, 336, 334, 333, 331, 330, 328, 327, 326, 324, 323, 322,
	320, 319, 318, 316, 315, 313, 312, 311, 309, 308, 306, 305, 304, 302, 301,
	300, 298, 297, 296, 294, 293, 292, 290, 289, 288, 286, 285, 284, 282, 281,
	280, 278, 277, 276, 274, 273, 272, 270, 269, 268, 267, 265, 264, 263, 261,
	260, 259, 258, 256, 255, 254, 252, 251, 250, 248, 247, 246, 245, 243, 242,
	241, 240, 238, 237, 236, 234, 233, 232, 231, 229, 228, 227, 226, 224, 223,
	222, 221, 219, 218, 217, 216, 214, 213, 212, 211, 210, 208, 207, 206, 205,
	203, 202, 201, 200, 198, 197, 196, 195, 194, 192, 191, 190, 189, 188, 186,
	185, 184, 183, 182, 180, 179, 178, 177, 176, 174, 173, 172, 171, 170, 168,
	167, 166, 165, 164, 162, 161, 160, 159, 158, 157, 156, 154, 153, 152, 151,
	150, 148, 147, 146, 145, 144, 143, 142, 140, 139, 138, 137, 136, 135, 134,
	132, 131, 130, 129, 128, 127, 126, 124, 123, 122, 121, 120, 119, 118, 116,
	115, 114, 113, 112, 111, 110, 109, 108, 106, 105, 104, 103, 102, 101, 100,
	99, 98, 97, 95, 94, 93, 92, 91, 90, 89, 88, 87, 86, 84, 83, 82, 81, 80, 79,
	78, 77, 76, 75, 74, 73, 72, 70, 69, 68, 67, 66, 65, 64, 63, 62, 61, 60, 59,
	58, 57, 56, 54, 53, 52, 51, 50, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39,
	38, 37, 36, 35, 34, 33, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19,
	18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1,
}

func sched(init, max int) []int {
	if max < 2 || init < 2 {
		panic("bad input")
	}
	if max <= init {
		return nil
	}
	list := make([]int, 64)
	k := max
	for i := range list {
		k = (k + 2) / 2
		list[i] = k
		i++
		if k <= init {
			list = list[:i]
			break
		}
	}
	return list
}

func (c Context) logLarge(z, x *Big) int64 {
	var p int64
	switch {
	// 1e+1000
	case x.Scale() <= 0:
		p = int64(x.Precision() - x.Scale() - 1)
	// 0.0001
	case x.Scale() >= x.Precision():
		p = -int64(-x.exp - x.Precision() + 1)
		c.Precision = int(float64(c.precision()) * 1.5)
	// 12.345
	default:
		p = int64(-x.Scale() + x.Precision() - 1)
	}

	// Rescale to 1 <= x < 10
	y := new(Big).Copy(x)
	y.exp = -(x.Precision() - 1)

	// Continued fraction algorithm is for log(1+x)
	c.Sub(y, y, one.get())
	g := lgen{
		ctx: c,
		pow: c.Mul(new(Big), y, y),
		z2:  c.Add(new(Big), y, two.get()),
		k:   -1,
		t:   Term{A: new(Big), B: new(Big)},
	}
	c.Quo(z, c.Mul(y, y, two.get()), c.Wallis(z, &g))
	return p
}

type lgen struct {
	ctx Context
	pow *Big // z*z
	z2  *Big // z+2
	k   int64
	t   Term
}

func (l *lgen) Context() Context {
	return l.ctx
}

func (l *lgen) Wallis() (a, a1, b, b1, p, eps *Big) {
	a = WithContext(l.ctx)
	a1 = WithContext(l.ctx)
	b = WithContext(l.ctx)
	b1 = WithContext(l.ctx)
	p = WithContext(l.ctx)
	eps = New(1, l.ctx.Precision)
	return a, a1, b, b1, p, eps
}

func (a *lgen) Next() bool {
	return true
}

func (a *lgen) Term() Term {
	// log(z) can be expressed as the following continued fraction:
	//
	//          2z      1^2 * z^2   2^2 * z^2   3^2 * z^2   4^2 * z^2
	//     ----------- ----------- ----------- ----------- -----------
	//      1 * (2+z) - 3 * (2+z) - 5 * (2+z) - 7 * (2+z) - 9 * (2+z) - ···
	//
	// (Cuyt, p 271).
	//
	// References:
	//
	// [Cuyt] - Cuyt, A.; Petersen, V.; Brigette, V.; Waadeland, H.; Jones, W.B.
	// (2008). Handbook of Continued Fractions for Special Functions. Springer
	// Netherlands. https://doi.org/10.1007/978-1-4020-6949-9

	a.k += 2
	if a.k != 1 {
		a.t.A.SetMantScale(-((a.k / 2) * (a.k / 2)), 0)
		a.ctx.Mul(a.t.A, a.t.A, a.pow)
	}
	a.t.B.SetMantScale(a.k, 0)
	a.ctx.Mul(a.t.B, a.t.B, a.z2)
	return a.t
}

// ln10 sets z to log(10) and returns z.
func (c Context) ln10(z *Big) *Big {
	prec := c.precision()
	if prec <= constPrec {
		// Copy takes 1/2 the time as ctx.Set since there's no rounding and in
		// most of our algorithms we just need >= prec.
		return z.Copy(_Ln10.get())
	}

	ctx := Context{Precision: prec + 10}
	g := lgen{
		ctx: ctx,
		pow: eightyOne.get(), // 9 * 9
		z2:  eleven.get(),    // 9 + 2
		k:   -1,
		t:   Term{A: new(Big), B: new(Big)},
	}
	ctx.Quo(z, eighteen.get() /* 9 * 2 */, ctx.Wallis(z, &g))

	return c.finish(z)
}

func (c Context) ln10Taylor(z *Big) *Big {
	prec := c.precision()

	z.Copy(_Ln10.get())
	if prec <= constPrec {
		return z
	}
	var (
		tmp  Big
		wctx Context
		uctx = ContextUnlimited
	)
	// N[ln(10), 25] = N[ln(10), 5] + (exp(-N[ln(10), 5]) * 10 - 1)
	//   ln(10) + (exp(-ln(10)) * 10 - 1)
	for p := bits.Len(uint(prec+2)) + 99; ; p *= 2 {
		wctx.Precision = p + 5
		z.SetSignbit(true)
		wctx.Exp(&tmp, z)
		z.SetSignbit(false)
		uctx.Add(z, z, wctx.FMA(&tmp, ten.get(), &tmp, negOne.get()))
		if !z.IsFinite() || p >= prec+2 {
			break
		}
	}
	return z
}

// Mul sets z to x * y and returns z.
func (c Context) Mul(z, x, y *Big) *Big {
	if z.invalidContext(c) {
		return z
	}
	return c.finish(c.mul(z, x, y))
}

// mul is the implementation of Mul.
func (c Context) mul(z, x, y *Big) *Big {
	if debug {
		x.validate()
		y.validate()
	}

	sign := x.form&signbit ^ y.form&signbit

	if x.IsFinite() && y.IsFinite() {
		z.form = finite | sign
		z.exp = x.exp + y.exp

		// Multiplication is simple, so inline it.
		if x.isCompact() {
			if y.isCompact() {
				hi, lo := bits.Mul64(x.compact, y.compact)
				if hi == 0 {
					z.compact = lo
					if lo == cst.Inflated {
						z.unscaled.SetUint64(cst.Inflated)
					}
					z.precision = arith.Length(lo)
					return z
				}
				arith.Set(&z.unscaled, hi, lo)
			} else { // y.isInflated
				arith.Mul(&z.unscaled, &y.unscaled, x.compact)
			}
		} else if y.isCompact() { // x.isInflated
			arith.Mul(&z.unscaled, &x.unscaled, y.compact)
		} else {
			z.unscaled.Mul(&x.unscaled, &y.unscaled)
		}
		return z.norm()
	}

	// NaN * NaN
	// NaN * y
	// x * NaN
	if z.checkNaNs(x, y, multiplication) {
		return z
	}

	if (x.IsInf(0) && !y.isZero()) ||
		(y.IsInf(0) && !x.isZero()) ||
		(y.IsInf(0) && x.IsInf(0)) {
		// ±Inf * y
		// x * ±Inf
		// ±Inf * ±Inf
		return z.SetInf(sign != 0)
	}

	// 0 * ±Inf
	// ±Inf * 0
	return z.setNaN(InvalidOperation, qnan, mul0inf)
}

// Neg sets z to -x and returns z.
//
// If x is positive infinity, z will be set to negative infinity
// and vice versa. If x == 0, z will be set to zero. It is an
// error if x is a NaN value
func (c Context) Neg(z, x *Big) *Big {
	if debug {
		x.validate()
	}
	if !z.invalidContext(c) && !z.checkNaNs(x, x, negation) {
		xform := x.form // copy in case z == x
		z.copyAbs(x)
		if !z.IsFinite() || z.compact != 0 || c.RoundingMode == ToNegativeInf {
			z.form = xform ^ signbit
		}
	}
	return c.finish(z)
}

// NextMinus sets z to the smallest representable number that's
// smaller than x and returns z.
//
// If x is negative infinity the result will be negative
// infinity. If the result is zero its sign will be negative and
// its scale will be MinScale.
func (c Context) NextMinus(z, x *Big) *Big {
	if debug {
		x.validate()
	}
	if z.checkNaNs(x, x, nextminus) {
		return z
	}

	if x.IsInf(0) {
		if x.IsInf(-1) {
			return z.SetInf(true)
		}
		m := &z.unscaled
		maxfor(m, c.precision(), +1)
		return z.SetBigMantScale(m, -c.etop())
	}

	c.RoundingMode = ToNegativeInf
	c.Set(z, x)
	c.Sub(z, x, new(Big).SetMantScale(1, -c.etiny()+1))
	z.Context.Conditions &= c.Conditions
	return z
}

// NextPlus sets z to the largest representable number that's
// larger than x and returns z.
//
// If x is positive infinity the result will be positive
// infinity. If the result is zero it will be positive and its
// scale will be MaxScale.
func (c Context) NextPlus(z, x *Big) *Big {
	if debug {
		x.validate()
	}
	if z.checkNaNs(x, x, nextminus) {
		return z
	}

	if x.IsInf(0) {
		if x.IsInf(+1) {
			return z.SetInf(false)
		}
		m := &z.unscaled
		maxfor(m, c.precision(), -1)
		return z.SetBigMantScale(m, -c.etop())
	}

	c.RoundingMode = ToPositiveInf
	c.Set(z, x)
	c.Add(z, x, new(Big).SetMantScale(1, -c.etiny()+1))
	z.Context.Conditions &= c.Conditions
	return z
}

// pi2 sets z to the mathematical constant pi / 2 and returns z.
func (c Context) pi2(z *Big) *Big {
	if c.Precision <= constPrec {
		return c.Set(z, _Pi2.get())
	}
	return c.Quo(z, c.pi(z), two.get())
}

// pi sets z to the mathematical constant pi and returns z.
func (c Context) pi(z *Big) *Big {
	if c.Precision <= constPrec {
		return c.Set(z, _Pi.get())
	}
	return c.piChudnovskyBrothers(z)
}

// Pi sets z to the mathematical constant pi and returns z.
func (c Context) Pi(z *Big) *Big {
	return c.pi(z)
}

func (c Context) getPiA() func(n uint64) *Big {
	return func(n uint64) *Big {
		// returns A + Bn
		var tmp Big
		tmp.SetUint64(n)
		c.Mul(&tmp, _545140134.get(), &tmp)
		c.Add(&tmp, _13591409.get(), &tmp)
		return &tmp
	}
}

func (c Context) getPiP() func(n uint64) *Big {
	var tmp0, tmp1, tmp2, cn, c6n, c2n Big
	return func(n uint64) *Big {
		// returns (5-6n)(2n-1)(6n-1)

		if n == 0 {
			return one.get()
		}

		// we'll choose to do normal integer math when small enough
		if n < 504103 {
			return New((5-6*int64(n))*(2*int64(n)-1)*(6*int64(n)-1), 0)
		}

		cn.SetUint64(n)
		c.Mul(&c6n, &cn, six.get())
		c.Mul(&c2n, &cn, two.get())

		c.Sub(&tmp0, five.get(), &c6n)
		c.Sub(&tmp1, &c2n, one.get())
		c.Sub(&tmp2, &c6n, one.get())

		var tmp Big
		c.Mul(&tmp, &tmp0, &tmp1)
		c.Mul(&tmp, &tmp, &tmp2)

		return &tmp
	}
}

func getPiB() func(n uint64) *Big {
	return func(n uint64) *Big {
		return one.get()
	}
}

func (c Context) getPiQ() func(n uint64) *Big {
	// returns (0) = 1, q(n) = (n^3)(C^3)/24 for n > 0
	// (C^3)/24 = 10939058860032000
	return func(n uint64) *Big {
		if n == 0 {
			return WithContext(c).SetUint64(1)
		}

		var nn, tmp Big
		// When n is super small we can be super fast.
		if n < 12 {
			tmp.SetUint64(n * n * n * 10939058860032000)
			return &tmp
		}

		// And until bit larger we can still speed up a portion.
		if n < 2642246 {
			tmp.SetUint64(n * n * n)
		} else {
			nn.SetUint64(n)
			c.Mul(&tmp, &nn, &nn)
			c.Mul(&tmp, &tmp, &nn)
		}
		c.Mul(&tmp, &tmp, _10939058860032000.get())

		return &tmp
	}
}

// piChudnovskyBrothers calculates PI using binary splitting on the series
// definition of the Chudnovsky Brothers' algorithm for computing pi.
//
//                426880*sqrt(10005)
//    Pi = --------------------------------
// 		    13591409*aSum + 545140134*bSum
//
//    where
// 		           24(6n-5)(2n-1)(6n-1)
// 			a_n = ----------------------
// 					 (640320^3)*n^3
//
//    and
// 			aSum = sum_(n=0)^n a_n
// 			bSum = sum_(n=0)^n a_n*n
//
//    a(n) = 1,
//    b(n) = 1,
//    p(0) = 1, p(n) = (13591409 + 545140134n)(5 − 6n)(2n − 1)(6n − 1),
//    q(0) = 1, q(n) = (n^3)(640320^3)/24 for n > 0
//
func (c Context) piChudnovskyBrothers(z *Big) *Big {
	// Since this algorithm's rate of convergence is static, calculating the
	// number of iteration required will always be faster than the dynamic
	// method of binarysplit.

	calcPrec := c.Precision + 16
	niters := uint64(math.Ceil(float64(calcPrec)/14)) + 1
	ctx2 := Context{Precision: calcPrec}

	var value Big
	BinarySplit(&value, ctx2, 0, niters,
		ctx2.getPiA(), ctx2.getPiP(), getPiB(), ctx2.getPiQ())

	var tmp Big
	ctx2.Sqrt(&tmp, _10005.get())
	ctx2.Mul(&tmp, _426880.get(), &tmp)
	ctx2.Quo(z, &tmp, &value)

	return c.finish(z)
}

// TODO(eric): (c Context) Pow(z, x, y, m *Big) *Big

// Pow sets z to x**y and returns z.
func (c Context) Pow(z, x, y *Big) *Big {
	if z.CheckNaNs(x, y) {
		return z
	}

	if s := x.Sign(); s <= 0 {
		if s == 0 {
			if y.Sign() == 0 {
				// 0 ** 0 is undefined
				z.Context.Conditions |= InvalidOperation
				return z.SetNaN(false)
			}
			if y.Signbit() {
				// 0 ** -y = +Inf
				return z.SetInf(true)
			}
			// 0 ** y = 0
			return z.SetUint64(0)
		}
		if s < 0 && (!y.IsInt() || y.IsInf(0)) {
			// -x ** y.vvv is undefined
			// -x ** ±Inf is undefined
			z.Context.Conditions |= InvalidOperation
			return z.SetNaN(false)
		}
	}

	if x.IsInf(0) {
		switch y.Sign() {
		case +1:
			// ±Inf ** y = +Inf
			return z.SetInf(false)
		case -1:
			// ±Inf ** -y = 0
			return z.SetUint64(0)
		case 0:
			// ±Inf ** 0 = 1
			return z.SetUint64(1)
		}
	}

	if y.Sign() == 0 {
		// x ** 0 = 1
		return z.SetUint64(1)
	}

	if y.Cmp(ptFive.get()) == 0 {
		// x ** 0.5 = sqrt(x)
		return c.Sqrt(z, x)
	}

	if y.IsInt() {
		c.powInt(z, x, y)
	} else {
		c.powDec(z, x, y)
	}
	return c.finish(z)
}

// powInt sets z = x**y for the integer y.
func (c Context) powInt(z, x, y *Big) *Big {
	ctx := Context{
		Precision: c.precision() + y.Precision() - y.Scale() + 2,
	}

	if yy, ok := y.Uint64(); ok {
		return ctx.powUint64(z, x, yy)
	}

	x0 := getDec(ctx)
	if y.Signbit() {
		ctx.Precision++
		ctx.Quo(x0, one.get(), x)
	} else {
		x0.Copy(x)
	}
	z.SetUint64(1)
	sign := x.Signbit()

	y0 := y.Int(nil)
	sign = sign && y0.Bit(0) == 1
	y0.Abs(y0)
	for y0.Sign() != 0 {
		if y0.Bit(0) != 0 {
			ctx.Mul(z, z, x0)
			if !z.IsFinite() || z.Sign() == 0 ||
				z.Context.Conditions&Clamped != 0 {
				z.Context.Conditions |= Underflow | Subnormal
				sign = false
				break
			}
		}
		y0.Rsh(y0, 1)
		ctx.Mul(x0, x0, x0)
		if x0.IsNaN(0) {
			putDec(x0)
			z.Context.Conditions |= x0.Context.Conditions
			return z.SetNaN(false)
		}
	}
	putDec(x0)
	return z.SetSignbit(sign)
}

// powUint64 sets z = x**y.
func (c Context) powUint64(z, x *Big, y uint64) *Big {
	x0 := getDec(c).Copy(x)

	z.SetUint64(1)
	sign := x.Signbit() && y&1 == 1
	for y != 0 {
		if y&1 != 0 {
			c.Mul(z, z, x0)
			if !z.IsFinite() || z.Sign() == 0 ||
				z.Context.Conditions&Clamped != 0 {
				z.Context.Conditions |= Underflow | Subnormal
				sign = false
				break
			}
		}
		y >>= 1
		c.Mul(x0, x0, x0)
		if x0.IsNaN(0) {
			putDec(x0)
			z.Context.Conditions |= x0.Context.Conditions
			return z.SetNaN(false)
		}
	}
	putDec(x0)

	z.SetSignbit(sign)
	return z
}

// powDec sets z = x**y for a decimal y.
func (c Context) powDec(z, x, y *Big) *Big {
	if z == y {
		y = getDec(c).Copy(y)
		defer putDec(y)
	}
	neg := x.Signbit()
	if neg {
		x = getDec(c).CopyAbs(x)
		defer putDec(x)
	}

	work := c
	work.Precision = max(x.Precision(), c.precision()) + 4 + 19
	work.Exp(z, work.Mul(z, y, work.Log(z, x)))
	if neg && z.IsFinite() {
		z.CopyNeg(z)
	}
	return z
}

// Quantize sets z to the number equal in value and sign to z with the scale, n.
//
// In order to perform truncation, set the Context's RoundingMode to ToZero.
func (c Context) Quantize(z *Big, n int) *Big {
	if debug {
		z.validate()
	}
	if z.invalidContext(c) {
		return z
	}

	n = -n
	if z.isSpecial() {
		if z.form&inf != 0 {
			return z.setNaN(InvalidOperation, qnan, quantinf)
		}
		z.checkNaNs(z, z, quantization)
		return z
	}

	if n > c.emax() || n < c.etiny() {
		return z.setNaN(InvalidOperation, qnan, quantminmax)
	}

	if z.isZero() {
		z.exp = n
		return z
	}

	shift := z.exp - n
	if z.Precision()+shift > c.precision() {
		return z.setNaN(InvalidOperation, qnan, quantprec)
	}

	z.exp = n
	if shift == 0 {
		return z
	}

	if shift < 0 {
		z.Context.Conditions |= Rounded
	}

	m := c.RoundingMode
	neg := z.form & signbit
	if z.isCompact() {
		if shift > 0 {
			if zc, ok := arith.MulPow10(z.compact, uint64(shift)); ok {
				return z.setTriple(zc, neg, n)
			}
			// shift < 0
		} else if yc, ok := arith.Pow10(uint64(-shift)); ok {
			z.quo(m, z.compact, neg, yc, 0)
			return z
		}
		z.unscaled.SetUint64(z.compact)
		z.compact = cst.Inflated
	}

	if shift > 0 {
		arith.MulBigPow10(&z.unscaled, &z.unscaled, uint64(shift))
		z.precision = arith.BigLength(&z.unscaled)
	} else {
		var r big.Int
		z.quoBig(m, &z.unscaled, neg, arith.BigPow10(uint64(-shift)), 0, &r)
	}
	return z
}

// Quo sets z to x / y and returns z.
func (c Context) Quo(z, x, y *Big) *Big {
	if debug {
		x.validate()
		y.validate()
	}
	if z.invalidContext(c) {
		return z
	}

	sign := (x.form & signbit) ^ (y.form & signbit)
	if x.isSpecial() || y.isSpecial() {
		// NaN / NaN
		// NaN / y
		// x / NaN
		if z.checkNaNs(x, y, division) {
			return z
		}

		if x.form&inf != 0 {
			if y.form&inf != 0 {
				// ±Inf / ±Inf
				return z.setNaN(InvalidOperation, qnan, quoinfinf)
			}
			// ±Inf / y
			return z.SetInf(sign != 0)
		}
		// x / ±Inf
		z.Context.Conditions |= Clamped
		return z.setZero(sign, c.etiny())
	}

	if y.isZero() {
		if x.isZero() {
			// 0 / 0
			return z.setNaN(InvalidOperation|DivisionUndefined, qnan, quo00)
		}
		// x / 0
		z.Context.Conditions |= DivisionByZero
		return z.SetInf(sign != 0)
	}
	if x.isZero() {
		// 0 / y
		return c.fix(z.setZero(sign, x.exp-y.exp))
	}

	var (
		ideal = x.exp - y.exp // preferred exponent.
		m     = c.RoundingMode
		yp    = y.Precision() // stored since we might decrement it.
		zp    = c.precision() // stored because of overhead.
	)
	if zp == UnlimitedPrecision {
		m = unnecessary
		zp = x.Precision() + int(math.Ceil(10*float64(yp)/3))
	}

	if x.isCompact() && y.isCompact() {
		if cmpNorm(x.compact, x.Precision(), y.compact, yp) {
			yp--
		}

		shift := zp + yp - x.Precision()
		z.exp = (x.exp - y.exp) - shift
		expadj := ideal - z.exp
		if shift > 0 {
			if sx, ok := arith.MulPow10(x.compact, uint64(shift)); ok {
				if z.quo(m, sx, x.form, y.compact, y.form) && expadj > 0 {
					c.simpleReduce(z)
				}
				return z
			}
			xb := z.unscaled.SetUint64(x.compact)
			xb = arith.MulBigPow10(xb, xb, uint64(shift))
			yb := new(big.Int).SetUint64(y.compact)
			rb := new(big.Int)
			if z.quoBig(m, xb, x.form, yb, y.form, rb) && expadj > 0 {
				c.simpleReduce(z)
			}
			return z
		}
		if shift < 0 {
			if sy, ok := arith.MulPow10(y.compact, uint64(-shift)); ok {
				if z.quo(m, x.compact, x.form, sy, y.form) && expadj > 0 {
					c.simpleReduce(z)
				}
				return z
			}
			yb := z.unscaled.SetUint64(y.compact)
			yb = arith.MulBigPow10(yb, yb, uint64(-shift))
			xb := new(big.Int).SetUint64(x.compact)
			rb := new(big.Int)
			if z.quoBig(m, xb, x.form, yb, y.form, rb) && expadj > 0 {
				c.simpleReduce(z)
			}
			return z
		}
		if z.quo(m, x.compact, x.form, y.compact, y.form) && expadj > 0 {
			c.simpleReduce(z)
		}
		return z
	}

	xb, yb := &x.unscaled, &y.unscaled
	if x.isCompact() {
		xb = new(big.Int).SetUint64(x.compact)
	} else if y.isCompact() {
		yb = new(big.Int).SetUint64(y.compact)
	}

	if cmpNormBig(&z.unscaled, xb, x.Precision(), yb, yp) {
		yp--
	}

	shift := zp + yp - x.Precision()
	z.exp = (x.exp - y.exp) - shift

	var tmp *big.Int
	if shift > 0 {
		tmp = alias(&z.unscaled, yb)
		xb = arith.MulBigPow10(tmp, xb, uint64(shift))
	} else if shift < 0 {
		tmp = alias(&z.unscaled, xb)
		yb = arith.MulBigPow10(tmp, yb, uint64(-shift))
	} else {
		tmp = new(big.Int)
	}

	expadj := ideal - z.exp
	if z.quoBig(m, xb, x.form, yb, y.form, alias(tmp, &z.unscaled)) && expadj > 0 {
		c.simpleReduce(z)
	}
	return z
}

func (z *Big) quo(m RoundingMode, x uint64, xneg form, y uint64, yneg form) bool {
	z.form = xneg ^ yneg
	z.compact = x / y
	z.precision = arith.Length(z.compact)

	r := x % y
	if r == 0 {
		return true
	}

	z.Context.Conditions |= Inexact | Rounded
	if m == ToZero {
		return false
	}

	rc := 1
	if hi, lo := bits.Mul64(r, 2); hi == 0 {
		rc = arith.Cmp(lo, y)
	}

	if m == unnecessary {
		z.setNaN(InvalidOperation|InvalidContext|InsufficientStorage, qnan, quotermexp)
		return false
	}

	if m.needsInc(z.compact&1 != 0, rc, xneg == yneg) {
		z.Context.Conditions |= Rounded
		z.compact++

		// Test to see if we accidentally increased precision because of rounding.
		// For example, given n = 17 and RoundingMode = ToNearestEven, rounding
		//
		//   0.9999999999999999994284
		//
		// results in
		//
		//   0.99999999999999999 (precision = 17)
		//
		// which is rounded up to
		//
		//   1.00000000000000000 (precision = 18)
		if arith.Length(z.compact) != z.precision {
			z.compact /= 10
			z.exp++
		}
	}
	return false
}

func (z *Big) quoBig(
	m RoundingMode,
	x *big.Int, xneg form,
	y *big.Int, yneg form,
	r *big.Int,
) bool {
	z.compact = cst.Inflated
	z.form = xneg ^ yneg

	q, r := z.unscaled.QuoRem(x, y, r)
	if r.Sign() == 0 {
		z.norm()
		return true
	}

	z.Context.Conditions |= Inexact | Rounded
	if m == ToZero {
		z.norm()
		return false
	}

	var rc int
	rv := r.Uint64()
	// Drop into integers if possible.
	if r.IsUint64() && y.IsUint64() && rv <= math.MaxUint64/2 {
		rc = arith.Cmp(rv*2, y.Uint64())
	} else {
		rc = r.Mul(r, cst.TwoInt).CmpAbs(y)
	}

	if m == unnecessary {
		z.setNaN(InvalidOperation|InvalidContext|InsufficientStorage, qnan, quotermexp)
		return false
	}

	if m.needsInc(q.Bit(0) != 0, rc, xneg == yneg) {
		z.Context.Conditions |= Rounded
		z.precision = arith.BigLength(q)
		arith.Add(q, q, 1)
		if arith.BigLength(q) != z.precision {
			q.Quo(q, cst.TenInt)
			z.exp++
		}
	}
	z.norm()
	return false
}

// QuoInt sets z to x / y with the remainder truncated. See QuoRem for more
// details.
func (c Context) QuoInt(z, x, y *Big) *Big {
	if debug {
		x.validate()
		y.validate()
	}
	if z.invalidContext(c) {
		return z
	}

	sign := (x.form & signbit) ^ (y.form & signbit)
	if x.IsFinite() && y.IsFinite() {
		if y.isZero() {
			if x.isZero() {
				// 0 / 0
				return z.setNaN(InvalidOperation|DivisionUndefined, qnan, quo00)
			}
			// x / 0
			z.Context.Conditions |= DivisionByZero
			return z.SetInf(sign != 0)
		}
		if x.isZero() {
			// 0 / y
			return c.fix(z.setZero(sign, 0))
		}
		z, _ = c.quorem(z, nil, x, y)
		z.exp = 0
		if z.Precision() > c.precision() {
			return z.setNaN(DivisionImpossible, qnan, quointprec)
		}
		return z
	}

	// NaN / NaN
	// NaN / y
	// x / NaN
	if z.checkNaNs(x, y, division) {
		return z
	}

	if x.form&inf != 0 {
		if y.form&inf != 0 {
			// ±Inf / ±Inf
			return z.setNaN(InvalidOperation, qnan, quoinfinf)
		}
		// ±Inf / y
		return z.SetInf(sign != 0)
	}
	// x / ±Inf
	return z.setZero(sign, 0)
}

// QuoRem sets z to the quotient x / y and r to the remainder x % y, such that
// x = z * y + r, and returns the pair (z, r).
func (c Context) QuoRem(z, x, y, r *Big) (*Big, *Big) {
	if debug {
		x.validate()
		y.validate()
	}
	if z.invalidContext(c) {
		r.invalidContext(c)
		return z, r
	}

	sign := (x.form & signbit) ^ (y.form & signbit)
	if x.IsFinite() && y.IsFinite() {
		if y.isZero() {
			if x.isZero() {
				// 0 / 0
				z.setNaN(InvalidOperation|DivisionUndefined, qnan, quo00)
				r.setNaN(InvalidOperation|DivisionUndefined, qnan, quo00)
			}
			// x / 0
			z.Context.Conditions |= DivisionByZero
			r.Context.Conditions |= DivisionByZero
			return z.SetInf(sign != 0), r.SetInf(x.Signbit())
		}
		if x.isZero() {
			// 0 / y
			z.setZero((x.form^y.form)&signbit, 0)
			r.setZero(x.form, y.exp-x.exp)
			return c.fix(z), c.fix(r)
		}
		return c.quorem(z, r, x, y)
	}

	// NaN / NaN
	// NaN / y
	// x / NaN
	if z.checkNaNs(x, y, division) {
		return z, r.Set(z)
	}

	if x.form&inf != 0 {
		if y.form&inf != 0 {
			// ±Inf / ±Inf
			z.setNaN(InvalidOperation, qnan, quoinfinf)
			return z, r.Set(z)
		}
		// ±Inf / y
		return z.SetInf(sign != 0), r.SetInf(x.form&signbit != 0)
	}
	// x / ±Inf
	z.Context.Conditions |= Clamped
	z.setZero(sign, c.etiny())
	r.setZero(x.form&signbit, 0)
	return z, r
}

func (c Context) quorem(z0, z1, x, y *Big) (*Big, *Big) {
	m := c.RoundingMode
	zp := c.precision()

	if x.adjusted()-y.adjusted() > zp {
		if z0 != nil {
			z0.setNaN(DivisionImpossible, qnan, quorem_)
		}
		if z1 != nil {
			z1.setNaN(DivisionImpossible, qnan, quorem_)
		}
		return z0, z1
	}

	z := z0
	if z == nil {
		z = z1
	}

	if x.isCompact() && y.isCompact() {
		shift := x.exp - y.exp
		if shift > 0 {
			if sx, ok := arith.MulPow10(x.compact, uint64(shift)); ok {
				return m.quorem(z0, z1, sx, x.form, y.compact, y.form)
			}
			xb := z.unscaled.SetUint64(x.compact)
			xb = arith.MulBigPow10(xb, xb, uint64(shift))
			yb := new(big.Int).SetUint64(y.compact)
			return m.quoremBig(z0, z1, xb, x.form, yb, y.form)
		}
		if shift < 0 {
			if sy, ok := arith.MulPow10(y.compact, uint64(-shift)); ok {
				return m.quorem(z0, z1, x.compact, x.form, sy, y.form)
			}
			yb := z.unscaled.SetUint64(y.compact)
			yb = arith.MulBigPow10(yb, yb, uint64(-shift))
			xb := new(big.Int).SetUint64(x.compact)
			return m.quoremBig(z0, z1, xb, x.form, yb, y.form)
		}
		return m.quorem(z0, z1, x.compact, x.form, y.compact, y.form)
	}

	xb, yb := &x.unscaled, &y.unscaled
	if x.isCompact() {
		xb = new(big.Int).SetUint64(x.compact)
	} else if y.isCompact() {
		yb = new(big.Int).SetUint64(y.compact)
	}

	shift := x.exp - y.exp
	if shift > 0 {
		tmp := alias(&z.unscaled, yb)
		xb = arith.MulBigPow10(tmp, xb, uint64(shift))
	} else {
		tmp := alias(&z.unscaled, xb)
		yb = arith.MulBigPow10(tmp, yb, uint64(-shift))
	}
	return m.quoremBig(z0, z1, xb, x.form, yb, y.form)
}

// TODO(eric): quorem and quoremBig should not be methods on RoundingMode

func (m RoundingMode) quorem(
	z0, z1 *Big,
	x uint64, xneg form, y uint64, yneg form,
) (*Big, *Big) {
	if z0 != nil {
		z0.setTriple(x/y, xneg^yneg, 0)
	}
	if z1 != nil {
		z1.setTriple(x%y, xneg, 0)
	}
	return z0, z1
}

func (m RoundingMode) quoremBig(
	z0, z1 *Big,
	x *big.Int, xneg form,
	y *big.Int, yneg form,
) (*Big, *Big) {
	if z0 == nil {
		z1.unscaled.Rem(x, y)
		z1.form = xneg
		return z0, z1.norm()
	}

	if z1 != nil {
		z0.unscaled.QuoRem(x, y, &z1.unscaled)
		z1.form = xneg
		z1.norm()
	} else {
		z0.unscaled.QuoRem(x, y, new(big.Int))
	}
	z0.form = xneg ^ yneg
	return z0.norm(), z1
}

// Reduce reduces a finite z to its most simplest form.
func (c Context) Reduce(z *Big) *Big {
	if debug {
		z.validate()
	}
	c.Round(z)
	return c.simpleReduce(z)
}

// simpleReduce is the same as Reduce, but it does not round
// prior to reducing.
func (c Context) simpleReduce(z *Big) *Big {
	if z.isSpecial() {
		// Same semantics as plus(z), i.e. z+0.
		z.checkNaNs(z, z, reduction)
		return z
	}

	if z.isZero() {
		z.exp = 0
		z.precision = 1
		return z
	}

	if !z.isCompact() {
		if z.unscaled.Bit(0) != 0 {
			return z
		}

		var r big.Int
		for z.precision >= 20 {
			z.unscaled.QuoRem(&z.unscaled, cst.OneMillionInt, &r)
			if r.Sign() != 0 {
				// TODO(eric): which is less expensive? Copying
				// z.unscaled into a temporary or reconstructing
				// if we can't divide by N?
				z.unscaled.Mul(&z.unscaled, cst.OneMillionInt)
				z.unscaled.Add(&z.unscaled, &r)
				break
			}
			z.exp += 6
			z.precision -= 6

			// Try to avoid reconstruction for odd numbers.
			if z.unscaled.Bit(0) != 0 {
				break
			}
		}

		for z.precision >= 20 {
			z.unscaled.QuoRem(&z.unscaled, cst.TenInt, &r)
			if r.Sign() != 0 {
				z.unscaled.Mul(&z.unscaled, cst.TenInt)
				z.unscaled.Add(&z.unscaled, &r)
				break
			}
			z.exp++
			z.precision--
			if z.unscaled.Bit(0) != 0 {
				break
			}
		}

		if z.precision >= 20 {
			return z.norm()
		}
		z.compact = z.unscaled.Uint64()
	}

	for ; z.compact >= 10000 && z.compact%10000 == 0; z.precision -= 4 {
		z.compact /= 10000
		z.exp += 4
	}
	for ; z.compact%10 == 0; z.precision-- {
		z.compact /= 10
		z.exp++
	}
	return z
}

// Rem sets z to the remainder x % y.
//
// See QuoRem for more details.
func (c Context) Rem(z, x, y *Big) *Big {
	if debug {
		x.validate()
		y.validate()
	}
	if z.invalidContext(c) {
		return z
	}

	if x.IsFinite() && y.IsFinite() {
		if y.isZero() {
			if x.isZero() {
				// 0 / 0
				return z.setNaN(InvalidOperation|DivisionUndefined, qnan, quo00)
			}
			// x / 0
			return z.setNaN(InvalidOperation|DivisionByZero, qnan, remx0)
		}
		if x.isZero() {
			// 0 / y
			return z.setZero(x.form&signbit, min(x.exp, y.exp))
		}
		// TODO(eric): See if we can get rid of tmp. See issue
		// #72.
		var tmp Big
		_, z = c.quorem(&tmp, z, x, y)
		z.exp = min(x.exp, y.exp)
		tmp.exp = 0
		if tmp.Precision() > c.precision() {
			return z.setNaN(DivisionImpossible, qnan, quointprec)
		}
		return c.finish(z)
	}

	// NaN / NaN
	// NaN / y
	// x / NaN
	if z.checkNaNs(x, y, division) {
		return z
	}

	if x.form&inf != 0 {
		if y.form&inf != 0 {
			// ±Inf / ±Inf
			return z.setNaN(InvalidOperation, qnan, quoinfinf)
		}
		// ±Inf / y
		return z.setNaN(InvalidOperation, qnan, reminfy)
	}
	// x / ±Inf
	return c.Set(z, x)
}

// Round rounds z down to the Context's precision and returns z.
//
// For a finite z, result of Round will always be within the
// interval [⌊10**p⌋, z] where p = the precision of z. The result
// is undefined if z is not finite.
func (c Context) Round(z *Big) *Big {
	if debug {
		z.validate()
	}
	if z.invalidContext(c) {
		return z
	}
	return c.round(c.fix(z))
}

func (c Context) finish(z *Big) *Big {
	c.fix(z)
	if c.OperatingMode != GDA {
		return z
	}
	return c.round(z)
}

// round rounds z to the Context's precision, if necessary.
func (c Context) round(z *Big) *Big {
	n := c.precision()
	if n == UnlimitedPrecision || z.isSpecial() {
		return z
	}

	p := z.Precision()
	if p <= n {
		// Does not need to be rounded.
		return z
	}

	shift := p - n
	c.shiftr(z, uint64(shift))
	z.exp += shift
	z.Context.Conditions |= Rounded
	return z
}

// RoundToInt rounds z down to an integral value.
func (c Context) RoundToInt(z *Big) *Big {
	if z.isSpecial() || z.exp >= 0 {
		return z
	}
	c.Precision = z.Precision()
	return c.Quantize(z, 0)
}

func (c Context) shiftl(z *Big, n uint64) {
	if z.isZero() {
		return
	}
	if z.isCompact() {
		if zc, ok := arith.MulPow10(z.compact, n); ok {
			z.setTriple(zc, z.form, z.exp)
			return
		}
		z.unscaled.SetUint64(z.compact)
		z.compact = cst.Inflated
	}
	arith.MulBigPow10(&z.unscaled, &z.unscaled, n)
	z.precision = arith.BigLength(&z.unscaled)
}

func (c Context) shiftr(z *Big, n uint64) bool {
	if zp := uint64(z.Precision()); n >= zp {
		z.compact = 0
		z.precision = 1
		return n == zp
	}

	if z.isZero() {
		return false
	}

	m := c.RoundingMode
	if z.isCompact() {
		if y, ok := arith.Pow10(n); ok {
			return z.quo(m, z.compact, z.form, y, 0)
		}
		z.unscaled.SetUint64(z.compact)
		z.compact = cst.Inflated
	}
	var r big.Int
	return z.quoBig(m, &z.unscaled, z.form, arith.BigPow10(n), 0, &r)
}

// Sqrt sets z to the square root of x and returns z.
func (c Context) Sqrt(z, x *Big) *Big {
	if z.CheckNaNs(x, nil) {
		return z
	}

	ideal := -((-x.Scale() - (-x.Scale() & 1)) / 2)
	if xs := x.Sign(); xs <= 0 {
		if xs == 0 {
			return z.SetMantScale(0, ideal).CopySign(z, x)
		}
		z.Context.Conditions |= InvalidOperation
		return z.SetNaN(false)
	}

	// Already checked for negative numbers.
	if x.IsInf(+1) {
		return z.SetInf(false)
	}

	var (
		prec = c.precision()
		ctx  = Context{Precision: prec}
		rnd  = z.Context.Conditions&Rounded != 0
		ixt  = z.Context.Conditions&Inexact != 0
	)

	// Source for the following algorithm:
	//
	//  T. E. Hull and A. Abrham. 1985. Properly rounded variable
	//  precision square root. ACM Trans. Math. Softw. 11,
	//  3 (September 1985), 229-237.
	//  DOI: https://doi.org/10.1145/214408.214413

	xprec := x.Precision()

	// The algorithm requires a normalized "f in [0.1, 1)"
	// Of the two ways to normalize f, adjusting its scale is
	// the quickest. However, it then requires us to
	// increment approx's scale by e/2 instead of simply
	// setting it to e/2.
	f := new(Big).Copy(x)
	f.exp = -xprec
	e := x.exp + xprec

	var tmp Big // scratch space

	if e&1 == 0 {
		ctx.FMA(z, approx2.get(), f, approx1.get()) // approx := .259 + .819f
	} else {
		f.exp--                                     // f := f/10
		e++                                         // e := e + 1
		ctx.FMA(z, approx4.get(), f, approx3.get()) // approx := .0819 + 2.59f
	}

	maxp := prec + 5 // extra prec to skip weird +/- 0.5 adjustments
	ctx.Precision = 3
	pt5 := ptFive.get()
	for {
		// p := min(2*p - 2, maxp)
		ctx.Precision = min(2*ctx.Precision-2, maxp)

		// approx := .5*(approx + f/approx)
		ctx.Mul(z, pt5, ctx.Add(&tmp, z, ctx.Quo(&tmp, f, z)))
		if ctx.Precision == maxp {
			break
		}
	}

	// The paper also specifies an additional code block for
	// adjusting approx.  This code never went into the branches
	// that modified approx, and rounding to half even does the
	// same thing. The GDA spec requires us to use rounding mode
	// half even (speleotrove.com/decimal/daops.html#refsqrt)
	// anyway.

	z.exp += e / 2
	ctx.Reduce(z)
	if z.Precision() <= prec {
		if !rnd {
			z.Context.Conditions &= ^Rounded
		}
		if !ixt {
			z.Context.Conditions &= ^Inexact
		}
	}
	return c.finish(z)
}

// sqrt3 sets z to sqrt(3) and returns z.
func (c Context) sqrt3(z *Big) *Big {
	if c.Precision <= constPrec {
		return c.Set(z, _Sqrt3.get())
	}
	return c.Set(z, c.Sqrt(z, three.get()))
}

// Set sets z to x and returns z.
//
// The result might be rounded, even if z == x.
func (c Context) Set(z, x *Big) *Big {
	return c.finish(z.Copy(x))
}

// SetRat sets z to to the possibly rounded value of x and
// returns z.
func (c Context) SetRat(z *Big, x *big.Rat) *Big {
	if x.IsInt() {
		return c.finish(z.SetBigMantScale(x.Num(), 0))
	}
	var num, denom Big
	num.SetBigMantScale(x.Num(), 0)
	denom.SetBigMantScale(x.Denom(), 0)
	return c.Quo(z, &num, &denom)
}

// SetString sets z to the value of s, returning z and a bool
// indicating success.
//
// See Big.SetString for valid formats.
func (c Context) SetString(z *Big, s string) (*Big, bool) {
	if _, ok := z.SetString(s); !ok {
		return nil, false
	}
	return c.finish(z), true
}

// Sin returns the sine, in radians, of x.
//
// Range:
//     Input: all real numbers
//     Output: -1 <= Sin(x) <= 1
//
// Special cases:
//     Sin(NaN) = NaN
//     Sin(Inf) = NaN
func (c Context) Sin(z, x *Big) *Big {
	if debug {
		x.validate()
	}
	if z.invalidContext(c) {
		return z
	}
	if z.checkNaNs(x, x, sin) {
		return z
	}

	if !x.IsFinite() {
		// sin(inf) = NaN
		// sin(NaN) = NaN
		z.Context.Conditions |= InvalidOperation
		return z.SetNaN(false)
	}

	if x.isZero() {
		// sin(0) = 0
		return z.setZero(0, 0)
	}

	// Sin(x) = Cos(pi/2 - x)
	ctx := c.dup()
	ctx.Precision += defaultExtraPrecision
	pi2 := ctx.pi2(getDec(ctx))
	ctx.Cos(z, ctx.Sub(z, pi2, x))
	putDec(pi2)

	return c.finish(z)
}

// Sub sets z to x - y and returns z.
func (c Context) Sub(z, x, y *Big) *Big {
	if debug {
		x.validate()
		y.validate()
	}
	if z.invalidContext(c) {
		return z
	}

	if x.IsFinite() && y.IsFinite() {
		z.form = c.add(z, x, x.form, y, y.form^signbit)
		return c.finish(z)
	}

	// NaN - NaN
	// NaN - y
	// x - NaN
	if z.checkNaNs(x, y, subtraction) {
		return z
	}

	if x.form&inf != 0 {
		if y.form&inf != 0 && (x.form&signbit == y.form&signbit) {
			// -Inf - -Inf
			// -Inf - -Inf
			return z.setNaN(InvalidOperation, qnan, subinfinf)
		}
		// ±Inf - y
		// -Inf - +Inf
		// +Inf - -Inf
		return c.Set(z, x)
	}
	// x - ±Inf
	return z.Neg(y)
}

// Tan returns the tangent, in radians, of x.
//
// Range:
//     Input: -pi/2 <= x <= pi/2
//     Output: all real numbers
//
// Special cases:
//     Tan(NaN) = NaN
//     Tan(±Inf) = NaN
func (c Context) Tan(z, x *Big) *Big {
	if debug {
		x.validate()
	}
	if z.invalidContext(c) {
		return z
	}
	if z.checkNaNs(x, x, sin) {
		return z
	}

	if !x.IsFinite() {
		// tan(NaN) = NaN
		// tan(inf) = NaN
		z.Context.Conditions |= InvalidOperation
		return z.SetNaN(false)
	}

	ctx := c.dup()
	ctx.Precision += defaultExtraPrecision

	x0, ok := ctx.prepTan(z, x)
	if !ok {
		z.Context.Conditions |= InvalidOperation
		return z.SetNaN(false)
	}

	// tan(x) = sign(x)*sqrt(1/cos(x)^2-1)

	// tangent has an asymptote at pi/2 and we'll need more
	// precision as we get closer the reason we need it is as we
	// approach pi/2 is due to the squaring portion, it will
	// cause the small value of Cosine to be come extremely
	// small. we COULD fix it by simply doubling the precision,
	// however, when the precision gets larger it will be
	// a significant impact to performance, instead we'll only
	// add the extra precision when we need it by using the
	// difference to see how much extra precision we need we'll
	// speed things up by only using a quick compare to see if we
	// need to do a deeper inspection.
	tctx := ctx.dup()
	tmp := getDec(tctx)
	defer putDec(tmp)

	if x.CmpAbs(onePtFour.get()) >= 0 {
		if x.Signbit() {
			ctx.Add(tmp, x, ctx.pi2(tmp))
		} else {
			ctx.Sub(tmp, x, ctx.pi2(tmp))
		}
		tctx.Precision += tmp.Scale() - tmp.Precision()
	}

	tctx.Cos(tmp, x0)
	ctx.Mul(tmp, tmp, tmp)
	ctx.Quo(tmp, one.get(), tmp)
	ctx.Sub(tmp, tmp, one.get())
	tctx.Sqrt(tmp, tmp)
	if x0.Signbit() {
		tmp.CopyNeg(tmp)
	}
	ctx.Precision -= defaultExtraPrecision
	return ctx.Set(z, tmp)
}

func (c Context) prepTan(z, x *Big) (*Big, bool) {
	if z == x {
		x = getDec(c)
		defer putDec(x)
	}

	if pi2 := c.pi2(z); x.CmpAbs(pi2) >= 0 {
		// For tan to work correctly the input must be in (-2pi,
		// 2pi)
		if x.Signbit() {
			c.Add(z, x, pi2)
		} else {
			c.Sub(z, x, pi2)
		}

		var tmp Big
		c.QuoInt(z, z, c.pi(&tmp))
		if x.Signbit() {
			c.Sub(z, z, one.get())
		} else {
			c.Add(z, z, one.get())
		}

		v, ok := z.Int64()
		if !ok {
			return nil, false
		}
		uv := arith.Abs(v)

		// Adjust so we have ceil(v/10) + ctx.Precision, but check for overflows.
		// 1+((v-1)/10) will be widly incorrect for v == 0, but x/y = 0 iff
		// x = 0 and y != 0. In this case, -2pi <= x >= 2pi, so we're fine.
		prec, carry := bits.Add64(1+((uv-1)/10), uint64(c.Precision), 0)
		if carry != 0 || prec > arith.MaxInt {
			return nil, false
		}
		pctx := Context{Precision: int(prec)}

		pctx.Mul(z, pctx.pi(&tmp), z)

		c.Precision = c.precision() + 1
		// so toRemove = m*Pi so |x-toRemove| < Pi/2
		c.Sub(z, x, z)
	} else {
		z.Copy(x)
	}
	return z, true
}
