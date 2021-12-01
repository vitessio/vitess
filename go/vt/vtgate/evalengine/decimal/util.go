package decimal

import (
	"math/big"
	"strconv"
	"sync"

	"vitess.io/vitess/go/vt/vtgate/evalengine/decimal/internal/arith"
	cst "vitess.io/vitess/go/vt/vtgate/evalengine/decimal/internal/c"
)

// norm normalizes z to enforce the invariant that z.unscaled is
// only used if the decimal has over 20 digits.
func (z *Big) norm() *Big {
	if z.unscaled.IsUint64() {
		if v := z.unscaled.Uint64(); v != cst.Inflated {
			z.compact = v
			z.precision = arith.Length(v)
			return z
		}
	}
	z.precision = arith.BigLength(&z.unscaled)
	z.compact = cst.Inflated
	return z
}

// fix check for overflow, underflow, and clamping.
func (c Context) fix(z *Big) *Big {
	//mysql
	if z.isZero() {
		z.setZero(0, 0)
		return z
	}

	adj := z.adjusted()

	if adj > c.emax() {
		prec := c.precision()

		if z.isZero() {
			z.exp = c.emax()
			z.Context.Conditions |= Clamped
			return z
		}

		switch m := c.RoundingMode; m {
		case ToNearestAway, ToNearestEven, ToNearestTowardZero:
			z.SetInf(z.Signbit())
		case AwayFromZero:
			// OK
		case ToZero:
			z.exp = c.emax() - prec + 1
		case ToPositiveInf, ToNegativeInf:
			if m == ToPositiveInf == z.Signbit() {
				z.exp = c.emax() - prec + 1
			} else {
				z.SetInf(false)
			}
		}
		z.Context.Conditions |= Overflow | Inexact | Rounded
		return z
	}

	if adj < c.emin() {
		tiny := c.etiny()

		if z.isZero() {
			if z.exp < tiny {
				z.setZero(z.form, tiny)
				z.Context.Conditions |= Clamped
			}
			return z
		}

		z.Context.Conditions |= Subnormal
		if z.exp < tiny {
			if c.shiftr(z, uint64(tiny-z.exp)) {
				z.compact = 1
			}
			z.exp = tiny
			z.Context.Conditions |= Underflow
			if z.isZero() {
				z.Context.Conditions |= Clamped
			}
		}
	}
	return z
}

// alias returns z if z != x, otherwise a newly-allocated big.Int.
func alias(z, x *big.Int) *big.Int {
	if z != x {
		// We have to check the first element of their internal slices since
		// Big doesn't store a pointer to a big.Int.
		zb, xb := z.Bits(), x.Bits()
		if cap(zb) > 0 && cap(xb) > 0 && &zb[0:cap(zb)][cap(zb)-1] != &xb[0:cap(xb)][cap(xb)-1] {
			return z
		}
	}
	return new(big.Int)
}

// invalidContext reports whether the Context is invalid.
//
// If so, it modifies z appropriately.
func (z *Big) invalidContext(c Context) bool {
	switch {
	case c.Precision < 0:
		z.setNaN(InvalidContext, qnan, invctxpltz)
	case c.Precision > UnlimitedPrecision:
		z.setNaN(InvalidContext, qnan, invctxpgtu)
	case c.RoundingMode >= unnecessary:
		z.setNaN(InvalidContext, qnan, invctxrmode)
	case c.OperatingMode > Go:
		z.setNaN(InvalidContext, qnan, invctxomode)
	case c.MaxScale > MaxScale:
		z.setNaN(InvalidContext, qnan, invctxsgtu)
	case c.MinScale < MinScale:
		z.setNaN(InvalidContext, qnan, invctxsltu)
	default:
		return false
	}
	return true
}

// copybits can be useful when we want to allocate a big.Int without calling
// new or big.Int.Set. For example:
//
//   var x big.Int
//   if foo {
//       x.SetBits(copybits(y.Bits()))
//   }
//   ...
//
func copybits(x []big.Word) []big.Word {
	z := make([]big.Word, len(x))
	copy(z, x)
	return z
}

// cmpNorm compares x and y in the range [0.1, 0.999...] and
// reports whether x > y.
func cmpNorm(x uint64, xs int, y uint64, ys int) (ok bool) {
	goodx, goody := true, true

	// xs, ys > 0, so no overflow
	if diff := xs - ys; diff != 0 {
		if diff < 0 {
			x, goodx = arith.MulPow10(x, uint64(-diff))
		} else {
			y, goody = arith.MulPow10(y, uint64(diff))
		}
	}
	if goodx {
		if goody {
			return arith.Cmp(x, y) > 0
		}
		return false
	}
	return true
}

// cmpNormBig compares x and y in the range [0.1, 0.999...] and returns true if
// x > y. It uses z as backing storage, provided it does not alias x or y.
func cmpNormBig(z, x *big.Int, xs int, y *big.Int, ys int) (ok bool) {
	if xs != ys {
		z = alias(alias(z, x), y)
		if xs < ys {
			x = arith.MulBigPow10(z, x, uint64(ys-xs))
		} else {
			y = arith.MulBigPow10(z, y, uint64(xs-ys))
		}
	}
	// x and y are non-negative
	return x.Cmp(y) > 0
}

// scalex adjusts x by scale.
//
// If scale > 0, x = x * 10^scale, otherwise x = x / 10^-scale.
func scalex(x uint64, scale int) (sx uint64, ok bool) {
	if scale > 0 {
		if sx, ok = arith.MulPow10(x, uint64(scale)); !ok {
			return 0, false
		}
		return sx, true
	}
	p, ok := arith.Pow10(uint64(-scale))
	if !ok {
		return 0, false
	}
	return x / p, true
}

// bigScalex sets z to the big.Int equivalient of scalex.
func bigScalex(z, x *big.Int, scale int) *big.Int {
	if scale > 0 {
		return arith.MulBigPow10(z, x, uint64(scale))
	}
	return z.Quo(x, arith.BigPow10(uint64(-scale)))
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

// getmsd returns the n most significant digits in x.
func getmsd(x *Big, n int) uint64 {
	if n < 0 || n > arith.MaxLength {
		panic("getmsd: out of range: " + strconv.Itoa(n))
	}

	r := x.Precision() - n
	if r < 0 {
		r = 0
	}

	if x.isCompact() {
		p, _ := arith.Pow10(uint64(r))
		return x.compact / p
	}

	// Here, x.Precision >= n since n is in [0, 19] and an
	// inflated decimal is >= 1<<64-1.
	var w big.Int
	w.Quo(&x.unscaled, arith.BigPow10(uint64(r)))
	return w.Uint64()
}

var decPool = sync.Pool{
	New: func() interface{} {
		return new(Big)
	},
}

func getDec(ctx Context) *Big {
	x := decPool.Get().(*Big)
	x.Context = ctx
	return x
}

func putDec(x *Big) {
	decPool.Put(x)
}
