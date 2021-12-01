package decimal

import (
	"fmt"
)

// Term is a specific term in a continued fraction. A and B correspond with the
// a and b variables of the typical representation of a continued fraction. An
// example can be seen in the book, ``Numerical Recipes in C: The Art of
// Scientific Computing'' (ISBN 0-521-43105-5) in figure 5.2.1 on page 169.
type Term struct {
	A, B *Big
}

func (t Term) String() string {
	return fmt.Sprintf("[%s / %s]", t.A, t.B)
}

// Generator represents a continued fraction.
type Generator interface {
	// Next returns true if there are future terms. Every call to Term—even the
	// first—must be preceded by a call to Next. In general, Generators should
	// always return true unless an exceptional condition occurs.
	Next() bool

	// Term returns the next term in the fraction. The caller must not modify
	// any of the Term's fields.
	Term() Term
}

// Contexter allows Generators to provide a different Context than z's. It's
// intended to be analogous to the relationship between, for example,
// Context.Mul and Big.Mul.
type Contexter interface {
	Context() Context
}

// Okay, I _am_ sorry about the name of this interface. It's stupid.

// Walliser is analogous to Lentzer, except it's for the Wallis function.
type Walliser interface {
	// Wallis provides the backing storage for a Generator passed to the Wallis
	// function. See the Lentzer interface for more information.
	Wallis() (a, a1, b, b1, p, eps *Big)
}

type walliser struct {
	prec int
}

func (w walliser) Wallis() (a, a1, b, b1, p, eps *Big) {
	a = WithPrecision(w.prec)
	a1 = WithPrecision(w.prec)
	b = WithPrecision(w.prec)
	b1 = WithPrecision(w.prec)
	p = WithPrecision(w.prec)
	eps = New(1, w.prec)
	return a, a1, b, b1, p, eps
}

// Wallis sets z to the result of the continued fraction provided
// by the Generator and returns z.
//
// The fraction is evaluated in a top-down manner, using the
// recurrence algorithm discovered by John Wallis. For more
// information on continued fraction representations, see the
// Lentz function.
func (c Context) Wallis(z *Big, g Generator) *Big {
	if !g.Next() {
		return z
	}

	ws, ok := g.(Walliser)
	if !ok {
		ws = walliser{prec: c.precision() + 5}
	}
	a, a_1, b, b_1, p, eps := ws.Wallis()

	t := g.Term()

	a_1.SetUint64(1)
	a.Copy(t.B)
	b.SetUint64(1)

	ctx := z.Context
	if c, ok := g.(Contexter); ok {
		ctx = c.Context()
	}

	for g.Next() && p.IsFinite() {
		t = g.Term()

		z.Copy(a)
		ctx.FMA(a, a, t.B, ctx.Mul(a_1, a_1, t.A))
		a_1.Copy(z)

		z.Copy(b)
		ctx.FMA(b, b, t.B, ctx.Mul(b_1, b_1, t.A))
		b_1.Copy(z)

		ctx.Quo(z, a, b)
		if ctx.Sub(p, z, p).CmpAbs(eps) <= 0 {
			break
		}
		p.Copy(z)
	}
	return z
}

// Lentzer, if implemented, allows Generators to provide their own backing
// storage for the Lentz function.
type Lentzer interface {
	// Lentz provides the backing storage for a Generator passed to the Lentz
	// function.
	//
	// In Contexter isn't implemented, f, Δ, C, and D should have large enough
	// precision to provide a correct result, (See note for the Lentz function.)
	//
	// eps should be a sufficiently small decimal, likely 1e-15 or smaller.
	//
	// For more information, refer to "Numerical Recipes in C: The Art of
	// Scientific Computing" (ISBN 0-521-43105-5), pg 171.
	Lentz() (f, Δ, C, D, eps *Big)
}

// lentzer implements the Lentzer interface.
type lentzer struct{ prec int }

func (l lentzer) Lentz() (f, Δ, C, D, eps *Big) {
	f = WithPrecision(l.prec)
	Δ = WithPrecision(l.prec)
	C = WithPrecision(l.prec)
	D = WithPrecision(l.prec)
	eps = New(1, l.prec)
	return f, Δ, C, D, eps
}

var tiny = New(10, 60)

// Lentz sets z to the result of the continued fraction provided
// by the Generator and returns z.
//
// The continued fraction should be represented as such:
//
//                          a1
//     f(x) = b0 + --------------------
//                            a2
//                 b1 + ---------------
//                               a3
//                      b2 + ----------
//                                 a4
//                           b3 + -----
//                                  ...
//
// Or, equivalently:
//
//                  a1   a2   a3
//     f(x) = b0 + ---- ---- ----
//                  b1 + b2 + b3 + ···
//
// If terms need to be subtracted, the a_N terms should be
// negative. To compute a continued fraction without b_0, divide
// the result by a_1.
//
// If the first call to the Generator's Next method returns
// false, the result of Lentz is undefined.
//
// Note: the accuracy of the result may be affected by the
// precision of intermediate results. If larger precision is
// desired, it may be necessary for the Generator to implement
// the Lentzer interface and set a higher precision for f, Δ, C,
// and D.
func (c Context) Lentz(z *Big, g Generator) *Big {
	// We use the modified Lentz algorithm from
	// "Numerical Recipes in C: The Art of Scientific Computing" (ISBN
	// 0-521-43105-5), pg 171.
	//
	// Set f0 = b0; if b0 = 0 set f0 = tiny.
	// Set C0 = f0.
	// Set D0 = 0.
	// For j = 1, 2,...
	// 		Set D_j = b_j+a_j*D{_j−1}.
	// 		If D_j = 0, set D_j = tiny.
	// 		Set C_j = b_j+a_j/C{_j−1}.
	// 		If C_j = 0, set C_j = tiny.
	// 		Set D_j = 1/D_j.
	// 		Set ∆_j = C_j*D_j.
	// 		Set f_j = f{_j-1}∆j.
	// 		If |∆_j - 1| < eps then exit.

	if !g.Next() {
		return z
	}

	// See if our Generator provides us with backing storage.
	lz, ok := g.(Lentzer)
	if !ok {
		// TODO(eric): what is a sensible default precision?
		lz = lentzer{prec: c.precision() + 5}
	}
	f, Δ, C, D, eps := lz.Lentz()

	// tiny should be less than typical values of eps.
	tiny := tiny
	if eps.Scale() > tiny.Scale() {
		tiny = New(10, min(eps.Scale()*2, c.emax()))
	}

	t := g.Term()

	if t.B.Sign() != 0 {
		f.Copy(t.B)
	} else {
		f.Copy(tiny)
	}
	C.Copy(f)
	D.SetUint64(0)

	ctx := z.Context
	if c, ok := g.(Contexter); ok {
		ctx = c.Context()
	}

	for g.Next() && f.IsFinite() {
		t = g.Term()

		// Set D_j = b_j + a_j*D{_j-1}
		// Reuse D for the multiplication.
		ctx.FMA(D, t.A, D, t.B) // D.Add(t.B, D.Mul(t.A, D))

		// If D_j = 0, set D_j = tiny
		if D.Sign() == 0 {
			D.Copy(tiny)
		}

		// Set C_j = b_j + a_j/C{_j-1}
		// Reuse C for the division.
		ctx.Add(C, t.B, ctx.Quo(C, t.A, C))

		// If C_j = 0, set C_j = tiny
		if C.Sign() == 0 {
			C.Copy(tiny)
		}

		// Set D_j = 1/D_j
		ctx.Quo(D, one.get(), D)

		// Set Δ_j = C_j*D_j
		ctx.Mul(Δ, C, D)

		// Set f_j = f{_j-1}*Δ_j
		ctx.Mul(f, f, Δ)

		// If |Δ_j - 1| < eps then exit
		if ctx.Sub(Δ, Δ, one.get()).CmpAbs(eps) < 0 {
			break
		}
	}
	z.Context.Conditions |= f.Context.Conditions
	return ctx.Set(z, f)
}

/*
func dump(f, Δ, D, C, eps *Big) {
	fmt.Printf(`
f  : %s
Δ  : %s
D  : %s
C  : %s
eps: %s
`, f, Δ, D, C, eps)
}
*/
