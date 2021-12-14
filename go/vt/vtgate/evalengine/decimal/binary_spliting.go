package decimal

import (
	"vitess.io/vitess/go/vt/vtgate/evalengine/decimal/internal/arith"
)

// The binary splitting algorithm is made of four functions
// a(n), b(n), q(n), and p(n)
//
//                             a(n)p(0)...p(n)
//     S = sum_(n=0)^infinity -----------------
//                             b(n)q(0)...q(n)
//
//  We split it up into [n1, n2) slices and calculate using the following
//
//     B = b(n1)...b(n2-1)
//     P = p(n1)...p(n2-1)
//     Q = q(n1)...q(n2-1)
//
// then assign
//
//     T = BQS
//
// to solve for S
//
//     S = T/BQ
//
// ----
//
// The "trick" is that we plan to "binary-ly" split up the the range [0, n) such
// that for a given range [n1, n2) we will split it into two smaller ranges
// [n1, m) and [m, n2) where m = floor((n1+n2)/2). When n2 - n1 is either
// 1, 2, 3, or 4 we plan to calculate manually, but for anything else larger
// we then define a given range [n1, n2), split it into [n1, m) and [m, n2),
// noting a "Left" and "Right" side. Then, for each side with a B, P, Q, and T
// we'll note the subscript via a L or R. We then have the following formulation:
//
//     B = B_l*B_r
//     P = P_l*P_r
//     Q = Q_l*Q_r
//     T = B_l*P_l*T_r + B_r*Q_r*T_l
//
// (Take care in noticing Q_l and P_r aren't used in calculating T.)
// Then solve for S the same as above S = T/BQ.

// apbqBinarySplitState is used to hold intermediate values for each step in the
// calculation.
type apbqBinarySplitState struct {
	B *Big
	P *Big
	Q *Big
	T *Big
}

func newState() *apbqBinarySplitState {
	return &apbqBinarySplitState{
		B: new(Big),
		P: new(Big),
		Q: new(Big),
		T: new(Big),
	}
}

func (s *apbqBinarySplitState) term(z *Big, ctx Context) *Big {
	return ctx.Quo(z, s.T, ctx.Mul(z, s.B, s.Q)) // z = T / B*Q
}

// SplitFunc returns the intermediate value for a given n. The returned decimal
// must not be modified by the caller and may only be valid until the next
// invocation of said function. This allows the implementation to conserve
// memory usage.
type SplitFunc func(n uint64) *Big

// BinarySplit sets z to the result of the binary splitting formula and returns
// z. The formula is defined as:
//
//         ∞    a(n)p(0) ... p(n)
//     S = Σ   -------------------
//         n=0  b(n)q(0) ... q(n)
//
// It should only be used when the number of terms is known ahead of time. If
// start is not in [start, stop) or stop is not in (start, stop], BinarySplit
// will panic.
func BinarySplit(z *Big, ctx Context, start, stop uint64, A, P, B, Q SplitFunc) *Big {
	switch {
	case stop == start:
		panic("math: the start and stop of BinarySplit cannot be not be the same")
	case stop < start:
		panic("math: the stop of BinarySplit must be larger than the start")
	}

	state := newState()
	state.calculate(ctx, start, stop, A, P, B, Q)
	return state.term(z, ctx)
}

// BinarySplitDynamic sets z to the result of the binary splitting formula. It
// should be used when the number of terms is not known ahead of time. For more
// information, See BinarySplit.
func BinarySplitDynamic(ctx Context, A, P, B, Q SplitFunc) *Big {
	// TODO(eric): get a handle on this function's memory usage.

	// BinarySplitDynamic does not take a receiver since binary splitting uses
	// too much memory for a receiver to be useful. It's also difficult to keep
	// track of the receiver.

	// For this algorithm we start with a standard 16 terms to mark the first
	// return value's status, then we calculate the next 4 terms and mark the
	// difference (if ZERO return as is else repeat +4 terms until at least
	// 1 digit of ctx is gained). We then use that to linearly determine the
	// "last term," then repeat when calculating each of the parts the following
	// will be used:
	//
	//     B = B_l*B_r
	//     P = P_l*P_r
	//     Q = Q_l*Q_r
	//     T = B_l*P_l*T_r + B_r*Q_r*T_l

	currentLastTerm := uint64(16)
	current := newState()
	current.calculate(ctx, 0, currentLastTerm, A, P, B, Q)

	// the marked value is what should be returned which is T/(BQ)
	markValue1 := current.term(new(Big), ctx)
	markValue2 := new(Big)

	diff := new(Big) // markValue1 - markValue2

	// now get the next marked value, if the difference isn't already ZERO we need
	// at least one digit of ctx to continue
	nextLastTerm := currentLastTerm
	next := &apbqBinarySplitState{
		B: new(Big).Copy(current.B),
		P: new(Big).Copy(current.P),
		Q: new(Big).Copy(current.Q),
		T: new(Big).Copy(current.T),
	}
	var expectedLastTerm uint64
	deltaTerm := uint64(4)
	eps := New(1, ctx.Precision)

	tmp := newState()

	for {
		for {
			tmp.calculate(ctx, nextLastTerm, nextLastTerm+deltaTerm, A, P, B, Q)
			next.combine(ctx, next, tmp)
			nextLastTerm += deltaTerm

			// markValue2 = T / (B * Q)
			next.term(markValue2, ctx)

			// Terms have converged.
			if markValue1.Cmp(markValue2) == 0 {
				return markValue2
			}

			// if not equal one of two things could be happening
			// 1) markValue2 approaching a value away from markValue1 (something
			//    not close to markValue1)
			// 2) markValue2 approaching a value toward markValue1 (something
			//    close to markValue1)
			//
			// in the 1) case precision should stay the same but scale will change
			// in the 2) case scale & precision should stay the same but the difference
			// should see a reduction is the precision
			// we'll check for the first case since it doesn't require any "real"
			// calculations
			if markValue1.Scale() != markValue2.Scale() {
				// there was a change so save the current state
				current = next

				// next calculate the expectedLastTerm and add 4 to ensure it is always >0
				scaleDiff := arith.Abs(int64(markValue1.Scale()) - int64(markValue2.Scale()))
				expectedLastTerm = nextLastTerm + uint64(float64(nextLastTerm-currentLastTerm)*float64(ctx.Precision)/float64(scaleDiff)) + 4
				currentLastTerm = nextLastTerm
				break
			}

			// if not equal take the difference and figure out if we
			// have at least one digit of ctx gained
			ctx.Sub(diff, markValue1, markValue2)

			// here's the one case where we need to do a check for
			// something 1E-ctx if equal to or less than
			if diff.CmpAbs(eps) < 0 {
				return markValue2
			}

			// we want to have at least 1 digit which really means we
			// need a change in ctx of diff of 2 or greater

			precChange := arith.Abs(int64(markValue1.Precision()) - int64(diff.Precision()))
			if precChange > 1 {
				// we have something that we can use to
				// calculate the true expected last term
				// combine the currentState with this additional state
				// update the currentLastTerm and then calculate expectedLastTerm
				current = next

				// we'll calculate expectedLastTerm but also add 4 to ensure it is always >0
				expectedLastTerm = nextLastTerm + uint64(float64(nextLastTerm-currentLastTerm)*float64(ctx.Precision)/float64(precChange)) + 4
				currentLastTerm = nextLastTerm
				break
			}

			// if for some reason we haven't seen the expected change
			// it could be because the markValue1 and markValue2 are extremely different
			// so we'll breakout and hope the next iteration is better
			// worse case it's not and these continues until the value converges
			// in which case markValue1 and markValue2 will at some point be equal
			if nextLastTerm-currentLastTerm > 16 {
				// save the current state
				current = next

				// and set the expected and current to nextLastTerm
				expectedLastTerm = nextLastTerm
				currentLastTerm = nextLastTerm
				break
			}
		}

		// now we have what we expect to be way closer to the true n
		if currentLastTerm != expectedLastTerm {
			tmp.calculate(ctx, currentLastTerm, expectedLastTerm, A, P, B, Q)
			current.combine(ctx, current, tmp)
		}

		current.term(markValue1, ctx)

		currentLastTerm = expectedLastTerm
		nextLastTerm = currentLastTerm
		next = current
	}
}

func (s *apbqBinarySplitState) calculate(ctx Context, start, end uint64, A, P, B, Q SplitFunc) {
	switch n1 := start; end - start {
	case 1:
		s.B.Copy(B(n1))
		s.P.Copy(P(n1))
		s.Q.Copy(Q(n1))
		ctx.Mul(s.T, A(n1), s.P /* P1 */)
	case 2:
		n2 := n1 + 1

		// B = B1 * B2
		// P = P1 * P2
		// Q = Q1 * Q2
		// T =
		//   t0 = (A1 *    * B2 * P1 *          Q2) +
		//   t1 = (A2 * B1 *    * P1 * P2 *       )

		s.P.Copy(P(n1))
		s.B.Copy(B(n2))
		s.Q.Copy(Q(n2))

		// T = A1*P1*B2*Q2 + B1*A2*P12
		// Compute the first half of T.
		ctx.Mul(s.T, A(n1), s.P /* P1 */)
		ctx.Mul(s.T, s.T, s.B /* B2 */)
		ctx.Mul(s.T, s.T, s.Q /* Q2 */)

		// We no longer need Q, so compute Q.
		ctx.Mul(s.Q, s.Q, Q(n1))

		// We no longer need B2, so grab B1 and then compute B.
		B1 := B(n1)
		ctx.Mul(s.B, s.B, B1)

		// We no longer need P1 or P2, so calculate P12 which is needed for the
		// second half of T.
		ctx.Mul(s.P, s.P, P(n2))

		// Finish computing T.
		t1 := new(Big)
		ctx.Mul(t1, B1, A(n2))
		ctx.FMA(s.T, t1, s.P /* P12 */, s.T) // combine the final multiply with t0 + t1
	case 3:
		n2 := n1 + 1
		n3 := n2 + 1

		// B = B1 * B2 * B3
		// P = P1 * P2 * P3
		// Q = Q1 * Q2 * Q3
		// T =
		//   t0 = (A1 *      B2 * B3 * P1 *          __ * Q2 * Q3) +
		//   t1 = (A2 * B1 *      B3 * P1 * P2 *               Q3) +
		//   t2 = (A3 * B1 * B2 *      P1 * P2 * P3              )

		// A{1,2,3} are transient, so we don't need to store them.
		A1 := A(n1)

		s.P.Copy(P(n1)) // P = P1

		// T_0 = A1 * __ * __ * P1 * __ * __
		//            B2   B3        Q2   Q3
		ctx.Mul(s.T, A1, s.P)

		// P = P1 * P2 since we need it for t1.
		ctx.Mul(s.P, s.P, P(n2))

		t1 := new(Big)
		// T_1 = A2 * __ * __ * P1 * P2 * __
		//            B1   B3             Q3
		ctx.Mul(t1, A(n2), s.P)

		// P = P1 * P2 * P3; P is finished.
		ctx.Mul(s.P, s.P, P(n3))

		t2 := new(Big)
		// T_2 = A3 * __ * __ * P1 * P2 * P3
		//            B1   B2
		ctx.Mul(t2, A(n3), s.P)

		B1 := B(n1)
		s.B.Copy(B1)
		// T_1 = A2 * B1 * __ * P1 * P2 * __
		//                 B3             Q3
		ctx.Mul(t1, t1, s.B /* B1 */)

		B2 := B(n2)
		// T_0 = A1 * B2 * __ * P1 * __ * __
		//                 B3        Q2   Q3
		ctx.Mul(s.T, s.T, B2)

		// B = B1 * B2
		ctx.Mul(s.B, s.B, B(n2))

		// T_2 = A3 * B1 * B2 * P1 * P2 * P3; T_2 is finished.
		ctx.Mul(t2, t2, s.B /* B12 */)

		// T_1 = A2 * B1 * __ P1 * P2 * __
		//                 B3           Q3
		ctx.Mul(t1, t1, B2)

		B3 := B(n3)
		// T_0 = A1 * B2 * B3 * P1 * __ * __
		//                           Q2   Q3
		ctx.Mul(s.T, s.T, B3)
		// T_1 = A3 * B1 * B3 * P1 * P2 * P3 * __
		//                                     Q3
		ctx.Mul(t1, t1, B3)

		// B = B1 * B2 * B3; B is finished.
		ctx.Mul(s.B, s.B, B3)

		// Q = Q3.
		s.Q.Copy(Q(n3))

		// T_1 = A2 * B1 * B3 * P1 * P2 * Q3; T_1 is finished.
		ctx.Mul(t1, t1, s.Q)
		// Q = Q2 * Q3.
		ctx.Mul(s.Q, s.Q, Q(n2))
		// T_0 = A1 * B2 * B3 * P1 * Q2 * Q3; T_0 is finished.
		ctx.Mul(s.T, s.T, s.Q)
		// Q = Q1 * Q2 * Q3; Q is finished.
		ctx.Mul(s.Q, s.Q, Q(n1))

		// T = T_0 + T_1 + T_2; T is finsihed.
		ctx.Add(s.T, s.T, t1)
		ctx.Add(s.T, s.T, t2)
	case 4:
		n2 := n1 + 1
		n3 := n2 + 1
		n4 := n3 + 1

		// B = B1 * B2 * B3 * B3
		// P = P1 * P2 * P3 * P4
		// Q = Q1 * Q2 * Q3 * Q3
		// T =
		//   t0 = (A1 * P1 *                     B2 * B3 * B4 * __ * Q2 * Q3 * Q4) +
		//   t1 = (A2 * P1 * P2 *           B1 *      B3 * B4 *           Q3 * Q4) +
		//   t2 = (A3 * P1 * P2 * P3 *      B1 * B2 *      B4 *                Q4) +
		//   t3 = (A4 * P1 * P2 * P3 * P4 * B1 * B2 * B3                         )

		// A{1,2,3,4} are transient, so we don't need to store them.

		t1 := new(Big)
		t2 := new(Big)
		t3 := new(Big).Copy(A(n4)) // T_3 needs: P1234, B123

		s.Q.Copy(Q(n4))          // Q = Q4.
		ctx.Mul(t2, A(n3), s.Q)  // T_2 needs: P123, B124.
		ctx.Mul(s.Q, s.Q, Q(n3)) // Q = Q34.
		ctx.Mul(t1, A(n2), s.Q)  // T_1 needs: P12, B134.
		ctx.Mul(s.Q, s.Q, Q(n2)) // Q = Q234.
		ctx.Mul(s.T, A(n1), s.Q) // T_0 needs: P1, B234.
		ctx.Mul(s.Q, s.Q, Q(n1)) // Q = Q1234; Q is finished.

		s.P.Copy(P(n1))          // P = P1.
		ctx.Mul(s.T, s.T, s.P)   // T_0 needs: B234.
		ctx.Mul(s.P, s.P, P(n2)) // P = P12.
		ctx.Mul(t1, t1, s.P)     // T_1 needs: B134.
		ctx.Mul(s.P, s.P, P(n3)) // P = P123.
		ctx.Mul(t2, t2, s.P)     // T_2 needs: B12.
		ctx.Mul(s.P, s.P, P(n4)) // P = P1234; P is finished.
		ctx.Mul(t3, t3, s.P)     // T_3 needs: B123.

		b1 := new(Big).Copy(B(n1))
		ctx.Mul(t3, t3, b1)            // T_3 needs: B23.
		ctx.Mul(t2, t2, b1)            // T_2 needs: B2.
		ctx.Mul(t1, t1, b1)            // T_1 is finished.
		s.B.Copy(B(n2))                // B = B2.
		ctx.Mul(t2, t2, s.B /* B2 */)  // T_2 is finished.
		ctx.Mul(s.B, s.B, B(n3))       // B = B23.
		ctx.Mul(t3, t3, s.B /* B23 */) // T_3 is finished.
		ctx.Mul(s.T, s.T, s.B)         // T_0 is finished.
		ctx.Mul(s.B, s.B, b1)          // B = B123.
		ctx.Mul(s.B, s.B, B(n4))       // B = B1234.

		ctx.Add(s.T, s.T, t1) // T = T_0 + T_1
		ctx.Add(s.T, s.T, t2) // T = T_0 + T_1 + T_2
		ctx.Add(s.T, s.T, t3) // T = T_0 + T_1 + T_2 + T_3
	default:
		// here we have something bigger so we'll do a binary split
		// first find the mid point between the points and create the two side
		// then do the calculations and return the value
		m := uint64((start + end) / 2)

		// We can reuse s as one of the states.
		s.calculate(ctx, start, m, A, P, B, Q)

		r := newState()
		r.calculate(ctx, m, end, A, P, B, Q)

		// Generically, the following is done
		//
		//     B = B_l*B_r
		//     P = P_l*P_r
		//     Q = Q_l*Q_r
		//     T = B_l*P_l*T_r + B_r*Q_r*T_l
		//
		s.combine(ctx, s, r)
	}
}

// combine computes the following:
//
//     B = B_l*B_r
//     P = P_l*P_r
//     Q = Q_l*Q_r
//     T = B_l*P_l*T_r + B_r*Q_r*T_l
//
func (s *apbqBinarySplitState) combine(ctx Context, L, R *apbqBinarySplitState) {
	// T = L.B*L.P*R.T, t1 = R.B*R.Q*L.T
	t0 := getDec(ctx)
	ctx.Mul(t0, L.B, L.P)
	ctx.Mul(t0, t0, R.T)

	t1 := getDec(ctx)
	ctx.Mul(t1, R.B, R.Q)
	ctx.FMA(s.T, t1, L.T, t0) // combine the final multiply and t0 + t1

	ctx.Mul(s.B, L.B, R.B)
	ctx.Mul(s.P, L.P, R.P)
	ctx.Mul(s.Q, L.Q, R.Q)

	putDec(t0)
	putDec(t1)
}
