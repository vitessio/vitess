package evalengine

import (
	"vitess.io/vitess/go/sqltypes"
)

type (
	ArithmeticExpr struct {
		BinaryExpr
		Op ArithmeticOp
	}

	// ArithmeticOp allows arithmetic expressions to not have to evaluate child expressions - this is done by the BinaryExpr
	ArithmeticOp interface {
		eval(left, right, out *EvalResult) error
		typeof(left sqltypes.Type) sqltypes.Type
		String() string
	}

	OpAddition       struct{}
	OpSubstraction   struct{}
	OpMultiplication struct{}
	OpDivision       struct{}
)

var _ ArithmeticOp = (*OpAddition)(nil)
var _ ArithmeticOp = (*OpSubstraction)(nil)
var _ ArithmeticOp = (*OpMultiplication)(nil)
var _ ArithmeticOp = (*OpDivision)(nil)

func (b *ArithmeticExpr) eval(env *ExpressionEnv, out *EvalResult) {
	var left, right EvalResult
	left.init(env, b.Left)
	right.init(env, b.Right)
	if left.null() || right.null() {
		out.setNull()
		return
	}
	if left.typeof() == sqltypes.Tuple || right.typeof() == sqltypes.Tuple {
		panic("failed to typecheck tuples")
	}
	if err := b.Op.eval(&left, &right, out); err != nil {
		throwEvalError(err)
	}
}

// typeof implements the Expr interface
func (b *ArithmeticExpr) typeof(env *ExpressionEnv) sqltypes.Type {
	// TODO: this is returning an unknown type for this arithmetic expression;
	// for some cases, it may be possible to calculate the resulting type
	// of the expression ahead of time, making the evaluation lazier.
	return -1
}

func (a *OpAddition) eval(left, right, out *EvalResult) error {
	return addNumericWithError(left, right, out)
}
func (a *OpAddition) typeof(left sqltypes.Type) sqltypes.Type { return left }
func (a *OpAddition) String() string                          { return "+" }

func (s *OpSubstraction) eval(left, right, out *EvalResult) error {
	return subtractNumericWithError(left, right, out)
}
func (s *OpSubstraction) typeof(left sqltypes.Type) sqltypes.Type { return left }
func (s *OpSubstraction) String() string                          { return "-" }

func (m *OpMultiplication) eval(left, right, out *EvalResult) error {
	return multiplyNumericWithError(left, right, out)
}
func (m *OpMultiplication) typeof(left sqltypes.Type) sqltypes.Type { return left }
func (m *OpMultiplication) String() string                          { return "*" }

func (d *OpDivision) eval(left, right, out *EvalResult) error {
	return divideNumericWithError(left, right, true, out)
}
func (d *OpDivision) typeof(sqltypes.Type) sqltypes.Type { return sqltypes.Float64 }
func (d *OpDivision) String() string                     { return "/" }
