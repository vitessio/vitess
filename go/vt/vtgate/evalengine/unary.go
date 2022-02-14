package evalengine

import "vitess.io/vitess/go/sqltypes"

type (
	UnaryExpr struct {
		Inner Expr
	}

	NegateExpr struct {
		UnaryExpr
	}
)

func (c *UnaryExpr) typeof(env *ExpressionEnv) sqltypes.Type {
	return c.Inner.typeof(env)
}

func (n *NegateExpr) eval(env *ExpressionEnv, result *EvalResult) {
	result.init(env, n.Inner)
	result.negateNumeric()
}

func (n *NegateExpr) typeof(env *ExpressionEnv) sqltypes.Type {
	// the type of a NegateExpr is not known beforehand because negating
	// a large enough value can cause it to be upcasted into a larger type
	return -1
}
