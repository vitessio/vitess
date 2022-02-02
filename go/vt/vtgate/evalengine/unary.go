package evalengine

import querypb "vitess.io/vitess/go/vt/proto/query"

type (
	UnaryExpr struct {
		Inner Expr
	}

	NegateExpr struct {
		UnaryExpr
	}
)

func (c *UnaryExpr) typeof(env *ExpressionEnv) querypb.Type {
	return c.Inner.typeof(env)
}

func (n *NegateExpr) eval(env *ExpressionEnv, result *EvalResult) {
	result.init(env, n.Inner)
	result.negateNumeric()
}

func (n *NegateExpr) typeof(env *ExpressionEnv) querypb.Type {
	// the type of a NegateExpr is not known beforehand because negating
	// a large enough value can cause it to be upcasted into a larger type
	return -1
}
