package evalengine

import (
	"encoding/json"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type RouteValue struct {
	Expr Expr
}

// ResolveValue allows for retrieval of the value we expose for public consumption
func (rv *RouteValue) ResolveValue(bindVars map[string]*querypb.BindVariable) (sqltypes.Value, error) {
	env := EnvWithBindVars(bindVars)
	evalResul, err := env.Evaluate(rv.Expr)
	if err != nil {
		return sqltypes.Value{}, err
	}
	return evalResul.Value(), nil
}

// ResolveList allows for retrieval of the value we expose for public consumption
func (rv *RouteValue) ResolveList(bindVars map[string]*querypb.BindVariable) ([]sqltypes.Value, error) {
	env := EnvWithBindVars(bindVars)
	evalResul, err := env.Evaluate(rv.Expr)
	if err != nil {
		return nil, err
	}
	return evalResul.TupleValues(), nil
}

func (rv *RouteValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(FormatExpr(rv.Expr))
}
