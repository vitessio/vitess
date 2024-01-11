/*
Copyright 2023 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package evalengine

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	TupleBindVariable struct {
		Key string

		Index     int
		Type      sqltypes.Type
		Collation collations.ID

		// dynamicTypeOffset is set when the type of this bind variable cannot be calculated
		// at translation time. Since expressions with dynamic types cannot be compiled ahead of time,
		// compilation will be delayed until the expression is first executed with the bind variables
		// sent by the user. See: UntypedExpr
		dynamicTypeOffset int
	}

	Offset struct {
	}
)

var _ IR = (*TupleBindVariable)(nil)
var _ Expr = (*TupleBindVariable)(nil)

func (bv *TupleBindVariable) IR() IR {
	return bv
}

func (bv *TupleBindVariable) IsExpr() {}

// eval implements the expression interface
func (bv *TupleBindVariable) eval(env *ExpressionEnv) (eval, error) {
	bvar, err := env.lookupBindVar(bv.Key)
	if err != nil {
		return nil, err
	}

	if bvar.Type != sqltypes.Tuple {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "query argument '%s' must be a tuple (is %s)", bv.Key, bvar.Type)
	}

	tuple := make([]eval, 0, len(bvar.Values))
	for _, value := range bvar.Values {
		if value.Type != sqltypes.Tuple {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "result value must be a tuple (is %s)", value.Type)
		}
		sValue := sqltypes.ProtoToValue(value)
		var evalErr error
		idx := 0
		found := false
		loopErr := sValue.ForEachValue(func(val sqltypes.Value) {
			idx++
			if idx-1 != bv.Index {
				return
			}
			found = true
			e, err := valueToEval(val, typedCoercionCollation(val.Type(), collations.CollationForType(val.Type(), bv.Collation)))
			if err != nil {
				evalErr = err
				return
			}
			tuple = append(tuple, e)

		})
		if loopErr != nil {
			return nil, loopErr
		}
		if evalErr != nil {
			return nil, evalErr
		}
		if !found {
			return nil, vterrors.VT13001("value not found in bind variable")
		}
	}
	return &evalTuple{t: tuple}, nil
}

// typeof implements the expression interface
func (bv *TupleBindVariable) typeof(env *ExpressionEnv) (ctype, error) {
	_, err := env.lookupBindVar(bv.Key)
	if err != nil {
		return ctype{}, err
	}

	return ctype{Type: sqltypes.Tuple}, nil
}

func (bv *TupleBindVariable) compile(c *compiler) (ctype, error) {
	return ctype{}, c.unsupported(bv)
}
