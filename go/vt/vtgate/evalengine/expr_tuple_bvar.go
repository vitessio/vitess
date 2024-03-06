/*
Copyright 2024 The Vitess Authors.

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
	"errors"

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
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "query argument '%s' must be a tuple (is %s)", bv.Key, bvar.Type.String())
	}

	tuple := make([]eval, 0, len(bvar.Values))
	for _, value := range bvar.Values {
		if value.Type != sqltypes.Tuple {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "result value must be a tuple (is %s)", value.Type.String())
		}
		sValue := sqltypes.ProtoToValue(value)
		var evalErr error
		idx := 0
		found := false
		// looking for a single index on each Tuple Value.
		loopErr := sValue.ForEachValue(func(val sqltypes.Value) {
			if found || idx != bv.Index {
				idx++
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
		if err = errors.Join(loopErr, evalErr); err != nil {
			return nil, err
		}
		if !found {
			return nil, vterrors.VT13001("value not found in the bind variable")
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
