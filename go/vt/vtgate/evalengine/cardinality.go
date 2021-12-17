/*
Copyright 2021 The Vitess Authors.

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
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func cardinalityError(expected int) error {
	return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.OperandColumns, "Operand should contain %d column(s)", expected)
}

func checkTupleCardinality(lVal, rVal *EvalResult) (bool, error) {
	switch {
	case lVal.typeof() == querypb.Type_TUPLE && rVal.typeof() == querypb.Type_TUPLE:
		return true, nil
	case lVal.typeof() == querypb.Type_TUPLE:
		return false, cardinalityError(len(lVal.tuple()))
	case rVal.typeof() == querypb.Type_TUPLE:
		return false, cardinalityError(1)
	default:
		return false, nil
	}
}

func (expr *BinaryExpr) ensureCardinality(env *ExpressionEnv, expected int) {
	if expected != 1 {
		throwEvalError(cardinalityError(1))
	}
}

func (expr *UnaryExpr) ensureCardinality(env *ExpressionEnv, expected int) {
	if expected != 1 {
		throwEvalError(cardinalityError(1))
	}
}
