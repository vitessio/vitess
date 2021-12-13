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
	"vitess.io/vitess/go/mysql/collations"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type (
	BinaryLogicalExpr struct {
		Left, Right Expr
	}
	NotExpr struct {
		UnaryExpr
	}
)

func (n *NotExpr) eval(env *ExpressionEnv) (EvalResult, error) {
	res, err := n.Inner.eval(env)
	if err != nil {
		return EvalResult{}, err
	}
	b, err := res.truthy()
	if err != nil {
		return EvalResult{}, err
	}
	return b.not().evalResult(), nil
}

func (n *NotExpr) Type(*ExpressionEnv) (querypb.Type, error) {
	return querypb.Type_UINT64, nil
}

func (n *NotExpr) Collation() collations.TypedCollation {
	return collationNumeric
}
