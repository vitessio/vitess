/*
Copyright 2022 The Vitess Authors.

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

// IsExpr represents the IS expression in MySQL.
// boolean_primary IS [NOT] {TRUE | FALSE | NULL}
type IsExpr struct {
	Expr Expr

	Negate bool

	// only one of the following should be set to true
	Null  bool
	True  bool
	False bool
}

func newIsNull(e Expr) *IsExpr {
	return &IsExpr{
		Expr:   e,
		Negate: false,
		Null:   true,
	}
}

func newIsNotNull(e Expr) *IsExpr {
	return &IsExpr{
		Expr:   e,
		Negate: true,
		Null:   true,
	}
}

func newIsTrue(e Expr) *IsExpr {
	return &IsExpr{
		Expr:   e,
		Negate: false,
		True:   true,
	}
}

func newIsFalse(e Expr) *IsExpr {
	return &IsExpr{
		Expr:   e,
		Negate: false,
		False:  true,
	}
}

func newIsNotTrue(e Expr) *IsExpr {
	return &IsExpr{
		Expr:   e,
		Negate: true,
		True:   true,
	}
}

func newIsNotFalse(e Expr) *IsExpr {
	return &IsExpr{
		Expr:   e,
		Negate: true,
		False:  true,
	}
}

var _ Expr = (*IsExpr)(nil)

func (i *IsExpr) eval(env *ExpressionEnv, result *EvalResult) {
	in := EvalResult{}
	in.init(env, i.Expr)
	var cmp bool
	switch {
	case in.null() && (i.True || i.False):
		// null IS [TRUE|FALSE]
		cmp = i.Negate
	case i.Null:
		cmp = in.null() == !i.Negate
	case i.True:
		cmp = in.uint64() != 0 == !i.Negate
	case i.False:
		cmp = in.uint64() == 0 == !i.Negate
	default:
		panic("should not happen")
	}
	result.setBool(cmp)
}

func (i *IsExpr) typeof(env *ExpressionEnv) querypb.Type {
	return querypb.Type_UINT64
}

func (i *IsExpr) collation() collations.TypedCollation {
	return collationNumeric
}
