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
	"testing"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/sqlparser"
)

// These tests are here because I use the evalengine to test for valid expressions

func TestSimplifyExpr(t *testing.T) {
	// ast struct for L0         +
	// L1                +            +
	// L2             +     +      +     +
	// L3            1 2   3 4    5 6   7 8

	// L3
	i1, i2, i3, i4, i5, i6, i7, i8 :=
		sqlparser.NewIntLiteral("1"),
		sqlparser.NewIntLiteral("2"),
		sqlparser.NewIntLiteral("3"),
		sqlparser.NewIntLiteral("4"),
		sqlparser.NewIntLiteral("5"),
		sqlparser.NewIntLiteral("6"),
		sqlparser.NewIntLiteral("7"),
		sqlparser.NewIntLiteral("8")
	// L2
	p21, p22, p23, p24 :=
		plus(i1, i2),
		plus(i3, i4),
		plus(i5, i6),
		plus(i7, i8)

	// L1
	p11, p12 :=
		plus(p21, p22),
		plus(p23, p24)

	// L0
	p0 := plus(p11, p12)

	expr := sqlparser.SimplifyExpr(p0, func(expr sqlparser.Expr) bool {
		local, err := ConvertEx(expr, nil, true)
		if err != nil {
			return false
		}
		res, err := (*ExpressionEnv)(nil).Evaluate(local)
		if err != nil {
			return false
		}
		toInt64, err := res.Value().ToInt64()
		if err != nil {
			return false
		}
		return toInt64 >= 8
	})
	log.Infof("simplest expr to evaluate to >= 8: [%s], started from: [%s]", sqlparser.String(expr), sqlparser.String(p0))

}

func plus(a, b sqlparser.Expr) sqlparser.Expr {
	return &sqlparser.BinaryExpr{
		Operator: sqlparser.PlusOp,
		Left:     a,
		Right:    b,
	}
}
