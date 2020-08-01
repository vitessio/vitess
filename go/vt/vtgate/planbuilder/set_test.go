/*
Copyright 2020 The Vitess Authors.

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

package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestExpressionOkToDelegateToTablet(t *testing.T) {
	tests := []struct {
		expr   string
		expect bool
	}{{
		expr:   "42",
		expect: true,
	}, {
		expr:   "(select count(*) from someTable)",
		expect: false,
	}, {
		expr:   "TIMESTAMP()",
		expect: false,
	}, {
		expr:   "concat('a','b')",
		expect: true,
	}}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			stmt, err := sqlparser.Parse("select " + tt.expr)
			require.NoError(t, err)
			e := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
			got := expressionOkToDelegateToTablet(e)
			require.Equal(t, tt.expect, got)
		})
	}
}
