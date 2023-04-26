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

package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/vtgate/semantics"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestCheckIfAlreadyExists(t *testing.T) {
	tests := []struct {
		name string
		expr *sqlparser.AliasedExpr
		sel  *sqlparser.Select
		want int
	}{
		{
			name: "No alias, both ColName",
			want: 0,
			expr: &sqlparser.AliasedExpr{Expr: sqlparser.NewColName("id")},
			sel:  &sqlparser.Select{SelectExprs: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{Expr: sqlparser.NewColName("id")}}},
		},
		{
			name: "Aliased expression and ColName",
			want: 0,
			expr: &sqlparser.AliasedExpr{Expr: sqlparser.NewColName("user_id")},
			sel:  &sqlparser.Select{SelectExprs: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{As: sqlparser.NewIdentifierCI("user_id"), Expr: sqlparser.NewColName("id")}}},
		},
		{
			name: "Non-ColName expressions",
			want: 0,
			expr: &sqlparser.AliasedExpr{Expr: sqlparser.NewStrLiteral("test")},
			sel:  &sqlparser.Select{SelectExprs: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{Expr: sqlparser.NewStrLiteral("test")}}},
		},
		{
			name: "No alias, multiple ColName in projection",
			want: 1,
			expr: &sqlparser.AliasedExpr{Expr: sqlparser.NewColName("id")},
			sel:  &sqlparser.Select{SelectExprs: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{Expr: sqlparser.NewColName("foo")}, &sqlparser.AliasedExpr{Expr: sqlparser.NewColName("id")}}},
		},
		{
			name: "No matching entry",
			want: -1,
			expr: &sqlparser.AliasedExpr{Expr: sqlparser.NewColName("id")},
			sel:  &sqlparser.Select{SelectExprs: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{Expr: sqlparser.NewColName("foo")}, &sqlparser.AliasedExpr{Expr: sqlparser.NewColName("name")}}},
		},
		{
			name: "No AliasedExpr in projection",
			want: -1,
			expr: &sqlparser.AliasedExpr{Expr: sqlparser.NewColName("id")},
			sel:  &sqlparser.Select{SelectExprs: []sqlparser.SelectExpr{&sqlparser.StarExpr{TableName: sqlparser.TableName{Name: sqlparser.NewIdentifierCS("user")}}, &sqlparser.StarExpr{TableName: sqlparser.TableName{Name: sqlparser.NewIdentifierCS("people")}}}},
		},
	}
	for _, tt := range tests {
		semTable := semantics.EmptySemTable()
		t.Run(tt.name, func(t *testing.T) {
			got := checkIfAlreadyExists(tt.expr, tt.sel, semTable)
			assert.Equal(t, tt.want, got)
		})
	}
}
