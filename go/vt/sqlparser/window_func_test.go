/*
Copyright 2025 The Vitess Authors.

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

package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWindowFunc(t *testing.T) {
	overClause := &OverClause{WindowName: NewIdentifierCI("w")}
	arg1 := NewIntLiteral("1")
	arg2 := NewIntLiteral("2")
	arg3 := NewIntLiteral("3")

	// Clauses
	respectNulls := &NullTreatmentClause{Type: RespectNullsType}
	ignoreNulls := &NullTreatmentClause{Type: IgnoreNullsType}
	fromFirst := &FromFirstLastClause{Type: FromFirstType}
	fromLast := &FromFirstLastClause{Type: FromLastType}

	tests := []struct {
		name               string
		node               WindowFunc
		wantWindowFuncName string
		wantArgs           []Expr
	}{
		// Aggregate Functions
		// https://dev.mysql.com/doc/refman/8.0/en/window-functions-usage.html
		{
			name:               "Count",
			node:               &Count{Args: []Expr{arg1}, OverClause: overClause},
			wantWindowFuncName: "count",
			wantArgs:           []Expr{arg1},
		},
		{
			name:               "CountStar",
			node:               &CountStar{OverClause: overClause},
			wantWindowFuncName: "count",
			wantArgs:           nil,
		},
		{
			name:               "Sum",
			node:               &Sum{Arg: arg1, OverClause: overClause},
			wantWindowFuncName: "sum",
			wantArgs:           []Expr{arg1},
		},
		{
			name:               "Avg",
			node:               &Avg{Arg: arg1, OverClause: overClause},
			wantWindowFuncName: "avg",
			wantArgs:           []Expr{arg1},
		},
		{
			name:               "Max",
			node:               &Max{Arg: arg1, OverClause: overClause},
			wantWindowFuncName: "max",
			wantArgs:           []Expr{arg1},
		},
		{
			name:               "Min",
			node:               &Min{Arg: arg1, OverClause: overClause},
			wantWindowFuncName: "min",
			wantArgs:           []Expr{arg1},
		},
		{
			name:               "BitAnd",
			node:               &BitAnd{Arg: arg1, OverClause: overClause},
			wantWindowFuncName: "bit_and",
			wantArgs:           []Expr{arg1},
		},
		{
			name:               "BitOr",
			node:               &BitOr{Arg: arg1, OverClause: overClause},
			wantWindowFuncName: "bit_or",
			wantArgs:           []Expr{arg1},
		},
		{
			name:               "BitXor",
			node:               &BitXor{Arg: arg1, OverClause: overClause},
			wantWindowFuncName: "bit_xor",
			wantArgs:           []Expr{arg1},
		},
		{
			name:               "Std",
			node:               &Std{Arg: arg1, OverClause: overClause},
			wantWindowFuncName: "std",
			wantArgs:           []Expr{arg1},
		},
		{
			name:               "StdDev",
			node:               &StdDev{Arg: arg1, OverClause: overClause},
			wantWindowFuncName: "stddev",
			wantArgs:           []Expr{arg1},
		},
		{
			name:               "StdPop",
			node:               &StdPop{Arg: arg1, OverClause: overClause},
			wantWindowFuncName: "stddev_pop",
			wantArgs:           []Expr{arg1},
		},
		{
			name:               "StdSamp",
			node:               &StdSamp{Arg: arg1, OverClause: overClause},
			wantWindowFuncName: "stddev_samp",
			wantArgs:           []Expr{arg1},
		},
		{
			name:               "VarPop",
			node:               &VarPop{Arg: arg1, OverClause: overClause},
			wantWindowFuncName: "var_pop",
			wantArgs:           []Expr{arg1},
		},
		{
			name:               "VarSamp",
			node:               &VarSamp{Arg: arg1, OverClause: overClause},
			wantWindowFuncName: "var_samp",
			wantArgs:           []Expr{arg1},
		},
		{
			name:               "Variance",
			node:               &Variance{Arg: arg1, OverClause: overClause},
			wantWindowFuncName: "variance",
			wantArgs:           []Expr{arg1},
		},
		{
			name:               "JSONArrayAgg",
			node:               &JSONArrayAgg{Expr: arg1, OverClause: overClause},
			wantWindowFuncName: "json_arrayagg",
			wantArgs:           []Expr{arg1},
		},
		{
			name:               "JSONObjectAgg",
			node:               &JSONObjectAgg{Key: arg1, Value: arg2, OverClause: overClause},
			wantWindowFuncName: "json_objectagg",
			wantArgs:           []Expr{arg1, arg2},
		},

		// Non-Aggregate Window Functions
		// https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html
		{
			name:               "CumeDist",
			node:               &ArgumentLessWindowExpr{Type: CumeDistExprType, OverClause: overClause},
			wantWindowFuncName: "cume_dist",
			wantArgs:           nil,
		},
		{
			name:               "DenseRank",
			node:               &ArgumentLessWindowExpr{Type: DenseRankExprType, OverClause: overClause},
			wantWindowFuncName: "dense_rank",
			wantArgs:           nil,
		},
		{
			name:               "PercentRank",
			node:               &ArgumentLessWindowExpr{Type: PercentRankExprType, OverClause: overClause},
			wantWindowFuncName: "percent_rank",
			wantArgs:           nil,
		},
		{
			name:               "Rank",
			node:               &ArgumentLessWindowExpr{Type: RankExprType, OverClause: overClause},
			wantWindowFuncName: "rank",
			wantArgs:           nil,
		},
		{
			name:               "RowNumber",
			node:               &ArgumentLessWindowExpr{Type: RowNumberExprType, OverClause: overClause},
			wantWindowFuncName: "row_number",
			wantArgs:           nil,
		},
		// FIRST_VALUE(expr) [null_treatment] OVER ...
		// https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html#function_first-value
		{
			name:               "FirstValue",
			node:               &FirstOrLastValueExpr{Type: FirstValueExprType, Expr: arg1, OverClause: overClause},
			wantWindowFuncName: "first_value",
			wantArgs:           []Expr{arg1},
		},
		{
			name:               "FirstValue with Ignore Nulls",
			node:               &FirstOrLastValueExpr{Type: FirstValueExprType, Expr: arg1, OverClause: overClause, NullTreatmentClause: ignoreNulls},
			wantWindowFuncName: "first_value",
			wantArgs:           []Expr{arg1},
		},
		// LAST_VALUE(expr) [null_treatment] OVER ...
		// https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html#function_last-value
		{
			name:               "LastValue",
			node:               &FirstOrLastValueExpr{Type: LastValueExprType, Expr: arg1, OverClause: overClause},
			wantWindowFuncName: "last_value",
			wantArgs:           []Expr{arg1},
		},
		{
			name:               "LastValue with Respect Nulls",
			node:               &FirstOrLastValueExpr{Type: LastValueExprType, Expr: arg1, OverClause: overClause, NullTreatmentClause: respectNulls},
			wantWindowFuncName: "last_value",
			wantArgs:           []Expr{arg1},
		},
		// NTILE(N) OVER ...
		// https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html#function_ntile
		{
			name:               "Ntile",
			node:               &NtileExpr{N: arg1, OverClause: overClause},
			wantWindowFuncName: "ntile",
			wantArgs:           []Expr{arg1},
		},
		// NTH_VALUE(expr, N) [from_first_last] [null_treatment] OVER ...
		// https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html#function_nth-value
		{
			name:               "NthValue",
			node:               &NTHValueExpr{Expr: arg1, N: arg2, OverClause: overClause},
			wantWindowFuncName: "nth_value",
			wantArgs:           []Expr{arg1, arg2},
		},
		{
			name:               "NthValue with From First",
			node:               &NTHValueExpr{Expr: arg1, N: arg2, OverClause: overClause, FromFirstLastClause: fromFirst},
			wantWindowFuncName: "nth_value",
			wantArgs:           []Expr{arg1, arg2},
		},
		{
			name:               "NthValue with From Last and Ignore Nulls",
			node:               &NTHValueExpr{Expr: arg1, N: arg2, OverClause: overClause, FromFirstLastClause: fromLast, NullTreatmentClause: ignoreNulls},
			wantWindowFuncName: "nth_value",
			wantArgs:           []Expr{arg1, arg2},
		},
		// LAG(expr [, N [, default]]) [null_treatment] OVER ...
		// https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html#function_lag
		{
			name:               "Lag (3 args)",
			node:               &LagLeadExpr{Type: LagExprType, Expr: arg1, N: arg2, Default: arg3, OverClause: overClause},
			wantWindowFuncName: "lag",
			wantArgs:           []Expr{arg1, arg2, arg3},
		},
		{
			name:               "Lag (2 args)",
			node:               &LagLeadExpr{Type: LagExprType, Expr: arg1, N: arg2, OverClause: overClause},
			wantWindowFuncName: "lag",
			wantArgs:           []Expr{arg1, arg2},
		},
		{
			name:               "Lag (1 arg)",
			node:               &LagLeadExpr{Type: LagExprType, Expr: arg1, OverClause: overClause},
			wantWindowFuncName: "lag",
			wantArgs:           []Expr{arg1},
		},
		{
			name:               "Lag with Null Treatment",
			node:               &LagLeadExpr{Type: LagExprType, Expr: arg1, OverClause: overClause, NullTreatmentClause: ignoreNulls},
			wantWindowFuncName: "lag",
			wantArgs:           []Expr{arg1},
		},
		// LEAD(expr [, N [, default]]) [null_treatment] OVER ...
		// https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html#function_lead
		{
			name:               "Lead (3 args)",
			node:               &LagLeadExpr{Type: LeadExprType, Expr: arg1, N: arg2, Default: arg3, OverClause: overClause},
			wantWindowFuncName: "lead",
			wantArgs:           []Expr{arg1, arg2, arg3},
		},
		{
			name:               "Lead (2 args)",
			node:               &LagLeadExpr{Type: LeadExprType, Expr: arg1, N: arg2, OverClause: overClause},
			wantWindowFuncName: "lead",
			wantArgs:           []Expr{arg1, arg2},
		},
		{
			name:               "Lead (1 arg)",
			node:               &LagLeadExpr{Type: LeadExprType, Expr: arg1, OverClause: overClause},
			wantWindowFuncName: "lead",
			wantArgs:           []Expr{arg1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantWindowFuncName, tt.node.WindowFuncName())
			assert.Equal(t, overClause, tt.node.GetOverClause())
			assert.Equal(t, tt.wantArgs, tt.node.GetArgs())
		})
	}
}
