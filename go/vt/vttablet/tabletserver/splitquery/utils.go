/*
Copyright 2019 The Vitess Authors.

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

package splitquery

// utils.go contains general utility functions used in the splitquery package.

import (
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// populateNewBindVariable inserts 'bindVariableName' with 'bindVariableValue' to the
// 'resultBindVariables' map. Panics if 'bindVariableName' already exists in the map.
func populateNewBindVariable(
	bindVariableName string,
	bindVariableValue *querypb.BindVariable,
	resultBindVariables map[string]*querypb.BindVariable) {
	_, alreadyInMap := resultBindVariables[bindVariableName]
	if alreadyInMap {
		panic(fmt.Sprintf(
			"bindVariable %v already exists in map: %v. bindVariableValue given: %v",
			bindVariableName,
			resultBindVariables,
			bindVariableValue))
	}
	resultBindVariables[bindVariableName] = bindVariableValue
}

// cloneBindVariables returns a shallow-copy of the given bindVariables map.
func cloneBindVariables(bindVariables map[string]*querypb.BindVariable) map[string]*querypb.BindVariable {
	result := make(map[string]*querypb.BindVariable)
	for key, value := range bindVariables {
		result[key] = value
	}
	return result
}

// addAndTermToWhereClause replaces the WHERE clause of the query given in 'selectAST' with
// a new WHERE clause consisting of the boolean expression in the original 'WHERE' clause with
// the additional 'andTerm' ANDed to it.
// Note that it does not modify the original 'Where' clause  of 'selectAST' (so it does not affect
// other objects holding a pointer to the original 'Where' clause).
// If 'selectAST' does not have a WHERE clause, this function will add a new WHERE clause
// consisting of 'andTerm' alone.
func addAndTermToWhereClause(selectAST *sqlparser.Select, andTerm sqlparser.Expr) {
	if selectAST.Where == nil || selectAST.Where.Expr == nil {
		selectAST.Where = sqlparser.NewWhere(sqlparser.WhereStr, andTerm)
	} else {
		selectAST.Where = sqlparser.NewWhere(sqlparser.WhereStr,
			&sqlparser.AndExpr{
				Left:  &sqlparser.ParenExpr{Expr: selectAST.Where.Expr},
				Right: &sqlparser.ParenExpr{Expr: andTerm},
			},
		)
	}
}

// int64Max computes the max of two int64 values.
func int64Max(a, b int64) int64 {
	if b > a {
		return b
	}
	return a
}
