package splitquery

// utils.go contains general utility functions used in the splitquery package.

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// populateNewBindVariable inserts 'bindVariableName' with 'bindVariableValue' to the
// 'resultBindVariables' map. Panics if 'bindVariableName' already exists in the map.
func populateNewBindVariable(
	bindVariableName string,
	bindVariableValue interface{},
	resultBindVariables map[string]interface{}) {
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
func cloneBindVariables(bindVariables map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
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
