// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

// analyzer.go contains utility analysis functions.
// TODO(sougou): Move some generic functions out of execution.go
// and router.go into this file.

import "fmt"

// GetDBName parses the specified DML and returns the
// db name if it was used to qualify the table name.
// It returns an error if parsing fails or if the statement
// is not a DML.
func GetDBName(sql string) (string, error) {
	statement, err := Parse(sql)
	if err != nil {
		return "", err
	}
	switch stmt := statement.(type) {
	case *Insert:
		return string(stmt.Table.Qualifier), nil
	case *Update:
		return string(stmt.Table.Qualifier), nil
	case *Delete:
		return string(stmt.Table.Qualifier), nil
	}
	return "", fmt.Errorf("statement '%s' is not a dml", sql)
}

// GetColName returns the column name, only if
// it's a simple expression. Otherwise, it returns "".
func GetColName(node Expr) string {
	if n, ok := node.(*ColName); ok {
		return string(n.Name)
	}
	return ""
}

// GetTableName returns the table name from the SimpleTableExpr
// only if it's a simple expression. Otherwise, it returns "".
func GetTableName(node SimpleTableExpr) string {
	if n, ok := node.(*TableName); ok && n.Qualifier == nil {
		return string(n.Name)
	}
	// sub-select or '.' expression
	return ""
}

// IsColName returns true if the ValExpr is a *ColName.
func IsColName(node ValExpr) bool {
	_, ok := node.(*ColName)
	return ok
}

// IsVal returns true if the ValExpr is a string, number or value arg.
// NULL is not considered to be a value.
func IsValue(node ValExpr) bool {
	switch node.(type) {
	case StrVal, NumVal, ValArg:
		return true
	}
	return false
}

// HasINCaluse returns true if an yof the conditions has an IN clause.
func HasINClause(conditions []BoolExpr) bool {
	for _, node := range conditions {
		if c, ok := node.(*ComparisonExpr); ok && c.Operator == AST_IN {
			return true
		}
	}
	return false
}

// IsSimpleTuple returns true if the ValExpr is a ValTuple that
// contains simple values.
func IsSimpleTuple(node ValExpr) bool {
	list, ok := node.(ValTuple)
	if !ok {
		// It's a subquery.
		return false
	}
	for _, n := range list {
		if !IsValue(n) {
			return false
		}
	}
	return true
}

// StringIn is a convenience function that returns
// true if str matches any of the values.
func StringIn(str string, values ...string) bool {
	for _, val := range values {
		if str == val {
			return true
		}
	}
	return false
}
