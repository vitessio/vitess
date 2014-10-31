// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

// analyzer.go contains utility analysis functions.

import (
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
)

// GetTableName returns the table name from the SimpleTableExpr
// only if it's a simple expression. Otherwise, it returns "".
func GetTableName(node SimpleTableExpr) string {
	if n, ok := node.(*TableName); ok && n.Qualifier == nil {
		return string(n.Name)
	}
	// sub-select or '.' expression
	return ""
}

// GetColName returns the column name, only if
// it's a simple expression. Otherwise, it returns "".
func GetColName(node Expr) string {
	if n, ok := node.(*ColName); ok {
		return string(n.Name)
	}
	return ""
}

// IsColName returns true if the ValExpr is a *ColName.
func IsColName(node ValExpr) bool {
	_, ok := node.(*ColName)
	return ok
}

// IsValue returns true if the ValExpr is a string, number or value arg.
// NULL is not considered to be a value.
func IsValue(node ValExpr) bool {
	switch node.(type) {
	case StrVal, NumVal, ValArg:
		return true
	}
	return false
}

// HasINCaluse returns true if any of the conditions has an IN clause.
func HasINClause(conditions []BoolExpr) bool {
	for _, node := range conditions {
		if c, ok := node.(*ComparisonExpr); ok && c.Operator == AST_IN {
			return true
		}
	}
	return false
}

// IsSimpleTuple returns true if the ValExpr is a ValTuple that
// contains simple values or if it's a list arg.
func IsSimpleTuple(node ValExpr) bool {
	switch vals := node.(type) {
	case ValTuple:
		for _, n := range vals {
			if !IsValue(n) {
				return false
			}
		}
		return true
	case ListArg:
		return true
	}
	// It's a subquery
	return false
}

// AsInterface converts the ValExpr to an interface. It converts
// ValTuple to []interface{}, ValArg to string, StrVal to sqltypes.String,
// NumVal to sqltypes.Numeric, NullVal to nil.
// Otherwise, it returns an error.
func AsInterface(node ValExpr) (interface{}, error) {
	switch node := node.(type) {
	case ValTuple:
		vals := make([]interface{}, 0, len(node))
		for _, val := range node {
			v, err := AsInterface(val)
			if err != nil {
				return nil, err
			}
			vals = append(vals, v)
		}
		return vals, nil
	case ValArg:
		return string(node), nil
	case ListArg:
		return string(node), nil
	case StrVal:
		return sqltypes.MakeString(node), nil
	case NumVal:
		n, err := sqltypes.BuildNumeric(string(node))
		if err != nil {
			return nil, fmt.Errorf("type mismatch: %s", err)
		}
		return n, nil
	case *NullVal:
		return nil, nil
	}
	return nil, fmt.Errorf("unexpected node %v", node)
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
