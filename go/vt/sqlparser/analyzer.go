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
func GetTableName(node SimpleTableExpr) TableIdent {
	if n, ok := node.(*TableName); ok && n.Qualifier.IsEmpty() {
		return n.Name
	}
	// sub-select or '.' expression
	return NewTableIdent("")
}

// IsColName returns true if the ValExpr is a *ColName.
func IsColName(node ValExpr) bool {
	_, ok := node.(*ColName)
	return ok
}

// IsValue returns true if the ValExpr is a string, integral or value arg.
// NULL is not considered to be a value.
func IsValue(node ValExpr) bool {
	v, ok := node.(*SQLVal)
	if !ok {
		return false
	}
	switch v.Type {
	case StrVal, HexVal, IntVal, ValArg:
		return true
	}
	return false
}

// IsNull returns true if the ValExpr is SQL NULL
func IsNull(node ValExpr) bool {
	switch node.(type) {
	case *NullVal:
		return true
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
// IntVal to sqltypes.Numeric, NullVal to nil.
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
	case *SQLVal:
		switch node.Type {
		case ValArg:
			return string(node.Val), nil
		case StrVal:
			return sqltypes.MakeString(node.Val), nil
		case HexVal:
			v, err := node.HexDecode()
			if err != nil {
				return nil, err
			}
			return sqltypes.MakeString(v), nil
		case IntVal:
			n, err := sqltypes.BuildIntegral(string(node.Val))
			if err != nil {
				return nil, fmt.Errorf("type mismatch: %s", err)
			}
			return n, nil
		}
	case ListArg:
		return string(node), nil
	case *NullVal:
		return nil, nil
	}
	return nil, fmt.Errorf("unexpected node '%v'", String(node))
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
