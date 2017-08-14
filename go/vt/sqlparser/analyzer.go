/*
Copyright 2017 Google Inc.

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

// analyzer.go contains utility analysis functions.

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/youtube/vitess/go/sqltypes"
)

// These constants are used to identify the SQL statement type.
const (
	StmtSelect = iota
	StmtInsert
	StmtReplace
	StmtUpdate
	StmtDelete
	StmtDDL
	StmtBegin
	StmtCommit
	StmtRollback
	StmtSet
	StmtShow
	StmtUse
	StmtOther
	StmtUnknown
)

// Preview analyzes the beginning of the query using a simpler and faster
// textual comparison to identify the statement type.
func Preview(sql string) int {
	trimmed := StripLeadingComments(sql)

	firstWord := trimmed
	if end := strings.IndexFunc(trimmed, unicode.IsSpace); end != -1 {
		firstWord = trimmed[:end]
	}

	// Comparison is done in order of priority.
	loweredFirstWord := strings.ToLower(firstWord)
	switch loweredFirstWord {
	case "select":
		return StmtSelect
	case "insert":
		return StmtInsert
	case "replace":
		return StmtReplace
	case "update":
		return StmtUpdate
	case "delete":
		return StmtDelete
	}
	switch strings.ToLower(trimmed) {
	case "begin", "start transaction":
		return StmtBegin
	case "commit":
		return StmtCommit
	case "rollback":
		return StmtRollback
	}
	switch loweredFirstWord {
	case "create", "alter", "rename", "drop":
		return StmtDDL
	case "set":
		return StmtSet
	case "show":
		return StmtShow
	case "use":
		return StmtUse
	case "analyze", "describe", "desc", "explain", "repair", "optimize", "truncate":
		return StmtOther
	}
	return StmtUnknown
}

// IsDML returns true if the query is an INSERT, UPDATE or DELETE statement.
func IsDML(sql string) bool {
	switch Preview(sql) {
	case StmtInsert, StmtReplace, StmtUpdate, StmtDelete:
		return true
	}
	return false
}

// GetTableName returns the table name from the SimpleTableExpr
// only if it's a simple expression. Otherwise, it returns "".
func GetTableName(node SimpleTableExpr) TableIdent {
	if n, ok := node.(TableName); ok && n.Qualifier.IsEmpty() {
		return n.Name
	}
	// sub-select or '.' expression
	return NewTableIdent("")
}

// IsColName returns true if the Expr is a *ColName.
func IsColName(node Expr) bool {
	_, ok := node.(*ColName)
	return ok
}

// IsValue returns true if the Expr is a string, integral or value arg.
// NULL is not considered to be a value.
func IsValue(node Expr) bool {
	switch v := node.(type) {
	case *SQLVal:
		switch v.Type {
		case StrVal, HexVal, IntVal, ValArg:
			return true
		}
	}
	return false
}

// IsNull returns true if the Expr is SQL NULL
func IsNull(node Expr) bool {
	switch node.(type) {
	case *NullVal:
		return true
	}
	return false
}

// IsSimpleTuple returns true if the Expr is a ValTuple that
// contains simple values or if it's a list arg.
func IsSimpleTuple(node Expr) bool {
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

// NewPlanValue builds a sqltypes.PlanValue from an Expr.
func NewPlanValue(node Expr) (sqltypes.PlanValue, error) {
	switch node := node.(type) {
	case *SQLVal:
		switch node.Type {
		case ValArg:
			return sqltypes.PlanValue{Key: string(node.Val[1:])}, nil
		case IntVal:
			n, err := sqltypes.NewIntegral(string(node.Val))
			if err != nil {
				return sqltypes.PlanValue{}, err
			}
			return sqltypes.PlanValue{Value: n}, nil
		case StrVal:
			return sqltypes.PlanValue{Value: sqltypes.MakeTrusted(sqltypes.VarBinary, node.Val)}, nil
		case HexVal:
			v, err := node.HexDecode()
			if err != nil {
				return sqltypes.PlanValue{}, err
			}
			return sqltypes.PlanValue{Value: sqltypes.MakeTrusted(sqltypes.VarBinary, v)}, nil
		}
	case ListArg:
		return sqltypes.PlanValue{ListKey: string(node[2:])}, nil
	case ValTuple:
		pv := sqltypes.PlanValue{
			Values: make([]sqltypes.PlanValue, 0, len(node)),
		}
		for _, val := range node {
			innerpv, err := NewPlanValue(val)
			if err != nil {
				return sqltypes.PlanValue{}, err
			}
			if innerpv.ListKey != "" || innerpv.Values != nil {
				return sqltypes.PlanValue{}, errors.New("unsupported: nested lists")
			}
			pv.Values = append(pv.Values, innerpv)
		}
		return pv, nil
	case *NullVal:
		return sqltypes.PlanValue{}, nil
	}
	return sqltypes.PlanValue{}, fmt.Errorf("expression is too complex '%v'", String(node))
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

// ExtractSetValues returns a map of key-value pairs
// if the query is a SET statement. Values can be int64 or string.
// Since set variable names are case insensitive, all keys are returned
// as lower case.
func ExtractSetValues(sql string) (map[string]interface{}, error) {
	stmt, err := Parse(sql)
	if err != nil {
		return nil, err
	}
	setStmt, ok := stmt.(*Set)
	if !ok {
		return nil, fmt.Errorf("ast did not yield *sqlparser.Set: %T", stmt)
	}
	result := make(map[string]interface{})
	for _, expr := range setStmt.Exprs {
		if !expr.Name.Qualifier.IsEmpty() {
			return nil, fmt.Errorf("invalid syntax: %v", String(expr.Name))
		}
		key := expr.Name.Name.Lowered()

		switch expr := expr.Expr.(type) {
		case *SQLVal:
			switch expr.Type {
			case StrVal:
				result[key] = string(expr.Val)
			case IntVal:
				num, err := strconv.ParseInt(string(expr.Val), 0, 64)
				if err != nil {
					return nil, err
				}
				result[key] = num
			default:
				return nil, fmt.Errorf("invalid value type: %v", String(expr))
			}
		case *NullVal:
			result[key] = nil
		default:
			return nil, fmt.Errorf("invalid syntax: %s", String(expr))
		}
	}
	return result, nil
}
