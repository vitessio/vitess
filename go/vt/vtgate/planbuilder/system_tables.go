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
	"strings"

	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type notImplementedSchemaInfoConverter struct{}

func (f *notImplementedSchemaInfoConverter) ColumnLookup(*sqlparser.ColName) (int, error) {
	return 0, vterrors.VT12001("comparing table schema name with a column name")
}

func (f *notImplementedSchemaInfoConverter) CollationForExpr(sqlparser.Expr) collations.ID {
	return collations.Unknown
}

func (f *notImplementedSchemaInfoConverter) DefaultCollation() collations.ID {
	return collations.Default()
}

func (pb *primitiveBuilder) findSysInfoRoutingPredicates(expr sqlparser.Expr, rut *route, reservedVars *sqlparser.ReservedVars) error {
	isTableSchema, bvName, out, err := extractInfoSchemaRoutingPredicate(expr, reservedVars)
	if err != nil {
		return err
	}
	if out == nil {
		// we didn't find a predicate to use for routing, so we just exit early
		return nil
	}

	if isTableSchema {
		rut.eroute.SysTableTableSchema = append(rut.eroute.SysTableTableSchema, out)
	} else {
		if rut.eroute.SysTableTableName == nil {
			rut.eroute.SysTableTableName = map[string]evalengine.Expr{}
		}
		rut.eroute.SysTableTableName[bvName] = out
	}

	return nil
}

func findOtherComparator(cmp *sqlparser.ComparisonExpr) (bool, sqlparser.Expr, sqlparser.Expr, func(arg sqlparser.Argument)) {
	if schema, table := isTableSchemaOrName(cmp.Left); schema || table {
		return schema, cmp.Left, cmp.Right, func(arg sqlparser.Argument) {
			cmp.Right = arg
		}
	}
	if schema, table := isTableSchemaOrName(cmp.Right); schema || table {
		return schema, cmp.Right, cmp.Left, func(arg sqlparser.Argument) {
			cmp.Left = arg
		}
	}

	return false, nil, nil, nil
}

func isTableSchemaOrName(e sqlparser.Expr) (isTableSchema bool, isTableName bool) {
	col, ok := e.(*sqlparser.ColName)
	if !ok {
		return false, false
	}
	return isDbNameCol(col), isTableNameCol(col)
}

var schemaColumns = map[string]any{
	"table_schema":            nil,
	"constraint_schema":       nil,
	"schema_name":             nil,
	"routine_schema":          nil,
	"specific_schema":         nil,
	"event_schema":            nil,
	"referenced_table_schema": nil,
	"index_schema":            nil,
	"trigger_schema":          nil,
	"event_object_schema":     nil,
}

func isDbNameCol(col *sqlparser.ColName) bool {
	_, found := schemaColumns[col.Name.Lowered()]
	return found
}

func isTableNameCol(col *sqlparser.ColName) bool {
	return col.Name.EqualString("table_name") || col.Name.EqualString("referenced_table_name")
}

func extractInfoSchemaRoutingPredicate(in sqlparser.Expr, reservedVars *sqlparser.ReservedVars) (bool, string, evalengine.Expr, error) {
	switch cmp := in.(type) {
	case *sqlparser.ComparisonExpr:
		if cmp.Operator == sqlparser.EqualOp {
			isSchemaName, col, other, replaceOther := findOtherComparator(cmp)
			if col != nil && shouldRewrite(other) {
				evalExpr, err := evalengine.Translate(other, &notImplementedSchemaInfoConverter{})
				if err != nil {
					if strings.Contains(err.Error(), evalengine.ErrTranslateExprNotSupported) {
						// This just means we can't rewrite this particular expression,
						// not that we have to exit altogether
						return false, "", nil, nil
					}
					return false, "", nil, err
				}
				var name string
				if isSchemaName {
					name = sqltypes.BvSchemaName
				} else {
					name = reservedVars.ReserveColName(col.(*sqlparser.ColName))
				}
				replaceOther(sqlparser.NewArgument(name))
				return isSchemaName, name, evalExpr, nil
			}
		}
	}
	return false, "", nil, nil
}

func shouldRewrite(e sqlparser.Expr) bool {
	switch node := e.(type) {
	case *sqlparser.FuncExpr:
		// we should not rewrite database() calls against information_schema
		return !(node.Name.EqualString("database") || node.Name.EqualString("schema"))
	}
	return true
}
