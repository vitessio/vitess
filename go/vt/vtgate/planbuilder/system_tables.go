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
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func (pb *primitiveBuilder) findSysInfoRoutingPredicates(expr sqlparser.Expr, rut *route) error {
	isTableSchema, out, err := extractInfoSchemaRoutingPredicate(expr)
	if err != nil {
		return err
	}
	if out == nil {
		// we didn't find a predicate to use for routing, so we just exit early
		return nil
	}

	if isTableSchema {
		if rut.eroute.SysTableTableSchema != nil && !evalengine.AreExprEqual(rut.eroute.SysTableTableSchema, out) {
			return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "two predicates for specifying the database are not supported")
		}
		rut.eroute.SysTableTableSchema = out
	} else {
		if rut.eroute.SysTableTableName != nil && !evalengine.AreExprEqual(rut.eroute.SysTableTableName, out) {
			return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "two predicates for table_name not supported")
		}
		rut.eroute.SysTableTableName = out
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

func isDbNameCol(col *sqlparser.ColName) bool {
	return col.Name.EqualString("table_schema") || col.Name.EqualString("constraint_schema") || col.Name.EqualString("schema_name") || col.Name.EqualString("routine_schema")
}

func isTableNameCol(col *sqlparser.ColName) bool {
	return col.Name.EqualString("table_name")
}

func extractInfoSchemaRoutingPredicate(in sqlparser.Expr) (bool, evalengine.Expr, error) {
	switch cmp := in.(type) {
	case *sqlparser.ComparisonExpr:
		if cmp.Operator == sqlparser.EqualOp {
			isSchemaName, col, other, replaceOther := findOtherComparator(cmp)
			if col != nil && shouldRewrite(other) {
				evalExpr, err := sqlparser.Convert(other)
				if err != nil {
					if err == sqlparser.ErrExprNotSupported {
						// This just means we can't rewrite this particular expression,
						// not that we have to exit altogether
						return false, nil, nil
					}
					return false, nil, err
				}
				name := ":"
				if isSchemaName {
					name += sqltypes.BvSchemaName
				} else {
					name += engine.BvTableName
				}
				replaceOther(sqlparser.NewArgument(name))
				return isSchemaName, evalExpr, nil
			}
		}
	}
	return false, nil, nil
}

func shouldRewrite(e sqlparser.Expr) bool {
	switch node := e.(type) {
	case *sqlparser.FuncExpr:
		// we should not rewrite database() calls against information_schema
		return !(node.Name.EqualString("database") || node.Name.EqualString("schema"))
	}
	return true
}
