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
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func (pb *primitiveBuilder) findSysInfoRoutingPredicates(expr sqlparser.Expr, rut *route) error {
	tableSchemas, tableNames, err := extractInfoSchemaRoutingPredicate(expr)
	if err != nil {
		return err
	}
	if len(tableSchemas) == 0 && len(tableNames) == 0 {
		// we didn't find a predicate to use for routing, so we just exit early
		return nil
	}

	if len(tableSchemas) > 0 {
		rut.eroute.SysTableTableSchema = append(rut.eroute.SysTableTableSchema, tableSchemas...)
	}

	if len(tableNames) > 0 {
		rut.eroute.SysTableTableName = append(rut.eroute.SysTableTableName, tableNames...)
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

func extractInfoSchemaRoutingPredicate(in sqlparser.Expr) ([]evalengine.Expr, []evalengine.Expr, error) {
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
						return nil, nil, nil
					}
					return nil, nil, err
				}
				exprs := []evalengine.Expr{evalExpr}
				if isSchemaName {
					replaceOther(sqlparser.NewArgument(sqltypes.BvSchemaName))
					return exprs, nil, nil
				}
				replaceOther(sqlparser.NewArgument(engine.BvTableName))
				return nil, exprs, nil
			}
		} else if cmp.Operator == sqlparser.InOp || cmp.Operator == sqlparser.NotInOp {
			// left side has to be the column, i.e (1, 2) IN column is not allowed.
			// At least one column has to be DB name or table name.
			colNames := checkAndSplitColumns(cmp.Left)
			if colNames == nil {
				return nil, nil, nil
			}
			valTuples := splitVals(cmp.Right, len(colNames))
			// check if the val tuples format is correct.
			if valTuples == nil {
				return nil, nil, nil
			}

			sysTableSchemas := make([]evalengine.Expr, 0, len(valTuples))
			sysTableNames := make([]evalengine.Expr, 0, len(valTuples))
			for index, col := range colNames {
				isSchema, isTable := isTableSchemaOrName(col)
				var name string
				if isSchema {
					name = sqltypes.BvSchemaName
				} else if isTable {
					name = engine.BvTableName
				} else {
					// only need to rewrite the SysTable and SysSchema
					continue
				}

				for _, tuple := range valTuples {
					expr := tuple[index]
					if shouldRewrite(expr) {
						tuple[index] = sqlparser.Argument(name)
						evalExpr, err := sqlparser.Convert(expr)
						if err != nil {
							if err == sqlparser.ErrExprNotSupported {
								// This just means we can't rewrite this particular expression,
								// not that we have to exit altogether
								return nil, nil, nil
							}
							return nil, nil, err
						}
						if isSchema {
							sysTableSchemas = append(sysTableSchemas, evalExpr)
						} else if isTable {
							sysTableNames = append(sysTableNames, evalExpr)
						}
					}
				}
			}
			// construct right side, rows of tuples of __vtschemaname or database()
			cmp.Right = populateValTuple(valTuples, len(colNames))
			return sysTableSchemas, sysTableNames, nil
		}
	}
	return nil, nil, nil
}

func populateValTuple(valTuples []sqlparser.ValTuple, numOfCol int) sqlparser.ValTuple {
	var retValTuples sqlparser.ValTuple
	retValTuples = make([]sqlparser.Expr, 0, len(valTuples))
	for _, tuple := range valTuples {
		if numOfCol == 1 {
			// only one col per row, of colName type.
			retValTuples = append(retValTuples, tuple[0])
		} else {
			retValTuples = append(retValTuples, tuple)
		}
	}
	return retValTuples
}

// Convert the right side of In ops to a list of rows.
func splitVals(e sqlparser.Expr, numOfCols int) []sqlparser.ValTuple {
	// could either be (1, 2, 3) or ((1,2), (3,5))
	expressions, ok := e.(sqlparser.ValTuple)
	if !ok {
		log.Errorf("Unsupported type, expecting val tuple %v", e)
		return nil
	}
	valTuples := make([]sqlparser.ValTuple, 0, len(expressions))

	for _, tuple := range expressions {
		if numOfCols == 1 {
			// values could be literal, float or other types.
			valTuple := []sqlparser.Expr{tuple}
			valTuples = append(valTuples, valTuple)
		} else {
			valTuple, ok := tuple.(sqlparser.ValTuple)
			if !ok {
				log.Errorf("Unsupported type, expecting a list of val tuple %v", tuple)
				return nil
			}
			valTuples = append(valTuples, valTuple)
		}

	}
	return valTuples
}

// Convert the left side of In ops to a list of columns.
func checkAndSplitColumns(e sqlparser.Expr) []sqlparser.Expr {
	colNames := make([]sqlparser.Expr, 0)
	switch cols := e.(type) {
	case sqlparser.ValTuple:
		colNames = cols
	case *sqlparser.ColName:
		colNames = append(colNames, cols)
	default:
		// unexpected left side of the in ops.
		return nil
	}
	containSystemTable := false
	for _, col := range colNames {
		containsDB, containsTable := isTableSchemaOrName(col)
		if containsDB || containsTable {
			containSystemTable = true
			break
		}
	}
	if !containSystemTable {
		log.Infof("left side of (not) in operator don't have a DB name or table name, don't need to rewrite. ")
		return nil
	}
	return colNames
}

func shouldRewrite(e sqlparser.Expr) bool {
	switch node := e.(type) {
	case *sqlparser.FuncExpr:
		// we should not rewrite database() calls against information_schema
		return !(node.Name.EqualString("database") || node.Name.EqualString("schema"))
	}
	return true
}
