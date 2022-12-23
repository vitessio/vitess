/*
Copyright 2022 The Vitess Authors.

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

package operators

import (
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func (r *Route) findSysInfoRoutingPredicatesGen4(predicates []sqlparser.Expr, reservedVars *sqlparser.ReservedVars) error {
	for _, pred := range predicates {
		isTableSchema, bvName, out, err := extractInfoSchemaRoutingPredicate(pred, reservedVars)
		if err != nil {
			return err
		}
		if out == nil {
			// we didn't find a predicate to use for routing, continue to look for next predicate
			continue
		}

		if r.SysTableTableName == nil {
			r.SysTableTableName = map[string]evalengine.Expr{}
		}

		if isTableSchema {
			r.SysTableTableSchema = append(r.SysTableTableSchema, out)
		} else {
			r.SysTableTableName[bvName] = out
		}
	}
	return nil
}

func extractInfoSchemaRoutingPredicate(in sqlparser.Expr, reservedVars *sqlparser.ReservedVars) (bool, string, evalengine.Expr, error) {
	cmp, ok := in.(*sqlparser.ComparisonExpr)
	if !ok || cmp.Operator != sqlparser.EqualOp {
		return false, "", nil, nil
	}

	isSchemaName, col := isTableOrSchemaRouteable(cmp)
	if col == nil || !shouldRewrite(cmp.Right) {
		return false, "", nil, nil
	}

	evalExpr, err := evalengine.Translate(cmp.Right, &notImplementedSchemaInfoConverter{})
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
		name = reservedVars.ReserveColName(col)
	}
	cmp.Right = sqlparser.NewArgument(name)
	return isSchemaName, name, evalExpr, nil
}

// isTableOrSchemaRouteable searches for a comparison where one side is a table or schema name column.
// if it finds the correct column name being used,
// it also makes sure that the LHS of the comparison contains the column, and the RHS the value sought after
func isTableOrSchemaRouteable(cmp *sqlparser.ComparisonExpr) (
	isSchema bool, // tells if we are dealing with a table or a schema name comparator
	col *sqlparser.ColName, // which is the colName we are comparing against
) {
	if col, schema, table := isTableSchemaOrName(cmp.Left); schema || table {
		return schema, col
	}
	if col, schema, table := isTableSchemaOrName(cmp.Right); schema || table {
		// to make the rest of the code easier, we shuffle these around so the ColName is always on the LHS
		cmp.Right, cmp.Left = cmp.Left, cmp.Right
		return schema, col
	}

	return false, nil
}

func createInfSchemaRoute(ctx *plancontext.PlanningContext, table *QueryTable) (ops.Operator, error) {
	ks, err := ctx.VSchema.AnyKeyspace()
	if err != nil {
		return nil, err
	}
	var schemaNameExprs []evalengine.Expr
	var tableNameExprs map[string]evalengine.Expr
	for _, pred := range table.Predicates {
		isTableSchema, bvName, out, err := extractInfoSchemaRoutingPredicate(pred, ctx.ReservedVars)
		if err != nil {
			return nil, err
		}
		if out == nil {
			// we didn't find a predicate to use for routing, continue to look for next predicate
			continue
		}

		if isTableSchema {
			schemaNameExprs = append(schemaNameExprs, out)
		} else {
			if tableNameExprs == nil {
				tableNameExprs = map[string]evalengine.Expr{}
			}
			tableNameExprs[bvName] = out
		}
	}

	if len(schemaNameExprs) > 0 {
		return sendToSingleKS(table, ks, tableNameExprs, schemaNameExprs)
	}

	/*
			We don't have enough info to send the query to a single keyspace, so we have to create a UNION.
			We're basically changing a query like this:
			Original: SELECT TABLE_NAME from information_schema.tables
			Plan:
			vtgate-union:
		 		KS0: SELECT TABLE_NAME from information_schema.tables
			    KS1: SELECT TABLE_NAME from information_schema.tables WHERE TABLE_SCHEMA = :__vtschemaname
				KS2: SELECT TABLE_NAME from information_schema.tables WHERE TABLE_SCHEMA = :__vtschemaname
	*/

	nameFor := schemaColNameFor(table.Table.Name.String())
	if nameFor == "" {
		return sendToArbitraryKeyspace(ctx, table)
	}

	keyspaces, err := ctx.VSchema.AllKeyspace()
	if err != nil {
		return nil, err
	}
	var routes []ops.Operator

	for i, ks := range keyspaces {
		tbl := &Table{
			QTable: table,
			VTable: &vindexes.Table{
				Name:     table.Table.Name,
				Keyspace: ks,
			},
		}

		var route ops.Operator = &Route{
			RouteOpCode:         engine.DBA,
			Source:              tbl,
			Keyspace:            ks,
			SysTableTableName:   tableNameExprs,
			SysTableTableSchema: schemaNameExprs,
		}
		if i > 0 {
			route, err = route.AddPredicate(ctx, schemaNameComparison(nameFor))
			if err != nil {
				return nil, err
			}
		}
		routes = append(routes, route)
	}
	union := &Union{
		Sources:  routes,
		Distinct: false,
	}
	return union, nil
}

var schemaColName57 = map[string][]string{
	"COLUMN_PRIVILEGES":       {"TABLE_SCHEMA"},
	"COLUMNS":                 {"TABLE_SCHEMA"},
	"EVENTS":                  {"EVENT_SCHEMA"},
	"FILES":                   {"TABLE_SCHEMA"},
	"KEY_COLUMN_USAGE":        {"CONSTRAINT_SCHEMA", "TABLE_SCHEMA", "REFERENCED_TABLE_SCHEMA"},
	"PARAMETERS":              {"SPECIFIC_SCHEMA"},
	"PARTITIONS":              {"TABLE_SCHEMA"},
	"REFERENTIAL_CONSTRAINTS": {"CONSTRAINT_SCHEMA", "UNIQUE_CONSTRAINT_SCHEMA"},
	"ROUTINES":                {"ROUTINE_SCHEMA"},
	"SCHEMA_PRIVILEGES":       {"TABLE_SCHEMA"},
	"STATISTICS":              {"TABLE_SCHEMA"},
	"TABLE_CONSTRAINTS":       {"TABLE_SCHEMA", "CONSTRAINT_SCHEMA"},
	"TABLE_PRIVILEGES":        {"TABLE_SCHEMA"},
	"TABLES":                  {"TABLE_SCHEMA"},
	"TRIGGERS":                {"TRIGGER_SCHEMA", "EVENT_OBJECT_SCHEMA"},
	"VIEW":                    {"TRIGGER_SCHEMA"},
}

var schemaColName80 = map[string][]string{
	"CHECK_CONSTRAINTS":            {"CONSTRAINT_SCHEMA"},
	"COLUMN_PRIVILEGES":            {"TABLE_SCHEMA"},
	"COLUMN_STATISTICS":            {"SCHEMA_NAME"},
	"COLUMNS":                      {"TABLE_SCHEMA"},
	"COLUMNS_EXTENSIONS":           {"TABLE_SCHEMA"},
	"EVENTS":                       {"EVENT_SCHEMA"},
	"FILES":                        {"TABLE_SCHEMA"},
	"KEY_COLUMN_USAGE":             {"CONSTRAINT_SCHEMA", "TABLE_SCHEMA", "REFERENCED_TABLE_SCHEMA"},
	"PARAMETERS":                   {"SPECIFIC_SCHEMA"},
	"PARTITIONS":                   {"TABLE_SCHEMA"},
	"REFERENTIAL_CONSTRAINTS":      {"CONSTRAINT_SCHEMA", "UNIQUE_CONSTRAINT_SCHEMA"},
	"ROLE_COLUMN_GRANTS":           {"TABLE_SCHEMA"},
	"ROLE_ROUTINE_GRANTS":          {"SPECIFIC_SCHEMA", "ROUTINE_SCHEMA"},
	"ROLE_TABLE_GRANTS":            {"TABLE_SCHEMA"},
	"ROUTINES":                     {"ROUTINE_SCHEMA"},
	"SCHEMA_PRIVILEGES":            {"TABLE_SCHEMA"},
	"SCHEMATA":                     {"SCHEMA_NAME"},
	"SCHEMATA_EXTENSIONS":          {"SCHEMA_NAME"},
	"ST_GEOMETRY_COLUMNS":          {"TABLE_SCHEMA"},
	"STATISTICS":                   {"TABLE_SCHEMA"},
	"TABLE_CONSTRAINTS":            {"TABLE_SCHEMA", "CONSTRAINT_SCHEMA"},
	"TABLE_CONSTRAINTS_EXTENSIONS": {"CONSTRAINT_SCHEMA"},
	"TABLE_PRIVILEGES":             {"TABLE_SCHEMA"},
	"TABLES":                       {"TABLE_SCHEMA"},
	"TABLES_EXTENSIONS":            {"TABLE_SCHEMA"},
	"TRIGGERS":                     {"TRIGGER_SCHEMA", "EVENT_OBJECT_SCHEMA"},
	"VIEW_ROUTINE_USAGE":           {"TABLE_SCHEMA", "SPECIFIC_SCHEMA"},
	"VIEW_TABLE_USAGE":             {"TABLE_SCHEMA", "VIEW_SCHEMA"},
	"VIEWS":                        {"TABLE_SCHEMA"},
}

func shouldRewrite(e sqlparser.Expr) bool {
	switch node := e.(type) {
	case *sqlparser.FuncExpr:
		// we should not rewrite database() calls against information_schema
		return !(node.Name.EqualString("database") || node.Name.EqualString("schema"))
	}
	return true
}

func isTableSchemaOrName(e sqlparser.Expr) (col *sqlparser.ColName, isTableSchema bool, isTableName bool) {
	col, ok := e.(*sqlparser.ColName)
	if !ok {
		return nil, false, false
	}
	return col, isDbNameCol(col), isTableNameCol(col)
}

func isDbNameCol(col *sqlparser.ColName) bool {
	return col.Name.EqualString("table_schema") || col.Name.EqualString("constraint_schema") || col.Name.EqualString("schema_name") || col.Name.EqualString("routine_schema")
}

func isTableNameCol(col *sqlparser.ColName) bool {
	return col.Name.EqualString("table_name")
}

func schemaColNames() map[string][]string {
	version := servenv.MySQLServerVersion()
	if strings.HasPrefix(version, "5.7") {
		return schemaColName57
	}

	return schemaColName80
}

func sendToArbitraryKeyspace(ctx *plancontext.PlanningContext, table *QueryTable) (ops.Operator, error) {
	// we are querying a information_schema table that does not have any column that defines the schema.
	// The best we can do is to send it to any arbitraty keyspace.
	ks, err := ctx.VSchema.AnyKeyspace()
	if err != nil {
		return nil, err
	}

	return &Route{
		RouteOpCode: engine.DBA,
		Source: &Table{
			QTable: table,
			VTable: &vindexes.Table{
				Name:     table.Table.Name,
				Keyspace: ks,
			},
		},
		Keyspace: ks,
	}, nil
}

func sendToSingleKS(table *QueryTable, ks *vindexes.Keyspace, SysTableTableName map[string]evalengine.Expr, SysTableTableSchema []evalengine.Expr) (ops.Operator, error) {
	// if we have enough info to send the query to a single keyspace,
	// we create a single route and are done with it
	return &Route{
		RouteOpCode: engine.DBA,
		Source: &Table{
			QTable: table,
			VTable: &vindexes.Table{
				Name:     table.Table.Name,
				Keyspace: ks,
			},
		},
		Keyspace:            ks,
		SysTableTableName:   SysTableTableName,
		SysTableTableSchema: SysTableTableSchema,
	}, nil
}

func schemaColNameFor(tableName string) string {
	cols := schemaColNames()[strings.ToUpper(tableName)]
	if len(cols) > 0 {
		return cols[0]
	}
	return ""
}

func schemaNameComparison(colName string) sqlparser.Expr {
	return &sqlparser.ComparisonExpr{
		Operator: sqlparser.EqualOp,
		Left:     sqlparser.NewColName(colName),
		Right:    sqlparser.Argument(sqltypes.BvSchemaName),
	}
}

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
