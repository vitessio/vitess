/*
Copyright 2023 The Vitess Authors.

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

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// InfoSchemaRouting used for information_schema queries.
// They are special because we usually don't know at plan-time
// what keyspace the query go to, because we don't see normalized literal values
type InfoSchemaRouting struct {
	SysTableTableSchema []sqlparser.Expr
	SysTableTableName   map[string]sqlparser.Expr
	Table               *QueryTable
}

func (isr *InfoSchemaRouting) UpdateRoutingParams(_ *plancontext.PlanningContext, rp *engine.RoutingParameters) error {
	rp.SysTableTableSchema = nil
	for _, expr := range isr.SysTableTableSchema {
		eexpr, err := evalengine.Translate(expr, &evalengine.Config{ResolveColumn: NotImplementedSchemaInfoResolver})
		if err != nil {
			return err
		}
		rp.SysTableTableSchema = append(rp.SysTableTableSchema, eexpr)
	}

	rp.SysTableTableName = make(map[string]evalengine.Expr, len(isr.SysTableTableName))
	for k, expr := range isr.SysTableTableName {
		eexpr, err := evalengine.Translate(expr, &evalengine.Config{ResolveColumn: NotImplementedSchemaInfoResolver})
		if err != nil {
			return err
		}

		rp.SysTableTableName[k] = eexpr
	}
	return nil
}

func (isr *InfoSchemaRouting) Clone() Routing {
	return &InfoSchemaRouting{
		SysTableTableSchema: slices.Clone(isr.SysTableTableSchema),
		SysTableTableName:   maps.Clone(isr.SysTableTableName),
		Table:               isr.Table,
	}
}

func (isr *InfoSchemaRouting) updateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error) {
	isTableSchema, bvName, out := extractInfoSchemaRoutingPredicate(expr, ctx.ReservedVars)
	if out == nil {
		return isr, nil
	}

	if isr.SysTableTableName == nil {
		isr.SysTableTableName = map[string]sqlparser.Expr{}
	}

	if isTableSchema {
		for _, s := range isr.SysTableTableSchema {
			if sqlparser.Equals.Expr(out, s) {
				// we already have this expression in the list
				// stating it again does not add value
				return isr, nil
			}
		}
		isr.SysTableTableSchema = append(isr.SysTableTableSchema, out)
	} else {
		isr.SysTableTableName[bvName] = out
	}
	return isr, nil
}

func (isr *InfoSchemaRouting) Cost() int {
	return 0
}

func (isr *InfoSchemaRouting) OpCode() engine.Opcode {
	return engine.DBA
}

func (isr *InfoSchemaRouting) Keyspace() *vindexes.Keyspace {
	// TODO: for some info schema queries, we do know which keyspace it will go to
	// if we had this information, more routes could be merged.
	return nil
}

func extractInfoSchemaRoutingPredicate(in sqlparser.Expr, reservedVars *sqlparser.ReservedVars) (bool, string, sqlparser.Expr) {
	cmp, ok := in.(*sqlparser.ComparisonExpr)
	if !ok || cmp.Operator != sqlparser.EqualOp {
		return false, "", nil
	}

	isSchemaName, col := isTableOrSchemaRoutable(cmp)
	rhs := cmp.Right
	if col == nil || !shouldRewrite(rhs) {
		return false, "", nil
	}

	// here we are just checking if this query can be translated to an evalengine expression
	// we'll need to do this translation again later when building the engine.Route
	_, err := evalengine.Translate(rhs, &evalengine.Config{ResolveColumn: NotImplementedSchemaInfoResolver})
	if err != nil {
		// if we can't translate this to an evalengine expression,
		// we are not going to be able to route based on this expression,
		// and might as well move on
		return false, "", nil
	}
	var name string
	if isSchemaName {
		name = sqltypes.BvSchemaName
	} else {
		name = reservedVars.ReserveColName(col)
	}
	cmp.Right = sqlparser.NewTypedArgument(name, sqltypes.VarChar)
	return isSchemaName, name, rhs
}

// isTableOrSchemaRoutable searches for a comparison where one side is a table or schema name column.
// if it finds the correct column name being used,
// it also makes sure that the LHS of the comparison contains the column, and the RHS the value sought after
func isTableOrSchemaRoutable(cmp *sqlparser.ComparisonExpr) (
	isSchema bool, // tells if we are dealing with a table or a schema name comparator
	col *sqlparser.ColName, // which is the colName we are comparing against
) {
	if col, schema, table := IsTableSchemaOrName(cmp.Left); schema || table {
		return schema, col
	}
	if col, schema, table := IsTableSchemaOrName(cmp.Right); schema || table {
		// to make the rest of the code easier, we shuffle these around so the ColName is always on the LHS
		cmp.Right, cmp.Left = cmp.Left, cmp.Right
		return schema, col
	}

	return false, nil
}

func tryMergeInfoSchemaRoutings(routingA, routingB Routing, m merger, lhsRoute, rhsRoute *Route) (ops.Operator, error) {
	// we have already checked type earlier, so this should always be safe
	isrA := routingA.(*InfoSchemaRouting)
	isrB := routingB.(*InfoSchemaRouting)
	emptyA := len(isrA.SysTableTableName) == 0 && len(isrA.SysTableTableSchema) == 0
	emptyB := len(isrB.SysTableTableName) == 0 && len(isrB.SysTableTableSchema) == 0

	switch {
	// if either side has no predicates to help us route, we can merge them
	case emptyA:
		return m.merge(lhsRoute, rhsRoute, isrB)
	case emptyB:
		return m.merge(lhsRoute, rhsRoute, isrA)

	// if we have no schema predicates on either side, we can merge if the table info is the same
	case len(isrA.SysTableTableSchema) == 0 && len(isrB.SysTableTableSchema) == 0:
		for k, expr := range isrB.SysTableTableName {
			if e, found := isrA.SysTableTableName[k]; found && !sqlparser.Equals.Expr(expr, e) {
				// schema names are the same, but we have contradicting table names, so we give up
				return nil, nil
			}
			isrA.SysTableTableName[k] = expr
		}
		return m.merge(lhsRoute, rhsRoute, isrA)

	// if both sides have the same schema predicate, we can safely merge them
	case sqlparser.Equals.Exprs(isrA.SysTableTableSchema, isrB.SysTableTableSchema):
		for k, expr := range isrB.SysTableTableName {
			isrA.SysTableTableName[k] = expr
		}
		return m.merge(lhsRoute, rhsRoute, isrA)

	// give up
	default:
		return nil, nil
	}

}

var (
	// these are filled in by the init() function below
	schemaColumns57 = map[string]any{}
	schemaColumns80 = map[string]any{}

	schemaColName57 = map[string][]string{
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
		"SCHEMATA":                {"SCHEMA_NAME"},
		"TABLE_CONSTRAINTS":       {"TABLE_SCHEMA", "CONSTRAINT_SCHEMA"},
		"TABLE_PRIVILEGES":        {"TABLE_SCHEMA"},
		"TABLES":                  {"TABLE_SCHEMA"},
		"TRIGGERS":                {"TRIGGER_SCHEMA", "EVENT_OBJECT_SCHEMA"},
		"VIEW":                    {"TRIGGER_SCHEMA"},
	}
	schemaColName80 = map[string][]string{
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
)

func init() {
	for _, cols := range schemaColName57 {
		for _, col := range cols {
			schemaColumns57[strings.ToLower(col)] = nil
		}
	}
	for _, cols := range schemaColName80 {
		for _, col := range cols {
			schemaColumns80[strings.ToLower(col)] = nil
		}
	}
}

func shouldRewrite(e sqlparser.Expr) bool {
	switch node := e.(type) {
	case *sqlparser.FuncExpr:
		// we should not rewrite database() calls against information_schema
		return !(node.Name.EqualString("database") || node.Name.EqualString("schema"))
	}
	return true
}

func IsTableSchemaOrName(e sqlparser.Expr) (col *sqlparser.ColName, isTableSchema bool, isTableName bool) {
	col, ok := e.(*sqlparser.ColName)
	if !ok {
		return nil, false, false
	}
	return col, isDbNameCol(col), isTableNameCol(col)
}

func isDbNameCol(col *sqlparser.ColName) bool {
	version := servenv.MySQLServerVersion()
	var schemaColumns map[string]any
	if strings.HasPrefix(version, "5.7") {
		schemaColumns = schemaColumns57
	} else {
		schemaColumns = schemaColumns80
	}

	_, found := schemaColumns[col.Name.Lowered()]
	return found
}

func isTableNameCol(col *sqlparser.ColName) bool {
	return col.Name.EqualString("table_name") || col.Name.EqualString("referenced_table_name")
}

func NotImplementedSchemaInfoResolver(*sqlparser.ColName) (int, error) {
	return 0, vterrors.VT12001("comparing table schema name with a column name")
}
