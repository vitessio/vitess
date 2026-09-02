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
	"maps"
	"slices"
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// InfoSchemaRouting used for information_schema queries.
// They are special because we usually don't know at plan-time
// what keyspace the query go to, because we don't see normalized literal values
type InfoSchemaRouting struct {
	SysTableTableSchema []sqlparser.Expr
	SysTableTableName   map[string]sqlparser.Expr
	Table               *QueryTable

	seenPredicates []sqlparser.Expr
}

func (isr *InfoSchemaRouting) UpdateRoutingParams(ctx *plancontext.PlanningContext, rp *engine.RoutingParameters) {
	rp.SysTableTableSchema = nil
	for _, expr := range isr.SysTableTableSchema {
		eexpr, err := evalengine.Translate(expr, &evalengine.Config{
			Collation:     collations.SystemCollation.Collation,
			ResolveColumn: NotImplementedSchemaInfoResolver,
			Environment:   ctx.VSchema.Environment(),
		})
		if err != nil {
			panic(err)
		}
		rp.SysTableTableSchema = append(rp.SysTableTableSchema, eexpr)
	}

	rp.SysTableTableName = make(map[string]evalengine.Expr, len(isr.SysTableTableName))
	for k, expr := range isr.SysTableTableName {
		eexpr, err := evalengine.Translate(expr, &evalengine.Config{
			Collation:     collations.SystemCollation.Collation,
			ResolveColumn: NotImplementedSchemaInfoResolver,
			Environment:   ctx.VSchema.Environment(),
		})
		if err != nil {
			panic(err)
		}

		rp.SysTableTableName[k] = eexpr
	}
}

func (isr *InfoSchemaRouting) Clone() Routing {
	return &InfoSchemaRouting{
		SysTableTableSchema: slices.Clone(isr.SysTableTableSchema),
		SysTableTableName:   maps.Clone(isr.SysTableTableName),
		Table:               isr.Table,
	}
}

func (isr *InfoSchemaRouting) updateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Routing {
	isr.seenPredicates = append(isr.seenPredicates, expr)
	isTableSchema, bvName, out := extractInfoSchemaRoutingPredicate(ctx, expr)
	if out == nil {
		return isr
	}

	if isr.SysTableTableName == nil {
		isr.SysTableTableName = map[string]sqlparser.Expr{}
	}

	if isTableSchema {
		for _, s := range isr.SysTableTableSchema {
			if sqlparser.Equals.Expr(out, s) {
				// we already have this expression in the list
				// stating it again does not add value
				return isr
			}
		}
		isr.SysTableTableSchema = append(isr.SysTableTableSchema, out)
	} else {
		isr.SysTableTableName[bvName] = out
	}
	return isr
}

func (isr *InfoSchemaRouting) resetRoutingLogic(ctx *plancontext.PlanningContext) Routing {
	isr.SysTableTableName = make(map[string]sqlparser.Expr)
	isr.SysTableTableSchema = nil
	seen := isr.seenPredicates
	isr.seenPredicates = nil
	var routing Routing = isr
	for _, expr := range seen {
		routing = UpdateRoutingLogic(ctx, expr, routing)
	}
	return routing
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

func extractInfoSchemaRoutingPredicate(ctx *plancontext.PlanningContext, in sqlparser.Expr) (bool, string, sqlparser.Expr) {
	cmp, ok := in.(*sqlparser.ComparisonExpr)
	if !ok {
		return false, "", nil
	}
	switch cmp.Operator {
	case sqlparser.EqualOp:
	case sqlparser.InOp:
		// Only routable columns are ever touched: an IN on any other column
		// must stay exactly as written. All guards run BEFORE any mutation.
		col, isSchema, isTable := IsTableSchemaOrName(cmp.Left, ctx.VSchema.Environment().MySQLVersion())
		if col == nil || (!isSchema && !isTable) {
			return false, "", nil
		}
		translates := func(e sqlparser.Expr) bool {
			// same Config as the shared translatability check below
			_, err := evalengine.Translate(e, &evalengine.Config{
				Collation:     collations.SystemCollation.Collation,
				ResolveColumn: NotImplementedSchemaInfoResolver,
				Environment:   ctx.VSchema.Environment(),
			})
			return err == nil
		}
		switch rhs := cmp.Right.(type) {
		case sqlparser.ValTuple:
			if len(rhs) == 1 {
				// A single-element IN list is an equality: only one destination
				// is named, so the predicate routes exactly like `=` (mirrors
				// ShardedRouting.planInOp). An element the equality path would
				// refuse (e.g. database(), which must stay in the query
				// untouched) leaves the IN exactly as written.
				if !shouldRewrite(rhs[0]) || !translates(rhs[0]) {
					return false, "", nil
				}
				cmp.Operator = sqlparser.EqualOp
				cmp.Right = rhs[0]
				break // continue into the shared equality tail below
			}
			// A multi-element schema list — a literal list with normalization
			// disabled, or a prepared statement's `IN (?, ?)` — cannot name
			// one keyspace. Carry the whole tuple so routeInfoSchemaQuery's
			// cardinality guard rejects it loudly at execution instead of the
			// query silently running against the default keyspace. The
			// equality rewrite below is safe: execution always errors on the
			// tuple before the rewritten query can be sent anywhere.
			// Multi-element table_name lists are left alone: they already
			// work as pushed-down filters once the schema routes.
			if !isSchema || !translates(rhs) {
				return false, "", nil
			}
			cmp.Operator = sqlparser.EqualOp
			cmp.Right = sqlparser.NewTypedArgument(sqltypes.BvSchemaName, sqltypes.VarChar)
			return true, sqltypes.BvSchemaName, rhs
		case sqlparser.ListArg:
			// The normalizer turns `in ('x')` into a list bindvar whose length
			// is unknown until execution, so routeInfoSchemaQuery decides
			// there: for the schema column one value routes and any other
			// length errors, so the predicate is rewritten to the equality
			// form up front. For the table_name column one value contributes
			// routed-table handling while other lengths keep working as the
			// pushed-down filter, so the predicate must stay exactly as
			// written: it is keyed by the list's own bind variable name and
			// the engine rewrites that variable's value in place.
			if isSchema {
				cmp.Operator = sqlparser.EqualOp
				cmp.Right = sqlparser.NewTypedArgument(sqltypes.BvSchemaName, sqltypes.VarChar)
				return true, sqltypes.BvSchemaName, rhs
			}
			// The client's list may be shared: the normalizer reuses one list
			// bind variable for identical IN tuples across predicates, so the
			// engine must never write it. Re-point this predicate at a
			// dedicated, vtgate-owned list variable; the stored expression
			// still reads the client's list, and the engine populates the
			// dedicated one (with the routed table name when applicable).
			bvName := ctx.GetReservedArgumentFor(rhs)
			cmp.Right = sqlparser.ListArg(bvName)
			return false, bvName, rhs
		default:
			return false, "", nil
		}
	default:
		return false, "", nil
	}

	isSchemaName, col := isTableOrSchemaRoutable(cmp, ctx.VSchema.Environment().MySQLVersion())
	rhs := cmp.Right
	if col == nil || !shouldRewrite(rhs) {
		return false, "", nil
	}

	// here we are just checking if this query can be translated to an evalengine expression
	// we'll need to do this translation again later when building the engine.Route
	_, err := evalengine.Translate(rhs, &evalengine.Config{
		Collation:     collations.SystemCollation.Collation,
		ResolveColumn: NotImplementedSchemaInfoResolver,
		Environment:   ctx.VSchema.Environment(),
	})
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
		name = ctx.GetReservedArgumentFor(col)
	}
	cmp.Right = sqlparser.NewTypedArgument(name, sqltypes.VarChar)
	return isSchemaName, name, rhs
}

// isTableOrSchemaRoutable searches for a comparison where one side is a table or schema name column.
// if it finds the correct column name being used,
// it also makes sure that the LHS of the comparison contains the column, and the RHS the value sought after
func isTableOrSchemaRoutable(cmp *sqlparser.ComparisonExpr, version string) (
	isSchema bool, // tells if we are dealing with a table or a schema name comparator
	col *sqlparser.ColName, // which is the colName we are comparing against
) {
	if col, schema, table := IsTableSchemaOrName(cmp.Left, version); schema || table {
		return schema, col
	}
	if col, schema, table := IsTableSchemaOrName(cmp.Right, version); schema || table {
		// to make the rest of the code easier, we shuffle these around so the ColName is always on the LHS
		cmp.Right, cmp.Left = cmp.Left, cmp.Right
		return schema, col
	}

	return false, nil
}

func tryMergeInfoSchemaRoutings(ctx *plancontext.PlanningContext, routingA, routingB Routing, m merger, lhsRoute, rhsRoute *Route) *Route {
	// we have already checked type earlier, so this should always be safe
	isrA := routingA.(*InfoSchemaRouting)
	isrB := routingB.(*InfoSchemaRouting)
	emptyA := len(isrA.SysTableTableName) == 0 && len(isrA.SysTableTableSchema) == 0
	emptyB := len(isrB.SysTableTableName) == 0 && len(isrB.SysTableTableSchema) == 0

	switch {
	// if either side has no predicates to help us route, we can merge them
	case emptyA:
		return m.merge(ctx, lhsRoute, rhsRoute, isrB)
	case emptyB:
		return m.merge(ctx, lhsRoute, rhsRoute, isrA)

	// if we have no schema predicates on either side, we can merge if the table info is the same
	case len(isrA.SysTableTableSchema) == 0 && len(isrB.SysTableTableSchema) == 0:
		for k, expr := range isrB.SysTableTableName {
			if e, found := isrA.SysTableTableName[k]; found && !sqlparser.Equals.Expr(expr, e) {
				// schema names are the same, but we have contradicting table names, so we give up
				return nil
			}
			isrA.SysTableTableName[k] = expr
		}
		return m.merge(ctx, lhsRoute, rhsRoute, isrA)

	// if both sides have the same schema predicate, we can safely merge them
	case equalExprs(isrA.SysTableTableSchema, isrB.SysTableTableSchema):
		maps.Copy(isrA.SysTableTableName, isrB.SysTableTableName)
		return m.merge(ctx, lhsRoute, rhsRoute, isrA)

	// give up
	default:
		return nil
	}
}

func equalExprs(a, b []sqlparser.Expr) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !sqlparser.Equals.Expr(a[i], b[i]) {
			return false
		}
	}
	return true
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
		return !node.Name.EqualString("database") && !node.Name.EqualString("schema")
	}
	return true
}

func IsTableSchemaOrName(e sqlparser.Expr, version string) (col *sqlparser.ColName, isTableSchema bool, isTableName bool) {
	col, ok := e.(*sqlparser.ColName)
	if !ok {
		return nil, false, false
	}
	return col, isDbNameCol(col, version), isTableNameCol(col)
}

func isDbNameCol(col *sqlparser.ColName, version string) bool {
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
