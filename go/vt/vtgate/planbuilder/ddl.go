package planbuilder

import (
	"context"
	"errors"
	"fmt"

	"vitess.io/vitess/go/vt/key"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/dynamicconfig"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// Error messages for CreateView queries
const (
	ViewDifferentKeyspace string = "Select query does not belong to the same keyspace as the view statement"
	ViewComplex           string = "Complex select queries are not supported in create or alter view statements"
	DifferentDestinations string = "Tables or Views specified in the query do not belong to the same destination"
)

type fkContraint struct {
	found bool
}

func (fk *fkContraint) FkWalk(node sqlparser.SQLNode) (kontinue bool, err error) {
	switch node.(type) {
	case *sqlparser.CreateTable, *sqlparser.AlterTable,
		*sqlparser.TableSpec, *sqlparser.AddConstraintDefinition, *sqlparser.ConstraintDefinition:
		return true, nil
	case *sqlparser.ForeignKeyDefinition:
		fk.found = true
	}
	return false, nil
}

// buildGeneralDDLPlan builds a general DDL plan, which can be either normal DDL or online DDL.
// The two behave completely differently, and have two very different primitives.
// We want to be able to dynamically choose between normal/online plans according to Session settings.
// However, due to caching of plans, we're unable to make that choice right now. In this function we don't have
// a session context. It's only when we Execute() the primitive that we have that context.
// This is why we return a compound primitive (DDL) which contains fully populated primitives (Send & OnlineDDL),
// and which chooses which of the two to invoke at runtime.
func buildGeneralDDLPlan(ctx context.Context, sql string, ddlStatement sqlparser.DDLStatement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, cfg dynamicconfig.DDL) (*planResult, error) {
	if vschema.ShardDestination() != nil {
		return buildByPassPlan(sql, vschema, true)
	}
	normalDDLPlan, onlineDDLPlan, err := buildDDLPlans(ctx, sql, ddlStatement, reservedVars, vschema, cfg)
	if err != nil {
		return nil, err
	}

	if ddlStatement.IsTemporary() {
		err := vschema.ErrorIfShardedF(normalDDLPlan.Keyspace, "temporary table", "Temporary table not supported in sharded database %s", normalDDLPlan.Keyspace.Name)
		if err != nil {
			return nil, err
		}
		onlineDDLPlan = nil // emptying this so it does not accidentally gets used somewhere
	}

	eddl := &engine.DDL{
		Keyspace:        normalDDLPlan.Keyspace,
		SQL:             normalDDLPlan.Query,
		DDL:             ddlStatement,
		NormalDDL:       normalDDLPlan,
		OnlineDDL:       onlineDDLPlan,
		Config:          cfg,
		CreateTempTable: ddlStatement.IsTemporary(),
	}
	tc := &tableCollector{}
	for _, tbl := range ddlStatement.AffectedTables() {
		tc.addASTTable(normalDDLPlan.Keyspace.Name, tbl)
	}

	return newPlanResult(eddl, tc.getTables()...), nil
}

func buildByPassPlan(sql string, vschema plancontext.VSchema, isDDL bool) (*planResult, error) {
	keyspace, err := vschema.SelectedKeyspace()
	if err != nil {
		return nil, err
	}
	send := &engine.Send{
		Keyspace:          keyspace,
		TargetDestination: vschema.ShardDestination(),
		Query:             sql,
		IsDDL:             isDDL,
	}
	return newPlanResult(send), nil
}

func buildDDLPlans(ctx context.Context, sql string, ddlStatement sqlparser.DDLStatement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, cfg dynamicconfig.DDL) (*engine.Send, *engine.OnlineDDL, error) {
	var destination key.ShardDestination
	var keyspace *vindexes.Keyspace
	var err error

	switch ddl := ddlStatement.(type) {
	case *sqlparser.AlterTable, *sqlparser.CreateTable, *sqlparser.TruncateTable:
		// For ALTER TABLE and TRUNCATE TABLE, the table must already exist
		//
		// For CREATE TABLE, the table may (in the case of --declarative)
		// already exist.
		//
		// We should find the target of the query from this tables location.
		destination, keyspace, err = findTableDestinationAndKeyspace(vschema, ddlStatement)
		if err != nil {
			return nil, nil, err
		}
		// Remove keyspace qualifiers from all table references (including foreign key references).
		sqlparser.RemoveSpecificKeyspace(ddlStatement, keyspace.Name)
		err = checkFKError(vschema, ddlStatement, keyspace)
	case *sqlparser.CreateView:
		destination, keyspace, err = buildCreateViewCommon(ctx, vschema, reservedVars, cfg, ddl.Select, ddl)
	case *sqlparser.AlterView:
		destination, keyspace, err = buildCreateViewCommon(ctx, vschema, reservedVars, cfg, ddl.Select, ddl)
	case *sqlparser.DropView:
		destination, keyspace, err = buildDropView(vschema, ddlStatement)
	case *sqlparser.DropTable:
		destination, keyspace, err = buildDropTable(vschema, ddlStatement)
	case *sqlparser.RenameTable:
		destination, keyspace, err = buildRenameTable(vschema, ddl)
	case *sqlparser.CreateProcedure:
		destination, keyspace, err = buildCreateProcedurePlan(vschema, ddl)
	case *sqlparser.DropProcedure:
		destination, keyspace, err = buildDropProcedurePlan(vschema, ddl)
	default:
		return nil, nil, vterrors.VT13001(fmt.Sprintf("unexpected DDL statement type: %T", ddlStatement))
	}

	if err != nil {
		return nil, nil, err
	}

	if destination == nil {
		destination = key.DestinationAllShards{}
	}

	query := sql
	// If the query is fully parsed, generate the query from the ast. Otherwise, use the original query
	if ddlStatement.IsFullyParsed() {
		query = sqlparser.String(ddlStatement)
	}

	return &engine.Send{
			Keyspace:          keyspace,
			TargetDestination: destination,
			Query:             query,
		}, &engine.OnlineDDL{
			Keyspace:          keyspace,
			TargetDestination: destination,
			DDL:               ddlStatement,
			SQL:               query,
		}, nil
}

func buildDropProcedurePlan(vschema plancontext.VSchema, dp *sqlparser.DropProcedure) (key.ShardDestination, *vindexes.Keyspace, error) {
	destination, keyspace, _, err := vschema.TargetDestination(dp.Name.Qualifier.String())
	if err != nil {
		return nil, nil, err
	}
	// Clear out the qualifier from the table name.
	dp.SetTable("", dp.Name.Name.String())
	return destination, keyspace, nil
}

func buildCreateProcedurePlan(vschema plancontext.VSchema, cp *sqlparser.CreateProcedure) (key.ShardDestination, *vindexes.Keyspace, error) {
	destination, keyspace, _, err := vschema.TargetDestination(cp.Name.Qualifier.String())
	if err != nil {
		return nil, nil, err
	}
	if keyspace.Sharded {
		return nil, nil, vterrors.VT12001("CREATE PROCEDURE is not supported on sharded keyspaces")
	}
	// Clear out the qualifier from the table name.
	cp.SetTable("", cp.Name.Name.String())
	sqlparser.RemoveSpecificKeyspace(cp, keyspace.Name)
	return destination, keyspace, nil
}

func checkFKError(vschema plancontext.VSchema, ddlStatement sqlparser.DDLStatement, keyspace *vindexes.Keyspace) error {
	fkMode, err := vschema.ForeignKeyMode(keyspace.Name)
	if err != nil {
		return err
	}
	if fkMode == vschemapb.Keyspace_disallow {
		fk := &fkContraint{}
		_ = sqlparser.Walk(fk.FkWalk, ddlStatement)
		if fk.found {
			return vterrors.VT10001()
		}
	}
	return nil
}

func findTableDestinationAndKeyspace(vschema plancontext.VSchema, ddlStatement sqlparser.DDLStatement) (key.ShardDestination, *vindexes.Keyspace, error) {
	var table *vindexes.BaseTable
	var destination key.ShardDestination
	var keyspace *vindexes.Keyspace
	var err error
	table, _, _, _, destination, err = vschema.FindTableOrVindex(ddlStatement.GetTable())
	if err != nil {
		var notFoundError vindexes.NotFoundError
		isNotFound := errors.As(err, &notFoundError)
		if !isNotFound {
			return nil, nil, err
		}
	}
	if table == nil {
		destination, keyspace, _, err = vschema.TargetDestination(ddlStatement.GetTable().Qualifier.String())
		if err != nil {
			return nil, nil, err
		}
		ddlStatement.SetTable("", ddlStatement.GetTable().Name.String())
	} else {
		keyspace = table.Keyspace
		ddlStatement.SetTable("", table.Name.String())
	}
	return destination, keyspace, nil
}

func buildCreateViewCommon(
	ctx context.Context,
	vschema plancontext.VSchema,
	reservedVars *sqlparser.ReservedVars,
	cfg dynamicconfig.DDL,
	sel sqlparser.TableStatement,
	ddl sqlparser.DDLStatement,
) (key.ShardDestination, *vindexes.Keyspace, error) {
	if vschema.IsViewsEnabled() {
		return createViewEnabled(vschema, reservedVars, sel, ddl)
	}

	// For Create View, we require that the keyspace exist and the select query can be satisfied within the keyspace itself
	// We should remove the keyspace name from the table name, as the database name in MySQL might be different than the keyspace name
	destination, keyspace, err := findTableDestinationAndKeyspace(vschema, ddl)
	if err != nil {
		return nil, nil, err
	}

	// views definition with `select *` should not be expanded as schema tracker might not be up-to-date
	// We copy the expressions and restore them after the planning context is created
	var expressions []*sqlparser.SelectExprs
	_ = sqlparser.VisitAllSelects(sel, func(p *sqlparser.Select, idx int) error {
		expressions = append(expressions, sqlparser.Clone(p.SelectExprs))
		return nil
	})
	selectPlan, err := createInstructionFor(ctx, sqlparser.String(sel), sel, reservedVars, vschema, cfg)
	if err != nil {
		return nil, nil, err
	}

	diffKs := false
	engine.Visit(selectPlan.primitive, func(node engine.Primitive) {
		if route, ok := node.(*engine.Route); ok {
			if route.Keyspace.Name != keyspace.Name {
				diffKs = true
			}
		}
	})

	if diffKs {
		return nil, nil, vterrors.VT12001(ViewDifferentKeyspace)
	}

	// We need to restore the original select expressions
	_ = sqlparser.VisitAllSelects(sel, func(p *sqlparser.Select, idx int) error {
		p.SelectExprs = expressions[idx]
		return nil
	})

	sqlparser.RemoveKeyspace(ddl)

	isRoutePlan, opCode := tryToGetRoutePlan(selectPlan.primitive)
	if !isRoutePlan {
		return nil, nil, vterrors.VT12001(ViewComplex)
	}
	if opCode != engine.Unsharded && opCode != engine.EqualUnique && opCode != engine.Scatter {
		return nil, nil, vterrors.VT12001(ViewComplex)
	}
	return destination, keyspace, nil
}

func createViewEnabled(vschema plancontext.VSchema, reservedVars *sqlparser.ReservedVars, ddlSelect sqlparser.TableStatement, ddl sqlparser.DDLStatement) (key.ShardDestination, *vindexes.Keyspace, error) {
	// For Create View, we require that the keyspace exist and the select query can be satisfied within the keyspace itself
	// We should remove the keyspace name from the table name, as the database name in MySQL might be different than the keyspace name
	destination, keyspace, err := findTableDestinationAndKeyspace(vschema, ddl)
	if err != nil {
		return nil, nil, err
	}

	// views definition with `select *` should not be expanded as schema tracker might not be up-to-date
	// We copy the expressions and restore them after the planning context is created
	var expressions []*sqlparser.SelectExprs
	_ = sqlparser.VisitAllSelects(ddlSelect, func(p *sqlparser.Select, idx int) error {
		expressions = append(expressions, sqlparser.Clone(p.SelectExprs))
		return nil
	})

	pCtx, err := plancontext.CreatePlanningContext(ddlSelect, reservedVars, vschema, Gen4)
	if err != nil {
		return nil, nil, err
	}

	var tblKs string
	for _, tbl := range pCtx.SemTable.Tables {
		vTbl := tbl.GetVindexTable()
		if vTbl == nil {
			continue
		}
		if tblKs == "" {
			tblKs = vTbl.Keyspace.Name
		}
		if tblKs != vTbl.Keyspace.Name {
			return nil, nil, vterrors.VT12001(ViewComplex)
		}
	}

	if tblKs != keyspace.Name {
		return nil, nil, vterrors.VT12001(ViewDifferentKeyspace)
	}

	// We need to restore the original select expressions
	_ = sqlparser.VisitAllSelects(ddlSelect, func(p *sqlparser.Select, idx int) error {
		p.SelectExprs = expressions[idx]
		return nil
	})

	sqlparser.RemoveKeyspace(ddl)

	return destination, keyspace, nil
}

func buildDropView(vschema plancontext.VSchema, ddlStatement sqlparser.DDLStatement) (key.ShardDestination, *vindexes.Keyspace, error) {
	if !vschema.IsViewsEnabled() {
		return buildDropTable(vschema, ddlStatement)
	}
	var ks *vindexes.Keyspace
	viewMap := make(map[string]any)
	for _, tbl := range ddlStatement.GetFromTables() {
		ksForView, err := vschema.FindViewTarget(tbl)
		if err != nil {
			return nil, nil, err
		}
		if ksForView == nil {
			return nil, nil, vterrors.VT09005()
		}
		if ks == nil {
			ks = ksForView
		} else if ks.Name != ksForView.Name {
			return nil, nil, vterrors.VT12001("cannot drop views from multiple keyspace in a single statement")
		}
		if _, exists := viewMap[tbl.Name.String()]; exists {
			return nil, nil, vterrors.VT03013(tbl.Name.String())
		}
		viewMap[tbl.Name.String()] = nil
		tbl.Qualifier = sqlparser.NewIdentifierCS("")
	}
	return key.DestinationAllShards{}, ks, nil
}

func buildDropTable(vschema plancontext.VSchema, ddlStatement sqlparser.DDLStatement) (key.ShardDestination, *vindexes.Keyspace, error) {
	var destination key.ShardDestination
	var keyspace *vindexes.Keyspace
	for i, tab := range ddlStatement.GetFromTables() {
		var destinationTab key.ShardDestination
		var keyspaceTab *vindexes.Keyspace
		var table *vindexes.BaseTable
		var err error
		table, _, _, _, destinationTab, err = vschema.FindTableOrVindex(tab)

		if err != nil {
			var notFoundError vindexes.NotFoundError
			isNotFound := errors.As(err, &notFoundError)
			if !isNotFound {
				return nil, nil, err
			}
		}
		if table == nil {
			destinationTab, keyspaceTab, _, err = vschema.TargetDestination(tab.Qualifier.String())
			if err != nil {
				return nil, nil, err
			}
			ddlStatement.GetFromTables()[i] = sqlparser.TableName{
				Name: tab.Name,
			}
		} else {
			keyspaceTab = table.Keyspace
			ddlStatement.GetFromTables()[i] = sqlparser.TableName{
				Name: table.Name,
			}
		}

		if destination == nil && keyspace == nil {
			destination = destinationTab
			keyspace = keyspaceTab
		}
		if destination != destinationTab || keyspace != keyspaceTab {
			return nil, nil, vterrors.VT12001(DifferentDestinations)
		}
	}
	return destination, keyspace, nil
}

func buildRenameTable(vschema plancontext.VSchema, renameTable *sqlparser.RenameTable) (key.ShardDestination, *vindexes.Keyspace, error) {
	var destination key.ShardDestination
	var keyspace *vindexes.Keyspace

	for _, tabPair := range renameTable.TablePairs {
		var destinationFrom key.ShardDestination
		var keyspaceFrom *vindexes.Keyspace
		var table *vindexes.BaseTable
		var err error
		table, _, _, _, destinationFrom, err = vschema.FindTableOrVindex(tabPair.FromTable)

		if err != nil {
			var notFoundError vindexes.NotFoundError
			isNotFound := errors.As(err, &notFoundError)
			if !isNotFound {
				return nil, nil, err
			}
		}
		if table == nil {
			destinationFrom, keyspaceFrom, _, err = vschema.TargetDestination(tabPair.FromTable.Qualifier.String())
			if err != nil {
				return nil, nil, err
			}
			tabPair.FromTable = sqlparser.TableName{
				Name: tabPair.FromTable.Name,
			}
		} else {
			keyspaceFrom = table.Keyspace
			tabPair.FromTable = sqlparser.TableName{
				Name: table.Name,
			}
		}

		if tabPair.ToTable.Qualifier.String() != "" {
			_, keyspaceTo, _, err := vschema.TargetDestination(tabPair.ToTable.Qualifier.String())
			if err != nil {
				return nil, nil, err
			}
			if keyspaceTo.Name != keyspaceFrom.Name {
				return nil, nil, vterrors.VT03002(keyspaceFrom.Name, keyspaceTo.Name)
			}
			tabPair.ToTable = sqlparser.TableName{
				Name: tabPair.ToTable.Name,
			}
		}

		if destination == nil && keyspace == nil {
			destination = destinationFrom
			keyspace = keyspaceFrom
		}
		if destination != destinationFrom || keyspace != keyspaceFrom {
			return nil, nil, vterrors.VT12001(DifferentDestinations)
		}
	}
	return destination, keyspace, nil
}

func tryToGetRoutePlan(selectPlan engine.Primitive) (valid bool, opCode engine.Opcode) {
	switch plan := selectPlan.(type) {
	case *engine.Route:
		return true, plan.Opcode
	default:
		return false, engine.Opcode(0)
	}
}
