package planbuilder

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// Error messages for CreateView queries
const (
	ViewDifferentKeyspace string = "Select query does not belong to the same keyspace as the view statement"
	ViewComplex           string = "Complex select queries are not supported in create or alter view statements"
	DifferentDestinations string = "Tables or Views specified in the query do not belong to the same destination"
)

// buildGeneralDDLPlan builds a general DDL plan, which can be either normal DDL or online DDL.
// The two behave compeltely differently, and have two very different primitives.
// We want to be able to dynamically choose between normal/online plans according to Session settings.
// However, due to caching of plans, we're unable to make that choice right now. In this function we don't have
// a session context. It's only when we Execute() the primitive that we have that context.
// This is why we return a compound primitive (DDL) which contains fully populated primitives (Send & OnlineDDL),
// and which chooses which of the two to invoke at runtime.
func buildGeneralDDLPlan(sql string, ddlStatement sqlparser.DDLStatement, reservedVars sqlparser.BindVars, vschema ContextVSchema) (engine.Primitive, error) {
	normalDDLPlan, onlineDDLPlan, err := buildDDLPlans(sql, ddlStatement, reservedVars, vschema)
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

	return &engine.DDL{
		Keyspace:        normalDDLPlan.Keyspace,
		SQL:             normalDDLPlan.Query,
		DDL:             ddlStatement,
		NormalDDL:       normalDDLPlan,
		OnlineDDL:       onlineDDLPlan,
		CreateTempTable: ddlStatement.IsTemporary(),
	}, nil
}

func buildDDLPlans(sql string, ddlStatement sqlparser.DDLStatement, reservedVars sqlparser.BindVars, vschema ContextVSchema) (*engine.Send, *engine.OnlineDDL, error) {
	var destination key.Destination
	var keyspace *vindexes.Keyspace
	var err error

	switch ddl := ddlStatement.(type) {
	case *sqlparser.AlterTable, *sqlparser.TruncateTable:
		// For Alter Table and other statements, the table must already exist
		// We should find the target of the query from this tables location
		destination, keyspace, err = findTableDestinationAndKeyspace(vschema, ddlStatement)
		if err != nil {
			return nil, nil, err
		}
	case *sqlparser.CreateView:
		destination, keyspace, err = buildCreateView(vschema, ddl, reservedVars)
		if err != nil {
			return nil, nil, err
		}
	case *sqlparser.AlterView:
		destination, keyspace, err = buildAlterView(vschema, ddl, reservedVars)
		if err != nil {
			return nil, nil, err
		}
	case *sqlparser.CreateTable:
		destination, keyspace, _, err = vschema.TargetDestination(ddlStatement.GetTable().Qualifier.String())
		// Remove the keyspace name as the database name might be different.
		ddlStatement.SetTable("", ddlStatement.GetTable().Name.String())
		if err != nil {
			return nil, nil, err
		}
	case *sqlparser.DropView, *sqlparser.DropTable:
		destination, keyspace, err = buildDropViewOrTable(vschema, ddlStatement)
		if err != nil {
			return nil, nil, err
		}
	case *sqlparser.RenameTable:
		destination, keyspace, err = buildRenameTable(vschema, ddl)
		if err != nil {
			return nil, nil, err
		}

	default:
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unexpected ddl statement type: %T", ddlStatement)
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
			IsDML:             false,
			SingleShardOnly:   false,
		}, &engine.OnlineDDL{
			Keyspace: keyspace,
			DDL:      ddlStatement,
			SQL:      query,
		}, nil
}

func findTableDestinationAndKeyspace(vschema ContextVSchema, ddlStatement sqlparser.DDLStatement) (key.Destination, *vindexes.Keyspace, error) {
	var table *vindexes.Table
	var destination key.Destination
	var keyspace *vindexes.Keyspace
	var err error
	table, _, _, _, destination, err = vschema.FindTableOrVindex(ddlStatement.GetTable())
	if err != nil {
		_, isNotFound := err.(vindexes.NotFoundError)
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

func buildAlterView(vschema ContextVSchema, ddl *sqlparser.AlterView, reservedVars sqlparser.BindVars) (key.Destination, *vindexes.Keyspace, error) {
	// For Alter View, we require that the view exist and the select query can be satisfied within the keyspace itself
	// We should remove the keyspace name from the table name, as the database name in MySQL might be different than the keyspace name
	destination, keyspace, err := findTableDestinationAndKeyspace(vschema, ddl)
	if err != nil {
		return nil, nil, err
	}

	var selectPlan engine.Primitive
	selectPlan, err = createInstructionFor(sqlparser.String(ddl.Select), ddl.Select, reservedVars, vschema)
	if err != nil {
		return nil, nil, err
	}
	routePlan, isRoute := selectPlan.(*engine.Route)
	if !isRoute {
		return nil, nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, ViewComplex)
	}
	if keyspace.Name != routePlan.GetKeyspaceName() {
		return nil, nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, ViewDifferentKeyspace)
	}
	if routePlan.Opcode != engine.SelectUnsharded && routePlan.Opcode != engine.SelectEqualUnique && routePlan.Opcode != engine.SelectScatter {
		return nil, nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, ViewComplex)
	}
	_ = sqlparser.Rewrite(ddl.Select, func(cursor *sqlparser.Cursor) bool {
		switch tableName := cursor.Node().(type) {
		case sqlparser.TableName:
			cursor.Replace(sqlparser.TableName{
				Name: tableName.Name,
			})
		}
		return true
	}, nil)
	return destination, keyspace, nil
}

func buildCreateView(vschema ContextVSchema, ddl *sqlparser.CreateView, reservedVars sqlparser.BindVars) (key.Destination, *vindexes.Keyspace, error) {
	// For Create View, we require that the keyspace exist and the select query can be satisfied within the keyspace itself
	// We should remove the keyspace name from the table name, as the database name in MySQL might be different than the keyspace name
	destination, keyspace, _, err := vschema.TargetDestination(ddl.ViewName.Qualifier.String())
	if err != nil {
		return nil, nil, err
	}
	ddl.ViewName.Qualifier = sqlparser.NewTableIdent("")

	var selectPlan engine.Primitive
	selectPlan, err = createInstructionFor(sqlparser.String(ddl.Select), ddl.Select, reservedVars, vschema)
	if err != nil {
		return nil, nil, err
	}
	routePlan, isRoute := selectPlan.(*engine.Route)
	if !isRoute {
		return nil, nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, ViewComplex)
	}
	if keyspace.Name != routePlan.GetKeyspaceName() {
		return nil, nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, ViewDifferentKeyspace)
	}
	if routePlan.Opcode != engine.SelectUnsharded && routePlan.Opcode != engine.SelectEqualUnique && routePlan.Opcode != engine.SelectScatter {
		return nil, nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, ViewComplex)
	}
	_ = sqlparser.Rewrite(ddl.Select, func(cursor *sqlparser.Cursor) bool {
		switch tableName := cursor.Node().(type) {
		case sqlparser.TableName:
			cursor.Replace(sqlparser.TableName{
				Name: tableName.Name,
			})
		}
		return true
	}, nil)
	return destination, keyspace, nil
}

func buildDropViewOrTable(vschema ContextVSchema, ddlStatement sqlparser.DDLStatement) (key.Destination, *vindexes.Keyspace, error) {
	var destination key.Destination
	var keyspace *vindexes.Keyspace
	for i, tab := range ddlStatement.GetFromTables() {
		var destinationTab key.Destination
		var keyspaceTab *vindexes.Keyspace
		var table *vindexes.Table
		var err error
		table, _, _, _, destinationTab, err = vschema.FindTableOrVindex(tab)

		if err != nil {
			_, isNotFound := err.(vindexes.NotFoundError)
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
			return nil, nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, DifferentDestinations)
		}
	}
	return destination, keyspace, nil
}

func buildRenameTable(vschema ContextVSchema, renameTable *sqlparser.RenameTable) (key.Destination, *vindexes.Keyspace, error) {
	var destination key.Destination
	var keyspace *vindexes.Keyspace

	for _, tabPair := range renameTable.TablePairs {
		var destinationFrom key.Destination
		var keyspaceFrom *vindexes.Keyspace
		var table *vindexes.Table
		var err error
		table, _, _, _, destinationFrom, err = vschema.FindTableOrVindex(tabPair.FromTable)

		if err != nil {
			_, isNotFound := err.(vindexes.NotFoundError)
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
				return nil, nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.ForbidSchemaChange, "Changing schema from '%s' to '%s' is not allowed", keyspaceFrom.Name, keyspaceTo.Name)
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
			return nil, nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, DifferentDestinations)
		}
	}
	return destination, keyspace, nil
}
