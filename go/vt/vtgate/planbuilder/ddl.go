package planbuilder

import (
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// Error messages for CreateView queries
const (
	CreateViewDifferentKeyspace string = "Select query does not belong to the same keyspace as the view statement"
	CreateViewComplex           string = "Complex select queries are not supported in create view statements"
)

// buildGeneralDDLPlan builds a general DDL plan, which can be either normal DDL or online DDL.
// The two behave compeltely differently, and have two very different primitives.
// We want to be able to dynamically choose between normal/online plans according to Session settings.
// However, due to caching of plans, we're unable to make that choice right now. In this function we don't have
// a session context. It's only when we Execute() the primitive that we have that context.
// This is why we return a compound primitive (DDL) which contains fully populated primitives (Send & OnlineDDL),
// and which chooses which of the two to invoke at runtime.
func buildGeneralDDLPlan(sql string, ddlStatement sqlparser.DDLStatement, vschema ContextVSchema) (engine.Primitive, error) {
	normalDDLPlan, onlineDDLPlan, err := buildDDLPlans(sql, ddlStatement, vschema)
	if err != nil {
		return nil, err
	}

	return &engine.DDL{
		Keyspace:  normalDDLPlan.Keyspace,
		SQL:       normalDDLPlan.Query,
		DDL:       ddlStatement,
		NormalDDL: normalDDLPlan,
		OnlineDDL: onlineDDLPlan,
	}, nil
}

func buildDDLPlans(sql string, ddlStatement sqlparser.DDLStatement, vschema ContextVSchema) (*engine.Send, *engine.OnlineDDL, error) {
	var table *vindexes.Table
	var destination key.Destination
	var keyspace *vindexes.Keyspace
	var err error

	switch ddl := ddlStatement.(type) {
	case *sqlparser.CreateIndex:
		// For Create index, the table must already exist
		// We should find the target of the query from this tables location
		table, _, _, _, destination, err = vschema.FindTableOrVindex(ddlStatement.GetTable())
		keyspace = table.Keyspace
		if err != nil {
			return nil, nil, err
		}
		if table == nil {
			return nil, nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "table does not exists: %s", ddlStatement.GetTable().Name.String())
		}
		ddlStatement.SetTable("", table.Name.String())
	case *sqlparser.DDL:
		// For DDL, it is only required that the keyspace exist
		// We should remove the keyspace name from the table name, as the database name in MySQL might be different than the keyspace name
		destination, keyspace, _, err = vschema.TargetDestination(ddlStatement.GetTable().Qualifier.String())
		if err != nil {
			return nil, nil, err
		}
		ddlStatement.SetTable("", ddlStatement.GetTable().Name.String())
	case *sqlparser.CreateView:
		// For Create View, we require that the keyspace exist and the select query can be satisfied within the keyspace itself
		// We should remove the keyspace name from the table name, as the database name in MySQL might be different than the keyspace name
		destination, keyspace, _, err = vschema.TargetDestination(ddl.ViewName.Qualifier.String())
		if err != nil {
			return nil, nil, err
		}
		ddl.ViewName.Qualifier = sqlparser.NewTableIdent("")

		var selectPlan engine.Primitive
		selectPlan, err = createInstructionFor(sqlparser.String(ddl.Select), ddl.Select, vschema)
		if err != nil {
			return nil, nil, err
		}
		routePlan, isRoute := selectPlan.(*engine.Route)
		if !isRoute {
			return nil, nil, vterrors.New(vtrpc.Code_INVALID_ARGUMENT, CreateViewComplex)
		}
		if keyspace.Name != routePlan.GetKeyspaceName() {
			return nil, nil, vterrors.New(vtrpc.Code_INVALID_ARGUMENT, CreateViewDifferentKeyspace)
		}
		if routePlan.Opcode != engine.SelectUnsharded && routePlan.Opcode != engine.SelectEqualUnique && routePlan.Opcode != engine.SelectScatter {
			return nil, nil, vterrors.New(vtrpc.Code_INVALID_ARGUMENT, CreateViewComplex)
		}
		sqlparser.Rewrite(ddl.Select, func(cursor *sqlparser.Cursor) bool {
			switch tableName := cursor.Node().(type) {
			case sqlparser.TableName:
				cursor.Replace(sqlparser.TableName{
					Name: tableName.Name,
				})
			}
			return true
		}, nil)

	case *sqlparser.CreateTable:
		destination, keyspace, _, err = vschema.TargetDestination(ddlStatement.GetTable().Qualifier.String())
		// Remove the keyspace name as the database name might be different.
		ddlStatement.SetTable("", ddlStatement.GetTable().Name.String())
		if err != nil {
			return nil, nil, err
		}
	default:
		return nil, nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "BUG: unexpected statement type: %T", ddlStatement)
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
