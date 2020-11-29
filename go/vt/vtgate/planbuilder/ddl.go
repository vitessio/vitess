package planbuilder

import (
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// buildGeneralDDLPlan builds a general DDL plan, which can be either normal DDL or online DDL.
// The two behave compeltely differently, and have two very different primitives.
// We want to be able to dynamically choose between normal/online plans according to Session settings.
// However, due to caching of plans, we're unable to make that choice right now. In this function we don't have
// a session context. It's only when we Execute() the primitive that we have that context.
// This is why we return a compound primitive (DDL) which contains fully populated primitives (Send & OnlineDDL),
// and which chooses which of the two to invoke at runtime.
func buildGeneralDDLPlan(sql string, ddlStatement sqlparser.DDLStatement, vschema ContextVSchema) (engine.Primitive, error) {
	normalDDLPlan, err := buildDDLPlan(sql, ddlStatement, vschema)
	if err != nil {
		return nil, err
	}
	onlineDDLPlan, err := buildOnlineDDLPlan(sql, ddlStatement, vschema)
	if err != nil {
		return nil, err
	}
	query := sql
	// If the query is fully parsed, generate the query from the ast. Otherwise, use the original query
	if ddlStatement.IsFullyParsed() {
		query = sqlparser.String(ddlStatement)
	}

	return &engine.DDL{
		Keyspace:  normalDDLPlan.Keyspace,
		SQL:       query,
		DDL:       ddlStatement,
		NormalDDL: normalDDLPlan,
		OnlineDDL: onlineDDLPlan,
	}, nil
}

func buildDDLPlan(sql string, ddlStatement sqlparser.DDLStatement, vschema ContextVSchema) (*engine.Send, error) {
	var table *vindexes.Table
	var destination key.Destination
	var keyspace *vindexes.Keyspace
	var err error

	switch ddlStatement.(type) {
	case *sqlparser.CreateIndex:
		// For Create index, the table must already exist
		// We should find the target of the query from this tables location
		table, _, _, _, destination, err = vschema.FindTableOrVindex(ddlStatement.GetTable())
		keyspace = table.Keyspace
		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "table does not exists: %s", ddlStatement.GetTable().Name.String())
		}
		ddlStatement.SetTable("", table.Name.String())
	case *sqlparser.DDL:
		destination, keyspace, _, err = vschema.TargetDestination(ddlStatement.GetTable().Qualifier.String())
		if err != nil {
			return nil, err
		}
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
	}, nil
}

func buildOnlineDDLPlan(query string, ddlStatement sqlparser.DDLStatement, vschema ContextVSchema) (*engine.OnlineDDL, error) {
	_, keyspace, _, err := vschema.TargetDestination(ddlStatement.GetTable().Qualifier.String())
	if err != nil {
		return nil, err
	}
	// strategy and options will be computed in real time, on Execute()
	return &engine.OnlineDDL{
		Keyspace: keyspace,
		DDL:      ddlStatement,
		SQL:      query,
	}, nil
}

func buildVSchemaDDLPlan(ddlStmt sqlparser.DDLStatement, vschema ContextVSchema) (engine.Primitive, error) {
	stmt, ok := ddlStmt.(*sqlparser.DDL)
	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "Incorrect type %T", ddlStmt)
	}
	_, keyspace, _, err := vschema.TargetDestination(stmt.Table.Qualifier.String())
	if err != nil {
		return nil, err
	}
	return &engine.AlterVSchema{
		Keyspace: keyspace,
		DDL:      stmt,
	}, nil
}
