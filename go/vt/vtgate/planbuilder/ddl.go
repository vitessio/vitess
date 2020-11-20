package planbuilder

import (
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func buildDDLPlan(sql string, stmt sqlparser.DDLStatement, vschema ContextVSchema) (engine.Primitive, error) {
	var table *vindexes.Table
	var destination key.Destination
	var keyspace *vindexes.Keyspace
	var err error

	switch stmt.(type) {
	case *sqlparser.CreateIndex:
		// For Create index, the table must already exist
		// We should find the target of the query from this tables location
		table, _, _, _, destination, err = vschema.FindTableOrVindex(stmt.GetTable())
		keyspace = table.Keyspace
		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "table does not exists: %s", stmt.GetTable().Name.String())
		}
		stmt.SetTable("", table.Name.String())
	case *sqlparser.DDL, *sqlparser.CreateView:
		// For Create View, etc it is only required that the keyspace exist
		// We should remove the keyspace name from the table name, as the database name in MySQL might be different than the keyspace name
		destination, keyspace, _, err = vschema.TargetDestination(stmt.GetTable().Qualifier.String())
		if err != nil {
			return nil, err
		}
		stmt.SetTable("", stmt.GetTable().Name.String())
	}

	if destination == nil {
		destination = key.DestinationAllShards{}
	}

	query := sql
	// If the query is fully parsed, generate the query from the ast. Otherwise, use the original query
	if stmt.IsFullyParsed() {
		query = sqlparser.String(stmt)
	}

	return &engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             query,
		IsDML:             false,
		SingleShardOnly:   false,
	}, nil
}

func buildOnlineDDLPlan(query string, stmt sqlparser.DDLStatement, vschema ContextVSchema) (engine.Primitive, error) {

	_, keyspace, _, err := vschema.TargetDestination(stmt.GetTable().Qualifier.String())
	if err != nil {
		return nil, err
	}
	if stmt.GetOnlineHint() == nil {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "not an online DDL: %s", query)
	}
	switch stmt.GetOnlineHint().Strategy {
	case schema.DDLStrategyGhost, schema.DDLStrategyPTOSC: // OK, do nothing
	case schema.DDLStrategyNormal:
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "not an online DDL strategy")
	default:
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "unknown online DDL strategy: '%v'", stmt.GetOnlineHint().Strategy)
	}
	return &engine.OnlineDDL{
		Keyspace: keyspace,
		DDL:      stmt,
		SQL:      query,
		Strategy: stmt.GetOnlineHint().Strategy,
		Options:  stmt.GetOnlineHint().Options,
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
