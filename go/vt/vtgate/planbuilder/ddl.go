package planbuilder

import (
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func buildDDLPlan(sql string, in sqlparser.Statement, vschema ContextVSchema) (engine.Primitive, error) {
	stmt := in.(*sqlparser.DDL)
	// This method call will validate the destination != nil check.
	destination, keyspace, _, err := vschema.TargetDestination(stmt.Table.Qualifier.String())
	if err != nil {
		return nil, err
	}

	if destination == nil {
		destination = key.DestinationAllShards{}
	}

	return &engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             sql, //This is original sql query to be passed as the parser can provide partial ddl AST.
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
