package planbuilder

import (
	"fmt"

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

func buildOnlineDDLPlan(query string, stmt *sqlparser.DDL, vschema ContextVSchema) (engine.Primitive, error) {
	_, keyspace, _, err := vschema.TargetDestination(stmt.Table.Qualifier.String())
	if err != nil {
		return nil, err
	}
	if stmt.OnlineHint == nil {
		return nil, fmt.Errorf("Not an online DDL: %s", query)
	}
	switch stmt.OnlineHint.Strategy {
	case schema.DDLStrategyGhost, schema.DDLStrategyPTOSC: // OK, do nothing
	case schema.DDLStrategyNormal:
		return nil, fmt.Errorf("Not an online DDL strategy")
	default:
		return nil, fmt.Errorf("Unknown online DDL strategy: '%v'", stmt.OnlineHint.Strategy)
	}
	return &engine.OnlineDDL{
		Keyspace: keyspace,
		DDL:      stmt,
		SQL:      query,
		Strategy: stmt.OnlineHint.Strategy,
		Options:  stmt.OnlineHint.Options,
	}, nil
}

func buildVSchemaDDLPlan(stmt *sqlparser.DDL, vschema ContextVSchema) (engine.Primitive, error) {
	_, keyspace, _, err := vschema.TargetDestination(stmt.Table.Qualifier.String())
	if err != nil {
		return nil, err
	}
	return &engine.AlterVSchema{
		Keyspace: keyspace,
		DDL:      stmt,
	}, nil
}
