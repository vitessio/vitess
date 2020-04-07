package planbuilder

import (
	"vitess.io/vitess/go/vt/key"
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
