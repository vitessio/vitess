package planbuilder

import (
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func buildDDLPlan(stmt *sqlparser.DDL, vschema ContextVSchema) (engine.Primitive, error) {
	query := generateQuery(stmt)
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
		Query:             query,
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
