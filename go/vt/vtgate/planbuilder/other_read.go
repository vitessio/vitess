package planbuilder

import (
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func buildOtherReadAndAdmin(sql string, vschema ContextVSchema) (engine.Primitive, error) {
	destination, keyspace, _, err := vschema.TargetDestination("")
	if err != nil {
		return nil, err
	}

	if destination == nil {
		destination = key.DestinationAnyShard{}
	}

	return &engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             sql, //This is original sql query to be passed as the parser can provide partial ddl AST.
		IsDML:             false,
		SingleShardOnly:   true,
	}, nil
}
