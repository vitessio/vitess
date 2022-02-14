package planbuilder

import (
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func buildCallProcPlan(stmt *sqlparser.CallProc, vschema plancontext.VSchema) (engine.Primitive, error) {
	var ks string
	if !stmt.Name.Qualifier.IsEmpty() {
		ks = stmt.Name.Qualifier.String()
	}

	dest, keyspace, _, err := vschema.TargetDestination(ks)
	if err != nil {
		return nil, err
	}

	if dest == nil {
		if err := vschema.ErrorIfShardedF(keyspace, "CALL", errNotAllowWhenSharded); err != nil {
			return nil, err
		}
		dest = key.DestinationAnyShard{}
	}

	stmt.Name.Qualifier = sqlparser.NewTableIdent("")

	return &engine.Send{
		Keyspace:          keyspace,
		TargetDestination: dest,
		Query:             sqlparser.String(stmt),
	}, nil
}

const errNotAllowWhenSharded = "CALL is not supported for sharded database"
