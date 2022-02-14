package planbuilder

import (
	"vitess.io/vitess/go/vt/key"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func buildPlanForBypass(stmt sqlparser.Statement, _ *sqlparser.ReservedVars, vschema plancontext.VSchema) (engine.Primitive, error) {
	switch vschema.Destination().(type) {
	case key.DestinationExactKeyRange:
		if _, ok := stmt.(*sqlparser.Insert); ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "INSERT not supported when targeting a key range: %s", vschema.TargetString())
		}
	}

	keyspace, err := vschema.DefaultKeyspace()
	if err != nil {
		return nil, err
	}
	return &engine.Send{
		Keyspace:             keyspace,
		TargetDestination:    vschema.Destination(),
		Query:                sqlparser.String(stmt),
		IsDML:                sqlparser.IsDMLStatement(stmt),
		SingleShardOnly:      false,
		MultishardAutocommit: sqlparser.MultiShardAutocommitDirective(stmt),
	}, nil
}
