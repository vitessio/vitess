package planbuilder

import (
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	topoprotopb "vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func buildDDLPlan(stmt *sqlparser.DDL, vschema ContextVSchema) (engine.Primitive, error) {
	query := generateQuery(stmt)
	switch stmt.Action {
	case sqlparser.CreateStr:
	default:
		return nil, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "unable to generate plan for: %s", query)
	}
	keyspace := stmt.Table.Qualifier.String()
	if keyspace == "" && vschema.TargetString() == "" {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "keyspace not specified for: %s", query)
	}
	var dest key.Destination
	if keyspace == "" {
		var err error
		keyspace, _, dest, err = topoprotopb.ParseDestination(vschema.TargetString(), topodatapb.TabletType_MASTER)
		if err != nil {
			return nil, err
		}
	}
	if dest == nil {
		dest = key.DestinationAllShards{}
	}

	return &engine.DDL{
		Keyspace:          keyspace,
		TargetDestination: dest,
		Query:             query,
	}, nil
}
