package planbuilder

import (
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func buildStreamPlan(stmt *sqlparser.Stream, vschema plancontext.VSchema) (engine.Primitive, error) {
	table, _, destTabletType, dest, err := vschema.FindTable(stmt.Table)
	if err != nil {
		return nil, err
	}
	if destTabletType != topodatapb.TabletType_PRIMARY {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "stream is supported only for primary tablet type, current type: %v", destTabletType)
	}
	if dest == nil {
		dest = key.DestinationExactKeyRange{}
	}
	return &engine.MStream{
		Keyspace:          table.Keyspace,
		TargetDestination: dest,
		TableName:         table.Name.CompliantName(),
	}, nil
}
