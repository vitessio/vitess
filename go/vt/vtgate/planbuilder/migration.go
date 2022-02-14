package planbuilder

import (
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func buildAlterMigrationPlan(query string, vschema plancontext.VSchema, enableOnlineDDL bool) (engine.Primitive, error) {
	if !enableOnlineDDL {
		return nil, schema.ErrOnlineDDLDisabled
	}
	dest, ks, tabletType, err := vschema.TargetDestination("")
	if err != nil {
		return nil, err
	}
	if ks == nil {
		return nil, vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.NoDB, "No database selected: use keyspace<:shard><@type> or keyspace<[range]><@type> (<> are optional)")
	}

	if tabletType != topodatapb.TabletType_PRIMARY {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "ALTER VITESS_MIGRATION works only on primary tablet")
	}

	if dest == nil {
		dest = key.DestinationAllShards{}
	}

	return &engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             query,
	}, nil
}

func buildRevertMigrationPlan(query string, stmt *sqlparser.RevertMigration, vschema plancontext.VSchema, enableOnlineDDL bool) (engine.Primitive, error) {
	if !enableOnlineDDL {
		return nil, schema.ErrOnlineDDLDisabled
	}
	dest, ks, tabletType, err := vschema.TargetDestination("")
	if err != nil {
		return nil, err
	}
	if ks == nil {
		return nil, vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.NoDB, "No database selected: use keyspace<:shard><@type> or keyspace<[range]><@type> (<> are optional)")
	}

	if tabletType != topodatapb.TabletType_PRIMARY {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "REVERT VITESS_MIGRATION works only on primary tablet")
	}

	if dest == nil {
		dest = key.DestinationAllShards{}
	}

	return &engine.RevertMigration{
		Keyspace:          ks,
		TargetDestination: dest,
		Stmt:              stmt,
		Query:             query,
	}, nil
}

func buildShowMigrationLogsPlan(query string, vschema plancontext.VSchema, enableOnlineDDL bool) (engine.Primitive, error) {
	if !enableOnlineDDL {
		return nil, schema.ErrOnlineDDLDisabled
	}
	dest, ks, tabletType, err := vschema.TargetDestination("")
	if err != nil {
		return nil, err
	}
	if ks == nil {
		return nil, vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.NoDB, "No database selected: use keyspace<:shard><@type> or keyspace<[range]><@type> (<> are optional)")
	}

	if tabletType != topodatapb.TabletType_PRIMARY {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "SHOW VITESS_MIGRATION works only on primary tablet")
	}

	if dest == nil {
		dest = key.DestinationAllShards{}
	}

	return &engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             query,
	}, nil
}
