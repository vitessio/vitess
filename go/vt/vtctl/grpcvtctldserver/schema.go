package grpcvtctldserver

import (
	"context"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
)

// helper method to asynchronously diff a schema
func (s *VtctldServer) diffSchema(ctx context.Context, masterSchema *tabletmanagerdatapb.SchemaDefinition, masterTabletAlias, alias *topodatapb.TabletAlias, excludeTables []string, includeViews bool) error {
	var rec concurrency.AllErrorRecorder

	log.Infof("Gathering schema for %v", topoproto.TabletAliasString(alias))

	req := &vtctldata.GetTabletRequest{
		TabletAlias: alias,
	}

	getTabletResp, err := s.GetTablet(ctx, req)
	if err != nil {
		return vterrors.Wrapf(err, "GetTablet(%v) failed: %v", alias, err)
	}

	replicaSchema, err := s.tmc.GetSchema(ctx, getTabletResp.GetTablet(), nil, excludeTables, includeViews)
	if err != nil {
		return vterrors.Wrapf(err, "GetSchema(%v, nil, %v, %v) failed: %v", alias, excludeTables, includeViews, err)
	}

	log.Infof("Diffing schema for %v", topoproto.TabletAliasString(alias))
	tmutils.DiffSchema(topoproto.TabletAliasString(masterTabletAlias), masterSchema, topoproto.TabletAliasString(alias), replicaSchema, &rec)

	return rec.Error()
}
