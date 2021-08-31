package grpcvtctldserver

import (
	"context"
	"fmt"
	"sync"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// helper method to asynchronously diff a schema
func (s *VtctldServer) diffSchema(ctx context.Context, masterSchema *tabletmanagerdatapb.SchemaDefinition, masterTabletAlias, alias *topodatapb.TabletAlias, excludeTables []string, includeViews bool, wg *sync.WaitGroup, er concurrency.ErrorRecorder) {
	defer wg.Done()
	log.Infof("Gathering schema for %v", topoproto.TabletAliasString(alias))

	req := &vtctldata.GetTabletRequest{
		TabletAlias: alias,
	}

	getTabletResp, err := s.GetTablet(ctx, req)
	if err != nil {
		er.RecordError(fmt.Errorf("GetTablet(%v) failed: %v", alias, err))
	}

	replicaSchema, err := s.tmc.GetSchema(ctx, getTabletResp.GetTablet(), nil, excludeTables, includeViews)
	if err != nil {
		er.RecordError(fmt.Errorf("GetSchema(%v, nil, %v, %v) failed: %v", alias, excludeTables, includeViews, err))
		return
	}

	log.Infof("Diffing schema for %v", topoproto.TabletAliasString(alias))
	tmutils.DiffSchema(topoproto.TabletAliasString(masterTabletAlias), masterSchema, topoproto.TabletAliasString(alias), replicaSchema, er)
}
