package schematools

import (
	"context"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

// GetSchema makes an RPC to get the schema from a remote tablet, after
// verifying a tablet with that alias exists in the topo.
func GetSchema(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, alias *topodatapb.TabletAlias, tables []string, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	ti, err := ts.GetTablet(ctx, alias)
	if err != nil {
		return nil, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "GetTablet(%v) failed: %v", alias, err)
	}

	sd, err := tmc.GetSchema(ctx, ti.Tablet, tables, excludeTables, includeViews)
	if err != nil {
		return nil, vterrors.Wrapf(err, "GetSchema(%v, %v, %v, %v) failed: %v", ti.Tablet, tables, excludeTables, includeViews, err)
	}

	return sd, nil
}
