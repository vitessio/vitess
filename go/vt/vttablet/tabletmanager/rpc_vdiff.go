package tabletmanager

import (
	"context"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

func (tm *TabletManager) VDiff(ctx context.Context, req *tabletmanagerdatapb.VDiffRequest) (*tabletmanagerdatapb.VDiffResponse, error) {
	resp, err := tm.VDiffEngine.PerformVDiffAction(ctx, req)
	return resp, err
}
