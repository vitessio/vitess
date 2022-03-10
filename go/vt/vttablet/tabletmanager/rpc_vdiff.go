package tabletmanager

import (
	"context"

	"vitess.io/vitess/go/vt/log"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

func (tm *TabletManager) VDiff(ctx context.Context, req *tabletmanagerdatapb.VDiffRequest) (*tabletmanagerdatapb.VDiffResponse, error) {
	log.Infof("VDiff called for %+v", req)
	resp, err := tm.VDiffEngine.PerformVDiffAction(ctx, req)
	return resp, err
}
