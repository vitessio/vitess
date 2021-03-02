package vexec

import (
	"context"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Executor should be implemented by any tablet-side structs which accept VExec commands
type Executor interface {
	VExec(ctx context.Context, vx *TabletVExec) (qr *querypb.QueryResult, err error)
}
