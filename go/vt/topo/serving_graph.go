// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/trace"
)

// UpdateEndPoints is a high level wrapper for TopoServer.UpdateEndPoints.
// It generates trace spans.
func UpdateEndPoints(ctx context.Context, ts Server, cell, keyspace, shard string, tabletType TabletType, addrs *EndPoints) error {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UpdateEndPoints")
	span.Annotate("cell", cell)
	span.Annotate("keyspace", keyspace)
	span.Annotate("shard", shard)
	span.Annotate("tablet_type", string(tabletType))
	defer span.Finish()

	return ts.UpdateEndPoints(cell, keyspace, shard, tabletType, addrs)
}
