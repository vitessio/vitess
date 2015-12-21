// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"strings"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/trace"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// UpdateEndPoints is a high level wrapper for TopoServer.UpdateEndPoints.
// It generates trace spans.
func UpdateEndPoints(ctx context.Context, ts Server, cell, keyspace, shard string, tabletType topodatapb.TabletType, addrs *topodatapb.EndPoints, existingVersion int64) error {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UpdateEndPoints")
	span.Annotate("cell", cell)
	span.Annotate("keyspace", keyspace)
	span.Annotate("shard", shard)
	span.Annotate("tablet_type", strings.ToLower(tabletType.String()))
	defer span.Finish()

	return ts.UpdateEndPoints(ctx, cell, keyspace, shard, tabletType, addrs, existingVersion)
}
