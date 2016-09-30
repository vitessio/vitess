// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"github.com/youtube/vitess/go/vt/topotools"
	"golang.org/x/net/context"
)

// RebuildKeyspaceGraph rebuilds the serving graph data while locking out other changes.
func (wr *Wrangler) RebuildKeyspaceGraph(ctx context.Context, keyspace string, cells []string) error {
	return topotools.RebuildKeyspace(ctx, wr.logger, wr.ts, keyspace, cells)
}
