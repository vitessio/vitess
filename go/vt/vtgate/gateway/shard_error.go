// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gateway

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vterrors"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// NewShardError returns a new error with the shard info amended.
func NewShardError(in error, target *querypb.Target, tablet *topodatapb.Tablet, inTransaction bool) error {
	if in == nil {
		return nil
	}
	if tablet != nil {
		return vterrors.WithPrefix(fmt.Sprintf("target: %s.%s.%s, used tablet: (%+v), ", target.Keyspace, target.Shard, topoproto.TabletTypeLString(target.TabletType), tablet), in)
	}
	return vterrors.WithPrefix(fmt.Sprintf("target: %s.%s.%s, ", target.Keyspace, target.Shard, topoproto.TabletTypeLString(target.TabletType)), in)
}
