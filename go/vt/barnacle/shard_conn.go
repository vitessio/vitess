// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package barnacle

import (
	"github.com/youtube/vitess/go/vt/client2/tablet"
	"github.com/youtube/vitess/go/vt/topo"
)

type ShardConn struct {
	address    string
	keyspace   string
	shard      string
	tabletType topo.TabletType
	balancer   *Balancer
	conn       *tablet.Conn
}

func NewShardConn(bctopo *bcTopo, keyspace, shard string, tabletType topo.TabletType) *ShardConn {
	return &ShardConn{
		keyspace:   keyspace,
		shard:      shard,
		tabletType: tabletType,
		balancer:   bctopo.Balancer(keyspace, shard, tabletType),
	}
}
