// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package barnacle

import (
	"fmt"
	"time"

	"github.com/youtube/vitess/go/vt/client2/tablet"
	"github.com/youtube/vitess/go/vt/topo"
)

type ShardConn struct {
	address    string
	keyspace   string
	shard      string
	tabletType topo.TabletType
	retryCount int
	balancer   *Balancer
	conn       *tablet.Conn
}

func NewShardConn(bctopo *bcTopo, keyspace, shard string, tabletType topo.TabletType, retryDelay time.Duration, retryCount int) *ShardConn {
	return &ShardConn{
		keyspace:   keyspace,
		shard:      shard,
		tabletType: tabletType,
		retryCount: retryCount,
		balancer:   bctopo.Balancer(keyspace, shard, tabletType, retryDelay),
	}
}

func (sdc *ShardConn) connect() error {
	var lastError error
	for i := 0; i < sdc.retryCount; i++ {
		addr, err := sdc.balancer.Get()
		if err != nil {
			return err
		}
		dbi := fmt.Sprintf("%s/%s/%s", addr, sdc.keyspace, sdc.shard)
		conn, err := tablet.DialTablet(dbi, false)
		if err != nil {
			lastError = err
			sdc.balancer.MarkDown(addr)
			continue
		}
		sdc.address = addr
		sdc.conn = conn
		return nil
	}
	return fmt.Errorf("could not obtain connection to %s.%s.%s, last error: %v", sdc.keyspace, sdc.shard, sdc.tabletType, lastError)
}
