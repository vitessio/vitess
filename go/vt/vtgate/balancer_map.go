// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/topo"
)

// BalancerMap builds and maintains a map from cell.keyspace.dbtype.shard to
// Balancers.
type BalancerMap struct {
	Toposerv  SrvTopoServer
	Cell      string
	mu        sync.Mutex
	balancers map[string]*Balancer
}

// NewBalancerMap builds a new BalancerMap. Each BalancerMap is dedicated to a
// cell. serv is the TopoServ used to fetch the list of tablets when needed.
func NewBalancerMap(serv SrvTopoServer, cell string) *BalancerMap {
	return &BalancerMap{
		Toposerv:  serv,
		Cell:      cell,
		balancers: make(map[string]*Balancer, 256),
	}
}

// Balancer creates a Balancer if one doesn't exist for a cell.keyspace.dbtype.shard.
// If one was previously created, then that is returned. The retryDelay is used only
// when a Balancer is created.
func (blm *BalancerMap) Balancer(keyspace, shard string, tabletType topo.TabletType, retryDelay time.Duration) *Balancer {
	key := fmt.Sprintf("%s.%s.%s.%s", blm.Cell, keyspace, tabletType, shard)
	blc, ok := blm.get(key)
	if ok {
		return blc
	}
	getAddresses := func() (*topo.EndPoints, error) {
		endpoints, err := blm.Toposerv.GetEndPoints(blm.Cell, keyspace, shard, tabletType)
		if err != nil {
			return nil, fmt.Errorf("endpoints fetch error: %v", err)
		}
		return endpoints, nil
	}
	return blm.set(key, NewBalancer(getAddresses, retryDelay))
}

func (blm *BalancerMap) get(key string) (blc *Balancer, ok bool) {
	blm.mu.Lock()
	blc, ok = blm.balancers[key]
	blm.mu.Unlock()
	return
}

func (blm *BalancerMap) set(key string, blc *Balancer) *Balancer {
	blm.mu.Lock()
	defer blm.mu.Unlock()
	cur, ok := blm.balancers[key]
	if ok {
		return cur
	}
	blm.balancers[key] = blc
	return blc
}
