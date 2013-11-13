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
	PortName  string
	mu        sync.Mutex
	balancers map[string]*Balancer
}

// NewBalancerMap builds a new BalancerMap. Each BalancerMap is dedicated to a
// cell. serv is the TopoServ used to fetch the list of tablets when needed.
// The Balancers will be built using the namedPort from each tablet info.
func NewBalancerMap(serv SrvTopoServer, cell, namedPort string) *BalancerMap {
	return &BalancerMap{
		Toposerv:  serv,
		Cell:      cell,
		PortName:  namedPort,
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
	getAddresses := func() ([]string, error) {
		endpoints, err := blm.Toposerv.GetEndPoints(blm.Cell, keyspace, shard, tabletType)
		if err != nil {
			return nil, fmt.Errorf("endpoints fetch error: %v", err)
		}
		result := make([]string, 0, len(endpoints.Entries))
		for _, endpoint := range endpoints.Entries {
			port, ok := endpoint.NamedPortMap[blm.PortName]
			if !ok {
				return nil, fmt.Errorf("endpoints fetch error: named port %s not found in %v", blm.PortName, endpoint.NamedPortMap)
			}
			result = append(result, fmt.Sprintf("%s:%d", endpoint.Host, port))
		}
		return result, nil
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
