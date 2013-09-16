// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package barnacle

import (
	"fmt"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/topo"
)

// bcTopoServ is a subset of topo.Server
type bcTopoServ interface {
	GetSrvTabletType(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.VtnsAddrs, error)
}

type bcTopo struct {
	Toposerv   bcTopoServ
	Cell       string
	PortName   string
	RetryDelay time.Duration
	mu         sync.Mutex
	balancers  map[string]*Balancer
}

func NewBCTopo(serv bcTopoServ, cell, namedPort string, retryDelay time.Duration) *bcTopo {
	return &bcTopo{
		Toposerv:   serv,
		Cell:       cell,
		PortName:   namedPort,
		RetryDelay: retryDelay,
		balancers:  make(map[string]*Balancer, 256),
	}
}

func (bct *bcTopo) Balancer(keyspace, shard string, tabletType topo.TabletType) *Balancer {
	key := fmt.Sprintf("%s.%s.%s.%s", bct.Cell, keyspace, tabletType, shard)
	blc, ok := bct.get(key)
	if ok {
		return blc
	}
	getAddresses := func() ([]string, error) {
		endpoints, err := bct.Toposerv.GetSrvTabletType(bct.Cell, keyspace, shard, tabletType)
		if err != nil {
			return nil, err
		}
		result := make([]string, 0, len(endpoints.Entries))
		for _, endpoint := range endpoints.Entries {
			port, ok := endpoint.NamedPortMap[bct.PortName]
			if !ok {
				return nil, fmt.Errorf("named port %s not found in %v", bct.PortName, endpoint.NamedPortMap)
			}
			result = append(result, fmt.Sprintf("%s:%d", endpoint.Host, port))
		}
		return result, nil
	}
	return bct.set(key, NewBalancer(getAddresses, bct.RetryDelay))
}

func (bct *bcTopo) get(key string) (blc *Balancer, ok bool) {
	bct.mu.Lock()
	blc, ok = bct.balancers[key]
	bct.mu.Unlock()
	return
}

func (bct *bcTopo) set(key string, blc *Balancer) *Balancer {
	bct.mu.Lock()
	defer bct.mu.Unlock()
	cur, ok := bct.balancers[key]
	if ok {
		return cur
	}
	bct.balancers[key] = blc
	return blc
}
