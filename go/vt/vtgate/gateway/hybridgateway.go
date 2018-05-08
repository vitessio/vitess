/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gateway

import (
	"fmt"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/stats"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
)

// HybridGateway implements the gateway.Gateway interface by forwarding
// the queries to the right underlying implementation:
// - it has one gateway that watches for tablets. Usually a DiscoveryGateway.
//   Useful for local tablets, or remote tablets that can be accessed.
// - it has a list of remote vtgate connections to talk to l2 vtgate processes.
//   Useful for remote tablets that are far away, or if the number of local
//   tablets grows too big.
//
// Note the WaitForTablets method for now only waits on the local gateway.
type HybridGateway struct {
	queryservice.QueryService

	// gw is the local gateway that has the local connections.
	gw Gateway

	// l2vtgates is the list of remote connections to other vtgate pools.
	l2vtgates []*L2VTGateConn
}

// NewHybridGateway returns a new HybridGateway based on the provided
// parameters. gw can be nil, in which case it is assumed there is no
// local tablets.
func NewHybridGateway(gw Gateway, addrs []string, retryCount int) (*HybridGateway, error) {
	h := &HybridGateway{
		gw: gw,
	}

	for i, addr := range addrs {
		conn, err := NewL2VTGateConn(fmt.Sprintf("%v", i), addr, retryCount)
		if err != nil {
			h.Close(context.Background())
			return nil, fmt.Errorf("dialing %v failed: %v", addr, err)
		}
		h.l2vtgates = append(h.l2vtgates, conn)
	}

	h.QueryService = queryservice.Wrap(nil, h.route)
	return h, nil
}

// Close is part of the queryservice.QueryService interface.
func (h *HybridGateway) Close(ctx context.Context) error {
	for _, l := range h.l2vtgates {
		l.Close(ctx)
	}
	return nil
}

// WaitForTablets is part of the Gateway interface.
// We just forward to the local Gateway, if any.
func (h *HybridGateway) WaitForTablets(ctx context.Context, tabletTypesToWait []topodatapb.TabletType) error {
	if h.gw != nil {
		return h.gw.WaitForTablets(ctx, tabletTypesToWait)
	}

	// No local tablets, we don't wait for anything here.
	return nil
}

// RegisterStats registers the l2vtgate connection counts stats.
func (h *HybridGateway) RegisterStats() {
	stats.NewCountersFuncWithMultiLabels(
		"L2VtgateConnections",
		"number of l2vtgate connection",
		[]string{"Keyspace", "ShardName", "TabletType"},
		h.servingConnStats)
}

func (h *HybridGateway) servingConnStats() map[string]int64 {
	res := make(map[string]int64)
	for _, l := range h.l2vtgates {
		l.servingConnStats(res)
	}
	return res
}

// CacheStatus is part of the Gateway interface. It just concatenates
// all statuses from all underlying parts.
func (h *HybridGateway) CacheStatus() TabletCacheStatusList {
	var result TabletCacheStatusList

	// Start with the local Gateway part.
	if h.gw != nil {
		result = h.gw.CacheStatus()
	}

	// Then add each gateway one at a time.
	for _, l := range h.l2vtgates {
		partial := l.CacheStatus()
		result = append(result, partial...)
	}

	return result
}

// route sends the action to the right underlying implementation.
// This doesn't retry, and doesn't collect stats, as these two are
// done by the underlying gw or l2VTGateConn.
//
// FIXME(alainjobart) now we only use gw, or the one l2vtgates we have.
// Need to deprecate this code in favor of using GetAggregateStats.
func (h *HybridGateway) route(ctx context.Context, target *querypb.Target, conn queryservice.QueryService, name string, inTransaction bool, inner func(context.Context, *querypb.Target, queryservice.QueryService) (error, bool)) error {
	if h.gw != nil {
		err, _ := inner(ctx, target, h.gw)
		return NewShardError(err, target, nil, inTransaction)
	}
	if len(h.l2vtgates) == 1 {
		err, _ := inner(ctx, target, h.l2vtgates[0])
		return NewShardError(err, target, nil, inTransaction)
	}
	return NewShardError(topo.ErrNoNode, target, nil, inTransaction)
}

// GetAggregateStats is part of the srvtopo.TargetStats interface, included
// in the gateway.Gateway interface.
func (h *HybridGateway) GetAggregateStats(target *querypb.Target) (*querypb.AggregateStats, queryservice.QueryService, error) {
	// Start with the local Gateway part.
	if h.gw != nil {
		stats, qs, err := h.gw.GetAggregateStats(target)
		if err != topo.ErrNoNode {
			// The local gateway either worked, or returned an
			// error. But it knows about this target.
			return stats, qs, err
		}
	}

	// The local gateway doesn't know about this target,
	// try the remote ones.
	for _, l := range h.l2vtgates {
		stats, err := l.GetAggregateStats(target)
		if err != topo.ErrNoNode {
			// This remote gateway either worked, or returned an
			// error. But it knows about this target.
			return stats, l, err
		}
	}

	// We couldn't find a way to resolve this.
	return nil, nil, topo.ErrNoNode
}

// GetMasterCell is part of the srvtopo.TargetStats interface, included
// in the gateway.Gateway interface.
func (h *HybridGateway) GetMasterCell(keyspace, shard string) (cell string, qs queryservice.QueryService, err error) {
	// Start with the local Gateway part.
	if h.gw != nil {
		cell, qs, err := h.gw.GetMasterCell(keyspace, shard)
		if err != topo.ErrNoNode {
			// The local gateway either worked, or returned an
			// error. But it knows about this target.
			return cell, qs, err
		}
		// The local gateway doesn't know about this target,
		// try the remote ones.
	}

	for _, l := range h.l2vtgates {
		cell, err := l.GetMasterCell(keyspace, shard)
		if err != topo.ErrNoNode {
			// This remote gateway either worked, or returned an
			// error. But it knows about this target.
			return cell, l, err
		}
	}

	// We couldn't find a way to resolve this.
	return "", nil, topo.ErrNoNode
}

var _ Gateway = (*HybridGateway)(nil)
var _ srvtopo.TargetStats = (*HybridGateway)(nil)
