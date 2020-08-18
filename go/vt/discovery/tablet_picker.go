/*
Copyright 2019 The Vitess Authors.

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

package discovery

import (
	"math/rand"
	"time"

	"vitess.io/vitess/go/vt/topo/topoproto"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"

	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	"vitess.io/vitess/go/vt/log"

	"golang.org/x/net/context"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
)

// TabletPicker gives a simplified API for picking tablets.
type TabletPicker struct {
	ts          *topo.Server
	cells       []string
	keyspace    string
	shard       string
	tabletTypes []topodatapb.TabletType
}

// NewTabletPicker returns a TabletPicker.
func NewTabletPicker(ts *topo.Server, cells []string, keyspace, shard, tabletTypesStr string) (*TabletPicker, error) {
	tabletTypes, err := topoproto.ParseTabletTypes(tabletTypesStr)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "failed to parse list of tablet types: %v", tabletTypesStr)
	}
	return &TabletPicker{
		ts:          ts,
		cells:       cells,
		keyspace:    keyspace,
		shard:       shard,
		tabletTypes: tabletTypes,
	}, nil
}

// PickForStreaming picks an available tablet
// All tablets that belong to tp.cells are evaluated and one is
// chosen at random
func (tp *TabletPicker) PickForStreaming(ctx context.Context) (*topodatapb.Tablet, error) {
	candidates := tp.getAllTablets(ctx)
	if len(candidates) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "no tablets available for cells:%v, keyspace/shard:%v/%v, tablet types:%v", tp.cells, tp.keyspace, tp.shard, tp.tabletTypes)
	}
	for {
		idx := 0
		// if there is only one candidate we use that, otherwise we find one randomly
		if len(candidates) > 1 {
			idx = rand.Intn(len(candidates))
		}
		alias := candidates[idx]
		// get tablet
		ti, err := tp.ts.GetTablet(ctx, alias)
		if err != nil {
			log.Warningf("unable to get tablet for alias %v", alias)
			candidates = append(candidates[:idx], candidates[idx+1:]...)
			if len(candidates) == 0 {
				break
			}
			continue
		}
		if !topoproto.IsTypeInList(ti.Tablet.Type, tp.tabletTypes) {
			// tablet is not of one of the desired types
			continue
		}

		// try to connect to tablet
		conn, err := tabletconn.GetDialer()(ti.Tablet, true)
		if err != nil {
			log.Warningf("unable to connect to tablet for alias %v", alias)
			candidates = append(candidates[:idx], candidates[idx+1:]...)
			if len(candidates) == 0 {
				break
			}
			continue
		}
		_ = conn.Close(ctx)
		return ti.Tablet, nil
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "can't find any healthy source tablet for keyspace/shard:%v/%v tablet types:%v", tp.keyspace, tp.shard, tp.tabletTypes)
}

func (tp *TabletPicker) getAllTablets(ctx context.Context) []*topodatapb.TabletAlias {
	// Special handling for MASTER tablet type
	// Since there is only one master, we ignore cell and find the master
	result := make([]*topodatapb.TabletAlias, 0)
	if len(tp.tabletTypes) == 1 && tp.tabletTypes[0] == topodatapb.TabletType_MASTER {
		si, err := tp.ts.GetShard(ctx, tp.keyspace, tp.shard)
		if err != nil {
			return result
		}
		result = append(result, si.MasterAlias)
		return result
	}
	actualCells := make([]string, 0)
	for _, cell := range tp.cells {
		// check if cell is actually an alias
		// non-blocking read so that this is fast
		alias, err := tp.ts.GetCellsAlias(ctx, cell, false)
		if err != nil {
			// either cellAlias doesn't exist or it isn't a cell alias at all. In that case assume it is a cell
			actualCells = append(actualCells, cell)
		} else {
			actualCells = append(actualCells, alias.Cells...)
		}
	}
	for _, cell := range actualCells {
		sri, err := tp.ts.GetShardReplication(ctx, cell, tp.keyspace, tp.shard)
		if err != nil {
			log.Warningf("error %v from GetShardReplication for %v %v %v", err, cell, tp.keyspace, tp.shard)
			continue
		}

		for _, node := range sri.Nodes {
			result = append(result, node.TabletAlias)
		}
	}
	return result
}

func init() {
	// TODO(sougou): consolidate this call to be once per process.
	rand.Seed(time.Now().UnixNano())
}
