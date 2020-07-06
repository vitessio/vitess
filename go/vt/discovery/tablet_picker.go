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
	"fmt"
	"math/rand"
	"strings"
	"time"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"

	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	"vitess.io/vitess/go/vt/log"

	"golang.org/x/net/context"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
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
func NewTabletPicker(ts *topo.Server, cell, keyspace, shard, tabletTypesStr string) (*TabletPicker, error) {
	tabletTypes, err := topoproto.ParseTabletTypes(tabletTypesStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse list of tablet types: %v", tabletTypesStr)
	}

	cells := strings.Split(cell, ",")

	return &TabletPicker{
		ts:          ts,
		cells:       cells,
		keyspace:    keyspace,
		shard:       shard,
		tabletTypes: tabletTypes,
	}, nil
}

// PickForStreaming picks all healthy tablets including the non-serving ones.
func (tp *TabletPicker) PickForStreaming(ctx context.Context) (*topodatapb.Tablet, error) {
	// TODO: parse tp.cell and call this for one cell at a time?
	candidates := tp.getAllTablets(ctx)
	if len(candidates) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "no tablets available for %v %v %v", tp.cells, tp.keyspace, tp.shard)
	}
	for {
		idx := rand.Intn(len(candidates))
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
	return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "can't find any healthy source tablet for %v %v %v", tp.keyspace, tp.shard, tp.tabletTypes)
}

func (tp *TabletPicker) getAllTablets(ctx context.Context) []*topodatapb.TabletAlias {
	result := make([]*topodatapb.TabletAlias, 0)
	for _, cell := range tp.cells {
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
