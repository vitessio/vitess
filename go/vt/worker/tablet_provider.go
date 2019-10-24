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

package worker

import (
	"fmt"

	"vitess.io/vitess/go/vt/vterrors"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// tabletProvider defines an interface to pick a tablet for reading data.
type tabletProvider interface {
	// getTablet returns a tablet.
	getTablet() (*topodatapb.Tablet, error)

	// returnTablet must be called after the tablet is no longer used and e.g.
	// TabletTracker.Untrack() should get called for it.
	returnTablet(*topodatapb.Tablet)

	// description returns a string which can be used in error messages e.g.
	// the name of the keyspace and the shard.
	description() string
}

// singleTabletProvider implements the tabletProvider interface and always
// returns the one tablet which was set at creation.
type singleTabletProvider struct {
	ctx   context.Context
	ts    *topo.Server
	alias *topodatapb.TabletAlias
}

func newSingleTabletProvider(ctx context.Context, ts *topo.Server, alias *topodatapb.TabletAlias) *singleTabletProvider {
	return &singleTabletProvider{ctx, ts, alias}
}

func (p *singleTabletProvider) getTablet() (*topodatapb.Tablet, error) {
	shortCtx, cancel := context.WithTimeout(p.ctx, *remoteActionsTimeout)
	tablet, err := p.ts.GetTablet(shortCtx, p.alias)
	cancel()
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to resolve tablet alias: %v err", topoproto.TabletAliasString(p.alias))
	}
	return tablet.Tablet, err
}

func (p *singleTabletProvider) returnTablet(*topodatapb.Tablet) {}

func (p *singleTabletProvider) description() string {
	return topoproto.TabletAliasString(p.alias)
}

// shardTabletProvider returns a random healthy RDONLY tablet for a given
// keyspace and shard. It uses the HealthCheck module to retrieve the tablets.
type shardTabletProvider struct {
	tsc        *discovery.TabletStatsCache
	tracker    *TabletTracker
	keyspace   string
	shard      string
	tabletType topodatapb.TabletType
}

func newShardTabletProvider(tsc *discovery.TabletStatsCache, tracker *TabletTracker, keyspace, shard string, tabletType topodatapb.TabletType) *shardTabletProvider {
	return &shardTabletProvider{tsc, tracker, keyspace, shard, tabletType}
}

func (p *shardTabletProvider) getTablet() (*topodatapb.Tablet, error) {
	// Pick any healthy serving tablet.
	tablets := p.tsc.GetHealthyTabletStats(p.keyspace, p.shard, p.tabletType)
	if len(tablets) == 0 {
		return nil, fmt.Errorf("%v: no healthy %v tablets available", p.description(), p.tabletType)
	}
	return p.tracker.Track(tablets), nil
}

func (p *shardTabletProvider) returnTablet(t *topodatapb.Tablet) {
	p.tracker.Untrack(t.Alias)
}

func (p *shardTabletProvider) description() string {
	return topoproto.KeyspaceShardString(p.keyspace, p.shard)
}
