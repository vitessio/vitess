package worker

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// tabletProvider defines an interface to pick a tablet for reading data.
type tabletProvider interface {
	// getTablet returns a tablet.
	getTablet() (*topodatapb.Tablet, error)

	// returnTablet must be called after the tablet is no longer used and e.g.
	// TabletTracker.Untrack() should get called for it.
	returnTablet(*topodata.Tablet)

	// description returns a string which can be used in error messages e.g.
	// the name of the keyspace and the shard.
	description() string
}

// singleTabletProvider implements the tabletProvider interface and always
// returns the one tablet which was set at creation.
type singleTabletProvider struct {
	ctx   context.Context
	ts    topo.Server
	alias *topodatapb.TabletAlias
}

func newSingleTabletProvider(ctx context.Context, ts topo.Server, alias *topodatapb.TabletAlias) *singleTabletProvider {
	return &singleTabletProvider{ctx, ts, alias}
}

func (p *singleTabletProvider) getTablet() (*topodatapb.Tablet, error) {
	shortCtx, cancel := context.WithTimeout(p.ctx, *remoteActionsTimeout)
	tablet, err := p.ts.GetTablet(shortCtx, p.alias)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("failed to resolve tablet alias: %v err: %v", topoproto.TabletAliasString(p.alias), err)
	}
	return tablet.Tablet, err
}

func (p *singleTabletProvider) returnTablet(*topodata.Tablet) {}

func (p *singleTabletProvider) description() string {
	return topoproto.TabletAliasString(p.alias)
}

// shardTabletProvider returns a random healthy RDONLY tablet for a given
// keyspace and shard. It uses the HealthCheck module to retrieve the tablets.
type shardTabletProvider struct {
	tsc      *discovery.TabletStatsCache
	tracker  *TabletTracker
	keyspace string
	shard    string
}

func newShardTabletProvider(tsc *discovery.TabletStatsCache, tracker *TabletTracker, keyspace, shard string) *shardTabletProvider {
	return &shardTabletProvider{tsc, tracker, keyspace, shard}
}

func (p *shardTabletProvider) getTablet() (*topodatapb.Tablet, error) {
	// Pick any healthy serving tablet.
	tablets := p.tsc.GetHealthyTabletStats(p.keyspace, p.shard, topodatapb.TabletType_RDONLY)
	if len(tablets) == 0 {
		return nil, fmt.Errorf("%v: no healthy RDONLY tablets available", p.description())
	}
	return p.tracker.Track(tablets), nil
}

func (p *shardTabletProvider) returnTablet(t *topodatapb.Tablet) {
	p.tracker.Untrack(t.Alias)
}

func (p *shardTabletProvider) description() string {
	return topoproto.KeyspaceShardString(p.keyspace, p.shard)
}
