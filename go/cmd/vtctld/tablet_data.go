package main

import (
	"encoding/json"
	"reflect"
	"sync"

	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

// This file maintains a tablet health cache. It establishes streaming
// connections with tablets, and updates its internal state with the
// result. The first time something is requested, the returned data is
// empty.  We assume the frontend will ask again a few seconds later
// and get the up-to-date data then.

// TabletHealth is the structure we export via json.
// Private fields are not exported.
type TabletHealth struct {
	// mu protects the entire data structure
	mu sync.Mutex

	Version           int
	HealthStreamReply actionnode.HealthStreamReply
	result            []byte
	lastError         error
}

func newTabletHealth(thc *tabletHealthCache, tabletAlias topo.TabletAlias) (*TabletHealth, error) {
	th := &TabletHealth{
		Version: 1,
	}
	th.HealthStreamReply.Tablet = &topo.Tablet{
		Alias: tabletAlias,
	}
	var err error
	th.result, err = json.MarshalIndent(th, "", "  ")
	if err != nil {
		return nil, err
	}
	go th.update(thc, tabletAlias)
	return th, nil
}

func (th *TabletHealth) update(thc *tabletHealthCache, tabletAlias topo.TabletAlias) {
	defer thc.delete(tabletAlias)

	ti, err := thc.ts.GetTablet(tabletAlias)
	if err != nil {
		return
	}

	ctx := context.Background()
	c, errFunc, err := thc.tmc.HealthStream(ctx, ti)
	if err != nil {
		return
	}

	for hsr := range c {
		th.mu.Lock()
		if !reflect.DeepEqual(hsr, &th.HealthStreamReply) {
			th.HealthStreamReply = *hsr
			th.Version++
			th.result, th.lastError = json.MarshalIndent(th, "", "  ")
		}
		th.mu.Unlock()
	}

	// we call errFunc as some implementations may use this to
	// free resources.
	errFunc()
}

func (th *TabletHealth) get() ([]byte, error) {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.result, th.lastError
}

type tabletHealthCache struct {
	ts  topo.Server
	tmc tmclient.TabletManagerClient

	mu        sync.Mutex
	tabletMap map[topo.TabletAlias]*TabletHealth
}

func newTabletHealthCache(ts topo.Server, tmc tmclient.TabletManagerClient) *tabletHealthCache {
	return &tabletHealthCache{
		ts:        ts,
		tmc:       tmc,
		tabletMap: make(map[topo.TabletAlias]*TabletHealth),
	}
}

func (thc *tabletHealthCache) get(tabletAlias topo.TabletAlias) ([]byte, error) {
	thc.mu.Lock()
	th, ok := thc.tabletMap[tabletAlias]
	if !ok {
		var err error
		th, err = newTabletHealth(thc, tabletAlias)
		if err != nil {
			thc.mu.Unlock()
			return nil, err
		}
		thc.tabletMap[tabletAlias] = th
	}
	thc.mu.Unlock()

	return th.get()
}

func (thc *tabletHealthCache) delete(tabletAlias topo.TabletAlias) {
	thc.mu.Lock()
	delete(thc.tabletMap, tabletAlias)
	thc.mu.Unlock()
}
