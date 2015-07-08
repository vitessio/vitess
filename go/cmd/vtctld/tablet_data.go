package main

import (
	"encoding/json"
	"reflect"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/query"
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

	Version              int
	TabletAlias          topo.TabletAlias
	StreamHealthResponse *pb.StreamHealthResponse
	result               []byte
	lastError            error
}

func newTabletHealth(thc *tabletHealthCache, tabletAlias topo.TabletAlias) (*TabletHealth, error) {
	th := &TabletHealth{
		Version:     1,
		TabletAlias: tabletAlias,
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

	ctx := context.Background()
	ti, err := thc.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return
	}

	ep, err := ti.EndPoint()
	if err != nil {
		return
	}

	conn, err := tabletconn.GetDialer()(ctx, *ep, ti.Keyspace, ti.Shard, 30*time.Second)
	if err != nil {
		return
	}

	stream, errFunc, err := conn.StreamHealth(ctx)
	if err != nil {
		return
	}

	for shr := range stream {
		th.mu.Lock()
		if !reflect.DeepEqual(shr, th.StreamHealthResponse) {
			th.StreamHealthResponse = shr
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
	ts topo.Server

	mu        sync.Mutex
	tabletMap map[topo.TabletAlias]*TabletHealth
}

func newTabletHealthCache(ts topo.Server) *tabletHealthCache {
	return &tabletHealthCache{
		ts:        ts,
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
