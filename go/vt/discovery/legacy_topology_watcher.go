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
	"bytes"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
	"sync"
	"time"

	"context"

	"vitess.io/vitess/go/trace"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// tabletInfo is used internally by the TopologyWatcher class
type legacyTabletInfo struct {
	alias  string
	key    string
	tablet *topodatapb.Tablet
}

// NewLegacyCellTabletsWatcher returns a LegacyTopologyWatcher that monitors all
// the tablets in a cell, and starts refreshing.
func NewLegacyCellTabletsWatcher(ctx context.Context, topoServer *topo.Server, tr LegacyTabletRecorder, cell string, refreshInterval time.Duration, refreshKnownTablets bool, topoReadConcurrency int) *LegacyTopologyWatcher {
	return NewLegacyTopologyWatcher(ctx, topoServer, tr, cell, refreshInterval, refreshKnownTablets, topoReadConcurrency, func(tw *LegacyTopologyWatcher) ([]*topodatapb.TabletAlias, error) {
		return tw.topoServer.GetTabletsByCell(ctx, tw.cell)
	})
}

// NewLegacyShardReplicationWatcher returns a LegacyTopologyWatcher that
// monitors the tablets in a cell/keyspace/shard, and starts refreshing.
func NewLegacyShardReplicationWatcher(ctx context.Context, topoServer *topo.Server, tr LegacyTabletRecorder, cell, keyspace, shard string, refreshInterval time.Duration, topoReadConcurrency int) *LegacyTopologyWatcher {
	return NewLegacyTopologyWatcher(ctx, topoServer, tr, cell, refreshInterval, true /* RefreshKnownTablets */, topoReadConcurrency, func(tw *LegacyTopologyWatcher) ([]*topodatapb.TabletAlias, error) {
		sri, err := tw.topoServer.GetShardReplication(ctx, tw.cell, keyspace, shard)
		switch {
		case err == nil:
			// we handle this case after this switch block
		case topo.IsErrType(err, topo.NoNode):
			// this is not an error
			return nil, nil
		default:
			return nil, err
		}

		result := make([]*topodatapb.TabletAlias, len(sri.Nodes))
		for i, node := range sri.Nodes {
			result[i] = node.TabletAlias
		}
		return result, nil
	})
}

// LegacyTopologyWatcher polls tablet from a configurable set of tablets
// periodically. When tablets are added / removed, it calls
// the LegacyTabletRecorder AddTablet / RemoveTablet interface appropriately.
type LegacyTopologyWatcher struct {
	// set at construction time
	topoServer          *topo.Server
	tr                  LegacyTabletRecorder
	cell                string
	refreshInterval     time.Duration
	refreshKnownTablets bool
	getTablets          func(tw *LegacyTopologyWatcher) ([]*topodatapb.TabletAlias, error)
	sem                 chan int
	ctx                 context.Context
	cancelFunc          context.CancelFunc
	// wg keeps track of all launched Go routines.
	wg sync.WaitGroup

	// mu protects all variables below
	mu sync.Mutex
	// tablets contains a map of alias -> tabletInfo for all known tablets
	tablets map[string]*legacyTabletInfo
	// topoChecksum stores a crc32 of the tablets map and is exported as a metric
	topoChecksum uint32
	// lastRefresh records the timestamp of the last topo refresh
	lastRefresh time.Time
	// firstLoadDone is true when first load of the topology data is done.
	firstLoadDone bool
	// firstLoadChan is closed when the initial loading of topology data is done.
	firstLoadChan chan struct{}
}

// NewLegacyTopologyWatcher returns a LegacyTopologyWatcher that monitors all
// the tablets in a cell, and starts refreshing.
func NewLegacyTopologyWatcher(ctx context.Context, topoServer *topo.Server, tr LegacyTabletRecorder, cell string, refreshInterval time.Duration, refreshKnownTablets bool, topoReadConcurrency int, getTablets func(tw *LegacyTopologyWatcher) ([]*topodatapb.TabletAlias, error)) *LegacyTopologyWatcher {
	tw := &LegacyTopologyWatcher{
		topoServer:          topoServer,
		tr:                  tr,
		cell:                cell,
		refreshInterval:     refreshInterval,
		refreshKnownTablets: refreshKnownTablets,
		getTablets:          getTablets,
		sem:                 make(chan int, topoReadConcurrency),
		tablets:             make(map[string]*legacyTabletInfo),
	}
	tw.firstLoadChan = make(chan struct{})

	// We want the span from the context, but not the cancelation that comes with it
	spanContext := trace.CopySpan(context.Background(), ctx)
	tw.ctx, tw.cancelFunc = context.WithCancel(spanContext)
	tw.wg.Add(1)
	go tw.watch()
	return tw
}

// watch polls all tablets and notifies LegacyTabletRecorder by adding/removing tablets.
func (tw *LegacyTopologyWatcher) watch() {
	defer tw.wg.Done()
	ticker := time.NewTicker(tw.refreshInterval)
	defer ticker.Stop()
	for {
		tw.loadTablets()
		select {
		case <-tw.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// loadTablets reads all tablets from topology, and updates LegacyTabletRecorder.
func (tw *LegacyTopologyWatcher) loadTablets() {
	var wg sync.WaitGroup
	newTablets := make(map[string]*legacyTabletInfo)
	replacedTablets := make(map[string]*legacyTabletInfo)

	tabletAliases, err := tw.getTablets(tw)
	topologyWatcherOperations.Add(topologyWatcherOpListTablets, 1)
	if err != nil {
		topologyWatcherErrors.Add(topologyWatcherOpListTablets, 1)
		select {
		case <-tw.ctx.Done():
			return
		default:
		}
		log.Errorf("cannot get tablets for cell: %v: %v", tw.cell, err)
		return
	}

	// Accumulate a list of all known alias strings to use later
	// when sorting
	tabletAliasStrs := make([]string, 0, len(tabletAliases))

	tw.mu.Lock()
	for _, tAlias := range tabletAliases {
		aliasStr := topoproto.TabletAliasString(tAlias)
		tabletAliasStrs = append(tabletAliasStrs, aliasStr)

		if !tw.refreshKnownTablets {
			if val, ok := tw.tablets[aliasStr]; ok {
				newTablets[aliasStr] = val
				continue
			}
		}

		wg.Add(1)
		go func(alias *topodatapb.TabletAlias) {
			defer wg.Done()
			tw.sem <- 1 // Wait for active queue to drain.
			tablet, err := tw.topoServer.GetTablet(tw.ctx, alias)
			topologyWatcherOperations.Add(topologyWatcherOpGetTablet, 1)
			<-tw.sem // Done; enable next request to run
			if err != nil {
				topologyWatcherErrors.Add(topologyWatcherOpGetTablet, 1)
				select {
				case <-tw.ctx.Done():
					return
				default:
				}
				log.Errorf("cannot get tablet for alias %v: %v", alias, err)
				return
			}
			tw.mu.Lock()
			aliasStr := topoproto.TabletAliasString(alias)
			newTablets[aliasStr] = &legacyTabletInfo{
				alias:  aliasStr,
				key:    TabletToMapKey(tablet.Tablet),
				tablet: tablet.Tablet,
			}
			tw.mu.Unlock()
		}(tAlias)
	}

	tw.mu.Unlock()
	wg.Wait()
	tw.mu.Lock()

	for alias, newVal := range newTablets {
		if val, ok := tw.tablets[alias]; !ok {
			// Check if there's a tablet with the same address key but a
			// different alias. If so, replace it and keep track of the
			// replaced alias to make sure it isn't removed later.
			found := false
			for _, otherVal := range tw.tablets {
				if newVal.key == otherVal.key {
					found = true
					tw.tr.ReplaceTablet(otherVal.tablet, newVal.tablet, alias)
					topologyWatcherOperations.Add(topologyWatcherOpReplaceTablet, 1)
					replacedTablets[otherVal.alias] = newVal
				}
			}
			if !found {
				tw.tr.AddTablet(newVal.tablet, alias)
				topologyWatcherOperations.Add(topologyWatcherOpAddTablet, 1)
			}

		} else if val.key != newVal.key {
			// Handle the case where the same tablet alias is now reporting
			// a different address key.
			replacedTablets[alias] = newVal
			tw.tr.ReplaceTablet(val.tablet, newVal.tablet, alias)
			topologyWatcherOperations.Add(topologyWatcherOpReplaceTablet, 1)
		}
	}

	for _, val := range tw.tablets {
		if _, ok := newTablets[val.alias]; !ok {
			if _, ok2 := replacedTablets[val.alias]; !ok2 {
				tw.tr.RemoveTablet(val.tablet)
				topologyWatcherOperations.Add(topologyWatcherOpRemoveTablet, 1)
			}
		}
	}
	tw.tablets = newTablets
	if !tw.firstLoadDone {
		tw.firstLoadDone = true
		close(tw.firstLoadChan)
	}

	// iterate through the tablets in a stable order and compute a
	// checksum of the tablet map
	sort.Strings(tabletAliasStrs)
	var buf bytes.Buffer
	for _, alias := range tabletAliasStrs {
		tabletInfo, ok := tw.tablets[alias]
		if ok {
			buf.WriteString(alias)
			buf.WriteString(tabletInfo.key)
		}
	}
	tw.topoChecksum = crc32.ChecksumIEEE(buf.Bytes())
	tw.lastRefresh = time.Now()

	tw.mu.Unlock()
}

// WaitForInitialTopology waits until the watcher reads all of the topology data
// for the first time and transfers the information to LegacyTabletRecorder via its
// AddTablet() method.
func (tw *LegacyTopologyWatcher) WaitForInitialTopology() error {
	select {
	case <-tw.ctx.Done():
		return tw.ctx.Err()
	case <-tw.firstLoadChan:
		return nil
	}
}

// Stop stops the watcher. It does not clean up the tablets added to LegacyTabletRecorder.
func (tw *LegacyTopologyWatcher) Stop() {
	tw.cancelFunc()
	// wait for watch goroutine to finish.
	tw.wg.Wait()
}

// RefreshLag returns the time since the last refresh
func (tw *LegacyTopologyWatcher) RefreshLag() time.Duration {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	return time.Since(tw.lastRefresh)
}

// TopoChecksum returns the checksum of the current state of the topo
func (tw *LegacyTopologyWatcher) TopoChecksum() uint32 {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	return tw.topoChecksum
}

// LegacyFilterByShard is a LegacyTabletRecorder filter that filters tablets by
// keyspace/shard.
type LegacyFilterByShard struct {
	// tr is the underlying LegacyTabletRecorder to forward requests too
	tr LegacyTabletRecorder

	// filters is a map of keyspace to filters for shards
	filters map[string][]*filterShard
}

// NewLegacyFilterByShard creates a new LegacyFilterByShard on top of an existing
// LegacyTabletRecorder. Each filter is a keyspace|shard entry, where shard
// can either be a shard name, or a keyrange. All tablets that match
// at least one keyspace|shard tuple will be forwarded to the
// underlying LegacyTabletRecorder.
func NewLegacyFilterByShard(tr LegacyTabletRecorder, filters []string) (*LegacyFilterByShard, error) {
	m := make(map[string][]*filterShard)
	for _, filter := range filters {
		parts := strings.Split(filter, "|")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid LegacyFilterByShard parameter: %v", filter)
		}

		keyspace := parts[0]
		shard := parts[1]

		// extract keyrange if it's a range
		canonical, kr, err := topo.ValidateShardName(shard)
		if err != nil {
			return nil, fmt.Errorf("error parsing shard name %v: %v", shard, err)
		}

		// check for duplicates
		for _, c := range m[keyspace] {
			if c.shard == canonical {
				return nil, fmt.Errorf("duplicate %v/%v entry", keyspace, shard)
			}
		}

		m[keyspace] = append(m[keyspace], &filterShard{
			keyspace: keyspace,
			shard:    canonical,
			keyRange: kr,
		})
	}

	return &LegacyFilterByShard{
		tr:      tr,
		filters: m,
	}, nil
}

// AddTablet is part of the LegacyTabletRecorder interface.
func (fbs *LegacyFilterByShard) AddTablet(tablet *topodatapb.Tablet, name string) {
	if fbs.isIncluded(tablet) {
		fbs.tr.AddTablet(tablet, name)
	}
}

// RemoveTablet is part of the LegacyTabletRecorder interface.
func (fbs *LegacyFilterByShard) RemoveTablet(tablet *topodatapb.Tablet) {
	if fbs.isIncluded(tablet) {
		fbs.tr.RemoveTablet(tablet)
	}
}

// ReplaceTablet is part of the LegacyTabletRecorder interface.
func (fbs *LegacyFilterByShard) ReplaceTablet(old, new *topodatapb.Tablet, name string) {
	if fbs.isIncluded(old) && fbs.isIncluded(new) {
		fbs.tr.ReplaceTablet(old, new, name)
	}
}

// isIncluded returns true iff the tablet's keyspace and shard should be
// forwarded to the underlying LegacyTabletRecorder.
func (fbs *LegacyFilterByShard) isIncluded(tablet *topodatapb.Tablet) bool {
	canonical, kr, err := topo.ValidateShardName(tablet.Shard)
	if err != nil {
		log.Errorf("Error parsing shard name %v, will ignore tablet: %v", tablet.Shard, err)
		return false
	}

	for _, c := range fbs.filters[tablet.Keyspace] {
		if canonical == c.shard {
			// Exact match (probably a non-sharded keyspace).
			return true
		}
		if kr != nil && c.keyRange != nil && key.KeyRangeIncludes(c.keyRange, kr) {
			// Our filter's KeyRange includes the provided KeyRange
			return true
		}
	}
	return false
}

// LegacyFilterByKeyspace is a LegacyTabletRecorder filter that filters tablets by
// keyspace
type LegacyFilterByKeyspace struct {
	tr LegacyTabletRecorder

	keyspaces map[string]bool
}

// NewLegacyFilterByKeyspace creates a new LegacyFilterByKeyspace on top of an existing
// LegacyTabletRecorder. Each filter is a keyspace entry. All tablets that match
// a keyspace will be forwarded to the underlying LegacyTabletRecorder.
func NewLegacyFilterByKeyspace(tr LegacyTabletRecorder, selectedKeyspaces []string) *LegacyFilterByKeyspace {
	m := make(map[string]bool)
	for _, keyspace := range selectedKeyspaces {
		m[keyspace] = true
	}

	return &LegacyFilterByKeyspace{
		tr:        tr,
		keyspaces: m,
	}
}

// AddTablet is part of the LegacyTabletRecorder interface.
func (fbk *LegacyFilterByKeyspace) AddTablet(tablet *topodatapb.Tablet, name string) {
	if fbk.isIncluded(tablet) {
		fbk.tr.AddTablet(tablet, name)
	}
}

// RemoveTablet is part of the LegacyTabletRecorder interface.
func (fbk *LegacyFilterByKeyspace) RemoveTablet(tablet *topodatapb.Tablet) {
	if fbk.isIncluded(tablet) {
		fbk.tr.RemoveTablet(tablet)
	}
}

// ReplaceTablet is part of the LegacyTabletRecorder interface.
func (fbk *LegacyFilterByKeyspace) ReplaceTablet(old *topodatapb.Tablet, new *topodatapb.Tablet, name string) {
	if old.Keyspace != new.Keyspace {
		log.Errorf("Error replacing old tablet in %v with new tablet in %v", old.Keyspace, new.Keyspace)
		return
	}

	if fbk.isIncluded(new) {
		fbk.tr.ReplaceTablet(old, new, name)
	}
}

// isIncluded returns true if the tablet's keyspace should be
// forwarded to the underlying LegacyTabletRecorder.
func (fbk *LegacyFilterByKeyspace) isIncluded(tablet *topodatapb.Tablet) bool {
	_, exist := fbk.keyspaces[tablet.Keyspace]
	return exist
}
