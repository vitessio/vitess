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

	"vitess.io/vitess/go/vt/topo/topoproto"

	"vitess.io/vitess/go/vt/key"

	"context"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/trace"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
)

const (
	topologyWatcherOpListTablets   = "ListTablets"
	topologyWatcherOpGetTablet     = "GetTablet"
	topologyWatcherOpAddTablet     = "AddTablet"
	topologyWatcherOpRemoveTablet  = "RemoveTablet"
	topologyWatcherOpReplaceTablet = "ReplaceTablet"
)

var (
	topologyWatcherOperations = stats.NewCountersWithSingleLabel("TopologyWatcherOperations", "Topology watcher operation counts",
		"Operation", topologyWatcherOpListTablets, topologyWatcherOpGetTablet, topologyWatcherOpAddTablet, topologyWatcherOpRemoveTablet, topologyWatcherOpReplaceTablet)
	topologyWatcherErrors = stats.NewCountersWithSingleLabel("TopologyWatcherErrors", "Topology watcher error counts",
		"Operation", topologyWatcherOpListTablets, topologyWatcherOpGetTablet)
)

// tabletInfo is used internally by the TopologyWatcher class
type tabletInfo struct {
	alias  string
	tablet *topodata.Tablet
}

// TopologyWatcher polls tablet from a configurable set of tablets
// periodically. When tablets are added / removed, it calls
// the LegacyTabletRecorder AddTablet / RemoveTablet interface appropriately.
type TopologyWatcher struct {
	// set at construction time
	topoServer          *topo.Server
	tabletRecorder      TabletRecorder
	tabletFilter        TabletFilter
	cell                string
	refreshInterval     time.Duration
	refreshKnownTablets bool
	getTablets          func(tw *TopologyWatcher) ([]*topodata.TabletAlias, error)
	sem                 chan int
	ctx                 context.Context
	cancelFunc          context.CancelFunc
	// wg keeps track of all launched Go routines.
	wg sync.WaitGroup

	// mu protects all variables below
	mu sync.Mutex
	// tablets contains a map of alias -> tabletInfo for all known tablets
	tablets map[string]*tabletInfo
	// topoChecksum stores a crc32 of the tablets map and is exported as a metric
	topoChecksum uint32
	// lastRefresh records the timestamp of the last topo refresh
	lastRefresh time.Time
	// firstLoadDone is true when first load of the topology data is done.
	firstLoadDone bool
	// firstLoadChan is closed when the initial loading of topology data is done.
	firstLoadChan chan struct{}
}

// NewTopologyWatcher returns a TopologyWatcher that monitors all
// the tablets in a cell, and starts refreshing.
func NewTopologyWatcher(ctx context.Context, topoServer *topo.Server, tr TabletRecorder, filter TabletFilter, cell string, refreshInterval time.Duration, refreshKnownTablets bool, topoReadConcurrency int, getTablets func(tw *TopologyWatcher) ([]*topodata.TabletAlias, error)) *TopologyWatcher {
	tw := &TopologyWatcher{
		topoServer:          topoServer,
		tabletRecorder:      tr,
		tabletFilter:        filter,
		cell:                cell,
		refreshInterval:     refreshInterval,
		refreshKnownTablets: refreshKnownTablets,
		getTablets:          getTablets,
		sem:                 make(chan int, topoReadConcurrency),
		tablets:             make(map[string]*tabletInfo),
	}
	tw.firstLoadChan = make(chan struct{})

	// We want the span from the context, but not the cancelation that comes with it
	spanContext := trace.CopySpan(context.Background(), ctx)
	tw.ctx, tw.cancelFunc = context.WithCancel(spanContext)
	return tw
}

// NewCellTabletsWatcher returns a TopologyWatcher that monitors all
// the tablets in a cell, and starts refreshing.
func NewCellTabletsWatcher(ctx context.Context, topoServer *topo.Server, tr TabletRecorder, f TabletFilter, cell string, refreshInterval time.Duration, refreshKnownTablets bool, topoReadConcurrency int) *TopologyWatcher {
	return NewTopologyWatcher(ctx, topoServer, tr, f, cell, refreshInterval, refreshKnownTablets, topoReadConcurrency, func(tw *TopologyWatcher) ([]*topodata.TabletAlias, error) {
		return tw.topoServer.GetTabletsByCell(ctx, tw.cell)
	})
}

// Start starts the topology watcher
func (tw *TopologyWatcher) Start() {
	tw.wg.Add(1)
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

// Stop stops the watcher. It does not clean up the tablets added to LegacyTabletRecorder.
func (tw *TopologyWatcher) Stop() {
	tw.cancelFunc()
	// wait for watch goroutine to finish.
	tw.wg.Wait()
}

func (tw *TopologyWatcher) loadTablets() {
	var wg sync.WaitGroup
	newTablets := make(map[string]*tabletInfo)

	// first get the list of relevant tabletAliases
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
			// we already have a tabletInfo for this and the flag tells us to not refresh
			if val, ok := tw.tablets[aliasStr]; ok {
				newTablets[aliasStr] = val
				continue
			}
		}

		wg.Add(1)
		go func(alias *topodata.TabletAlias) {
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
			if !(tw.tabletFilter == nil || tw.tabletFilter.IsIncluded(tablet.Tablet)) {
				return
			}
			tw.mu.Lock()
			aliasStr := topoproto.TabletAliasString(alias)
			newTablets[aliasStr] = &tabletInfo{
				alias:  aliasStr,
				tablet: tablet.Tablet,
			}
			tw.mu.Unlock()
		}(tAlias)
	}

	tw.mu.Unlock()
	wg.Wait()
	tw.mu.Lock()

	for alias, newVal := range newTablets {
		// trust the alias from topo and add it if it doesn't exist
		if val, ok := tw.tablets[alias]; !ok {
			tw.tabletRecorder.AddTablet(newVal.tablet)
			topologyWatcherOperations.Add(topologyWatcherOpAddTablet, 1)
		} else {
			// check if the host and port have changed. If yes, replace tablet
			oldKey := TabletToMapKey(val.tablet)
			newKey := TabletToMapKey(newVal.tablet)
			if oldKey != newKey {
				// This is the case where the same tablet alias is now reporting
				// a different address key.
				tw.tabletRecorder.ReplaceTablet(val.tablet, newVal.tablet)
				topologyWatcherOperations.Add(topologyWatcherOpReplaceTablet, 1)
			}
		}
	}

	for _, val := range tw.tablets {
		if _, ok := newTablets[val.alias]; !ok {
			tw.tabletRecorder.RemoveTablet(val.tablet)
			topologyWatcherOperations.Add(topologyWatcherOpRemoveTablet, 1)
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
		_, ok := tw.tablets[alias]
		if ok {
			buf.WriteString(alias)
		}
	}
	tw.topoChecksum = crc32.ChecksumIEEE(buf.Bytes())
	tw.lastRefresh = time.Now()

	tw.mu.Unlock()

}

// RefreshLag returns the time since the last refresh
func (tw *TopologyWatcher) RefreshLag() time.Duration {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	return time.Since(tw.lastRefresh)
}

// TopoChecksum returns the checksum of the current state of the topo
func (tw *TopologyWatcher) TopoChecksum() uint32 {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	return tw.topoChecksum
}

// TabletFilter is an interface that can be given to a TopologyWatcher
// to be applied as an additional filter on the list of tablets returned by its getTablets function
type TabletFilter interface {
	// IsIncluded returns whether tablet is included in this filter
	IsIncluded(tablet *topodata.Tablet) bool
}

// FilterByShard is a filter that filters tablets by
// keyspace/shard.
type FilterByShard struct {
	// filters is a map of keyspace to filters for shards
	filters map[string][]*filterShard
}

// filterShard describes a filter for a given shard or keyrange inside
// a keyspace
type filterShard struct {
	keyspace string
	shard    string
	keyRange *topodata.KeyRange // only set if shard is also a KeyRange
}

// NewFilterByShard creates a new FilterByShard on top of an existing
// LegacyTabletRecorder. Each filter is a keyspace|shard entry, where shard
// can either be a shard name, or a keyrange. All tablets that match
// at least one keyspace|shard tuple will be forwarded to the
// underlying LegacyTabletRecorder.
func NewFilterByShard(filters []string) (*FilterByShard, error) {
	m := make(map[string][]*filterShard)
	for _, filter := range filters {
		parts := strings.Split(filter, "|")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid FilterByShard parameter: %v", filter)
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

	return &FilterByShard{
		filters: m,
	}, nil
}

// IsIncluded returns true iff the tablet's keyspace and shard should be
// forwarded to the underlying LegacyTabletRecorder.
func (fbs *FilterByShard) IsIncluded(tablet *topodata.Tablet) bool {
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

// FilterByKeyspace is a filter that filters tablets by
// keyspace
type FilterByKeyspace struct {
	keyspaces map[string]bool
}

// NewFilterByKeyspace creates a new FilterByKeyspace.
// Each filter is a keyspace entry. All tablets that match
// a keyspace will be forwarded to the underlying LegacyTabletRecorder.
func NewFilterByKeyspace(selectedKeyspaces []string) *FilterByKeyspace {
	m := make(map[string]bool)
	for _, keyspace := range selectedKeyspaces {
		m[keyspace] = true
	}

	return &FilterByKeyspace{
		keyspaces: m,
	}
}

// IsIncluded returns true if the tablet's keyspace should be
// forwarded to the underlying LegacyTabletRecorder.
func (fbk *FilterByKeyspace) IsIncluded(tablet *topodata.Tablet) bool {
	_, exist := fbk.keyspaces[tablet.Keyspace]
	return exist
}
