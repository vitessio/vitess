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

	"golang.org/x/net/context"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/trace"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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

// TabletRecorder is the part of the HealthCheck interface that can
// add or remove tablets. We define it as a sub-interface here so we
// can add filters on tablets if needed.
type TabletRecorder interface {
	// AddTablet adds the tablet.
	// Name is an alternate name, like an address.
	AddTablet(tablet *topodatapb.Tablet, name string)

	// RemoveTablet removes the tablet.
	RemoveTablet(tablet *topodatapb.Tablet)

	// ReplaceTablet does an AddTablet and RemoveTablet in one call, effectively replacing the old tablet with the new.
	ReplaceTablet(old, new *topodatapb.Tablet, name string)
}

// NewCellTabletsWatcher returns a TopologyWatcher that monitors all
// the tablets in a cell, and starts refreshing.
func NewCellTabletsWatcher(ctx context.Context, topoServer *topo.Server, tr TabletRecorder, cell string, refreshInterval time.Duration, refreshKnownTablets bool, topoReadConcurrency int) *TopologyWatcher {
	return NewTopologyWatcher(ctx, topoServer, tr, cell, refreshInterval, refreshKnownTablets, topoReadConcurrency, func(tw *TopologyWatcher) ([]*topodatapb.TabletAlias, error) {
		return tw.topoServer.GetTabletsByCell(ctx, tw.cell)
	})
}

// NewShardReplicationWatcher returns a TopologyWatcher that
// monitors the tablets in a cell/keyspace/shard, and starts refreshing.
func NewShardReplicationWatcher(ctx context.Context, topoServer *topo.Server, tr TabletRecorder, cell, keyspace, shard string, refreshInterval time.Duration, topoReadConcurrency int) *TopologyWatcher {
	return NewTopologyWatcher(ctx, topoServer, tr, cell, refreshInterval, true /* refreshKnownTablets */, topoReadConcurrency, func(tw *TopologyWatcher) ([]*topodatapb.TabletAlias, error) {
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

// tabletInfo is used internally by the TopologyWatcher class
type tabletInfo struct {
	alias  string
	key    string
	tablet *topodatapb.Tablet
}

// TopologyWatcher polls tablet from a configurable set of tablets
// periodically. When tablets are added / removed, it calls
// the TabletRecorder AddTablet / RemoveTablet interface appropriately.
type TopologyWatcher struct {
	// set at construction time
	topoServer          *topo.Server
	tr                  TabletRecorder
	cell                string
	refreshInterval     time.Duration
	refreshKnownTablets bool
	getTablets          func(tw *TopologyWatcher) ([]*topodatapb.TabletAlias, error)
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
func NewTopologyWatcher(ctx context.Context, topoServer *topo.Server, tr TabletRecorder, cell string, refreshInterval time.Duration, refreshKnownTablets bool, topoReadConcurrency int, getTablets func(tw *TopologyWatcher) ([]*topodatapb.TabletAlias, error)) *TopologyWatcher {
	tw := &TopologyWatcher{
		topoServer:          topoServer,
		tr:                  tr,
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
	tw.wg.Add(1)
	go tw.watch()
	return tw
}

// watch polls all tablets and notifies TabletRecorder by adding/removing tablets.
func (tw *TopologyWatcher) watch() {
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

// loadTablets reads all tablets from topology, and updates TabletRecorder.
func (tw *TopologyWatcher) loadTablets() {
	var wg sync.WaitGroup
	newTablets := make(map[string]*tabletInfo)
	replacedTablets := make(map[string]*tabletInfo)

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
			newTablets[aliasStr] = &tabletInfo{
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
// for the first time and transfers the information to TabletRecorder via its
// AddTablet() method.
func (tw *TopologyWatcher) WaitForInitialTopology() error {
	select {
	case <-tw.ctx.Done():
		return tw.ctx.Err()
	case <-tw.firstLoadChan:
		return nil
	}
}

// Stop stops the watcher. It does not clean up the tablets added to TabletRecorder.
func (tw *TopologyWatcher) Stop() {
	tw.cancelFunc()
	// wait for watch goroutine to finish.
	tw.wg.Wait()
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

// FilterByShard is a TabletRecorder filter that filters tablets by
// keyspace/shard.
type FilterByShard struct {
	// tr is the underlying TabletRecorder to forward requests too
	tr TabletRecorder

	// filters is a map of keyspace to filters for shards
	filters map[string][]*filterShard
}

// filterShard describes a filter for a given shard or keyrange inside
// a keyspace
type filterShard struct {
	keyspace string
	shard    string
	keyRange *topodatapb.KeyRange // only set if shard is also a KeyRange
}

// NewFilterByShard creates a new FilterByShard on top of an existing
// TabletRecorder. Each filter is a keyspace|shard entry, where shard
// can either be a shard name, or a keyrange. All tablets that match
// at least one keyspace|shard tuple will be forwarded to the
// underlying TabletRecorder.
func NewFilterByShard(tr TabletRecorder, filters []string) (*FilterByShard, error) {
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
		tr:      tr,
		filters: m,
	}, nil
}

// AddTablet is part of the TabletRecorder interface.
func (fbs *FilterByShard) AddTablet(tablet *topodatapb.Tablet, name string) {
	if fbs.isIncluded(tablet) {
		fbs.tr.AddTablet(tablet, name)
	}
}

// RemoveTablet is part of the TabletRecorder interface.
func (fbs *FilterByShard) RemoveTablet(tablet *topodatapb.Tablet) {
	if fbs.isIncluded(tablet) {
		fbs.tr.RemoveTablet(tablet)
	}
}

// ReplaceTablet is part of the TabletRecorder interface.
func (fbs *FilterByShard) ReplaceTablet(old, new *topodatapb.Tablet, name string) {
	if fbs.isIncluded(old) && fbs.isIncluded(new) {
		fbs.tr.ReplaceTablet(old, new, name)
	}
}

// isIncluded returns true iff the tablet's keyspace and shard should be
// forwarded to the underlying TabletRecorder.
func (fbs *FilterByShard) isIncluded(tablet *topodatapb.Tablet) bool {
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
