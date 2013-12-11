// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"reflect"
	"sort"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
)

// GetTabletMap tries to read all the tablets in the provided list,
// and returns them all in a map.
// If error is topo.ErrPartialResul, the results in the dictionary are
// incomplete, meaning some tablets couldn't be read.
func GetTabletMap(ts topo.Server, tabletAliases []topo.TabletAlias) (map[topo.TabletAlias]*topo.TabletInfo, error) {
	wg := sync.WaitGroup{}
	mutex := sync.Mutex{}

	tabletMap := make(map[topo.TabletAlias]*topo.TabletInfo)
	var someError error

	for _, tabletAlias := range tabletAliases {
		wg.Add(1)
		go func(tabletAlias topo.TabletAlias) {
			defer wg.Done()
			tabletInfo, err := ts.GetTablet(tabletAlias)
			mutex.Lock()
			if err != nil {
				log.Warningf("%v: %v", tabletAlias, err)
				// There can be data races removing nodes - ignore them for now.
				if err != topo.ErrNoNode {
					someError = topo.ErrPartialResult
				}
			} else {
				tabletMap[tabletAlias] = tabletInfo
			}
			mutex.Unlock()
		}(tabletAlias)
	}
	wg.Wait()
	return tabletMap, someError
}

// GetTabletMapForShard returns the tablets for a shard. It can return
// topo.ErrPartialResult if it couldn't read all the cells, or all
// the individual tablets, in which case the map is valid, but partial.
func GetTabletMapForShard(ts topo.Server, keyspace, shard string) (map[topo.TabletAlias]*topo.TabletInfo, error) {
	return GetTabletMapForShardByCell(ts, keyspace, shard, nil)
}

// GetTabletMapForShardByCell returns the tablets for a shard. It can return
// topo.ErrPartialResult if it couldn't read all the cells, or all
// the individual tablets, in which case the map is valid, but partial.
func GetTabletMapForShardByCell(ts topo.Server, keyspace, shard string, cells []string) (map[topo.TabletAlias]*topo.TabletInfo, error) {
	// if we get a partial result, we keep going. It most likely means
	// a cell is out of commission.
	aliases, err := topo.FindAllTabletAliasesInShardByCell(ts, keyspace, shard, cells)
	if err != nil && err != topo.ErrPartialResult {
		return nil, err
	}

	// get the tablets for the cells we were able to reach, forward
	// topo.ErrPartialResult from FindAllTabletAliasesInShard
	result, gerr := GetTabletMap(ts, aliases)
	if gerr == nil && err != nil {
		gerr = err
	}
	return result, gerr
}

// Search within a tablet map for tablets
func FindTabletByIPAddrAndPort(tabletMap map[topo.TabletAlias]*topo.TabletInfo, addr, portName string, port int) (topo.TabletAlias, error) {
	for alias, ti := range tabletMap {
		if ti.IPAddr == addr && ti.Portmap[portName] == port {
			return alias, nil
		}
	}
	return topo.TabletAlias{}, topo.ErrNoNode
}

// Return a sorted list of tablets.
func GetAllTablets(ts topo.Server, cell string) ([]*topo.TabletInfo, error) {
	aliases, err := ts.GetTabletsByCell(cell)
	if err != nil {
		return nil, err
	}
	sort.Sort(topo.TabletAliasList(aliases))

	tabletMap, err := GetTabletMap(ts, aliases)
	if err != nil {
		// we got another error than ZNONODE
		return nil, err
	}
	tablets := make([]*topo.TabletInfo, 0, len(aliases))
	for _, tabletAlias := range aliases {
		tabletInfo, ok := tabletMap[tabletAlias]
		if !ok {
			// tablet disappeared on us (GetTabletMap ignores
			// ZNONODE), just echo a warning
			log.Warningf("failed to load tablet %v", tabletAlias)
		} else {
			tablets = append(tablets, tabletInfo)
		}
	}

	return tablets, nil
}

// GetAllTabletsAccrossCells returns all tablets from known cells.
func GetAllTabletsAccrossCells(ts topo.Server) ([]*topo.TabletInfo, error) {
	cells, err := ts.GetKnownCells()
	if err != nil {
		return nil, err
	}

	results := make(chan []*topo.TabletInfo)
	errors := make(chan error)
	for _, cell := range cells {
		go func(cell string) {
			tablets, err := GetAllTablets(ts, cell)
			if err != nil && err != topo.ErrNoNode {
				errors <- err
				return
			}
			results <- tablets
		}(cell)
	}

	allTablets := make([]*topo.TabletInfo, 0)
	for _ = range cells {
		select {
		case tablets := <-results:
			allTablets = append(allTablets, tablets...)
		case err := <-errors:
			return nil, err
		}
	}
	return allTablets, nil
}

// Copy keys from from map m into a new slice with the type specified
// by typeHint.  Reflection can't make a new slice type just based on
// the key type AFAICT.
func CopyMapKeys(m interface{}, typeHint interface{}) interface{} {
	mapVal := reflect.ValueOf(m)
	keys := reflect.MakeSlice(reflect.TypeOf(typeHint), 0, mapVal.Len())
	for _, k := range mapVal.MapKeys() {
		keys = reflect.Append(keys, k)
	}
	return keys.Interface()
}

func CopyMapValues(m interface{}, typeHint interface{}) interface{} {
	mapVal := reflect.ValueOf(m)
	vals := reflect.MakeSlice(reflect.TypeOf(typeHint), 0, mapVal.Len())
	for _, k := range mapVal.MapKeys() {
		vals = reflect.Append(vals, mapVal.MapIndex(k))
	}
	return vals.Interface()
}

func mapKeys(m interface{}) []interface{} {
	keys := make([]interface{}, 0, 16)
	mapVal := reflect.ValueOf(m)
	for _, kv := range mapVal.MapKeys() {
		keys = append(keys, kv.Interface())
	}
	return keys
}
