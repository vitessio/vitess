// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools

import (
	"reflect"
	"sort"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
)

// FindTabletByIPAddrAndPort searches within a tablet map for tablets
func FindTabletByIPAddrAndPort(tabletMap map[topo.TabletAlias]*topo.TabletInfo, addr, portName string, port int) (topo.TabletAlias, error) {
	for alias, ti := range tabletMap {
		if ti.IPAddr == addr && ti.Portmap[portName] == port {
			return alias, nil
		}
	}
	return topo.TabletAlias{}, topo.ErrNoNode
}

// GetAllTablets returns a sorted list of tablets.
func GetAllTablets(ts topo.Server, cell string) ([]*topo.TabletInfo, error) {
	aliases, err := ts.GetTabletsByCell(cell)
	if err != nil {
		return nil, err
	}
	sort.Sort(topo.TabletAliasList(aliases))

	tabletMap, err := topo.GetTabletMap(ts, aliases)
	if err != nil {
		// we got another error than topo.ErrNoNode
		return nil, err
	}
	tablets := make([]*topo.TabletInfo, 0, len(aliases))
	for _, tabletAlias := range aliases {
		tabletInfo, ok := tabletMap[tabletAlias]
		if !ok {
			// tablet disappeared on us (GetTabletMap ignores
			// topo.ErrNoNode), just echo a warning
			log.Warningf("failed to load tablet %v", tabletAlias)
		} else {
			tablets = append(tablets, tabletInfo)
		}
	}

	return tablets, nil
}

// GetAllTabletsAcrossCells returns all tablets from known cells.
// If it returns topo.ErrPartialResult, then the list is valid, but partial.
func GetAllTabletsAcrossCells(ts topo.Server) ([]*topo.TabletInfo, error) {
	cells, err := ts.GetKnownCells()
	if err != nil {
		return nil, err
	}

	results := make([][]*topo.TabletInfo, len(cells))
	errors := make([]error, len(cells))
	wg := sync.WaitGroup{}
	wg.Add(len(cells))
	for i, cell := range cells {
		go func(i int, cell string) {
			results[i], errors[i] = GetAllTablets(ts, cell)
			wg.Done()
		}(i, cell)
	}
	wg.Wait()

	err = nil
	allTablets := make([]*topo.TabletInfo, 0)
	for i, _ := range cells {
		if errors[i] == nil {
			allTablets = append(allTablets, results[i]...)
		} else {
			err = topo.ErrPartialResult
		}
	}
	return allTablets, err
}

// SortedTabletMap returns two maps:
// - The slaveMap contains all the non-master non-scrapped hosts.
//   This can be used as a list of slaves to fix up for reparenting
// - The masterMap contains all the tablets without parents
//   (scrapped or not). This can be used to special case
//   the old master, and any tablet in a weird state, left over, ...
func SortedTabletMap(tabletMap map[topo.TabletAlias]*topo.TabletInfo) (map[topo.TabletAlias]*topo.TabletInfo, map[topo.TabletAlias]*topo.TabletInfo) {
	slaveMap := make(map[topo.TabletAlias]*topo.TabletInfo)
	masterMap := make(map[topo.TabletAlias]*topo.TabletInfo)
	for alias, ti := range tabletMap {
		if ti.Type != topo.TYPE_MASTER && ti.Type != topo.TYPE_SCRAP {
			slaveMap[alias] = ti
		} else if ti.Parent.Uid == topo.NO_TABLET {
			masterMap[alias] = ti
		}
	}
	return slaveMap, masterMap
}

// CopyMapKeys copies keys from from map m into a new slice with the
// type specified by typeHint.  Reflection can't make a new slice type
// just based on the key type AFAICT.
func CopyMapKeys(m interface{}, typeHint interface{}) interface{} {
	mapVal := reflect.ValueOf(m)
	keys := reflect.MakeSlice(reflect.TypeOf(typeHint), 0, mapVal.Len())
	for _, k := range mapVal.MapKeys() {
		keys = reflect.Append(keys, k)
	}
	return keys.Interface()
}

// CopyMapKeys copies values from from map m into a new slice with the
// type specified by typeHint.  Reflection can't make a new slice type
// just based on the key type AFAICT.
func CopyMapValues(m interface{}, typeHint interface{}) interface{} {
	mapVal := reflect.ValueOf(m)
	vals := reflect.MakeSlice(reflect.TypeOf(typeHint), 0, mapVal.Len())
	for _, k := range mapVal.MapKeys() {
		vals = reflect.Append(vals, mapVal.MapIndex(k))
	}
	return vals.Interface()
}

// MapKeys returns an array with th provided map keys.
func MapKeys(m interface{}) []interface{} {
	keys := make([]interface{}, 0, 16)
	mapVal := reflect.ValueOf(m)
	for _, kv := range mapVal.MapKeys() {
		keys = append(keys, kv.Interface())
	}
	return keys
}
