/*
Copyright 2017 Google Inc.

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

package topotools

import (
	"reflect"
	"sort"
	"sync"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// FindTabletByHostAndPort searches within a tablet map for tablets.
func FindTabletByHostAndPort(tabletMap map[string]*topo.TabletInfo, addr, portName string, port int32) (*topodatapb.TabletAlias, error) {
	for _, ti := range tabletMap {
		if ti.Hostname == addr && ti.PortMap[portName] == port {
			return ti.Alias, nil
		}
	}
	return nil, topo.NewError(topo.NoNode, addr+":"+portName)
}

// GetAllTablets returns a sorted list of tablets.
func GetAllTablets(ctx context.Context, ts *topo.Server, cell string) ([]*topo.TabletInfo, error) {
	aliases, err := ts.GetTabletsByCell(ctx, cell)
	if err != nil {
		return nil, err
	}
	sort.Sort(topoproto.TabletAliasList(aliases))

	tabletMap, err := ts.GetTabletMap(ctx, aliases)
	if err != nil {
		// we got another error than topo.ErrNoNode
		return nil, err
	}
	tablets := make([]*topo.TabletInfo, 0, len(aliases))
	for _, tabletAlias := range aliases {
		tabletInfo, ok := tabletMap[topoproto.TabletAliasString(tabletAlias)]
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
func GetAllTabletsAcrossCells(ctx context.Context, ts *topo.Server) ([]*topo.TabletInfo, error) {
	cells, err := ts.GetKnownCells(ctx)
	if err != nil {
		return nil, err
	}

	results := make([][]*topo.TabletInfo, len(cells))
	errors := make([]error, len(cells))
	wg := sync.WaitGroup{}
	wg.Add(len(cells))
	for i, cell := range cells {
		go func(i int, cell string) {
			results[i], errors[i] = GetAllTablets(ctx, ts, cell)
			wg.Done()
		}(i, cell)
	}
	wg.Wait()

	err = nil
	var allTablets []*topo.TabletInfo
	for i := range cells {
		if errors[i] == nil {
			allTablets = append(allTablets, results[i]...)
		} else {
			err = topo.NewError(topo.PartialResult, "")
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
func SortedTabletMap(tabletMap map[string]*topo.TabletInfo) (map[string]*topo.TabletInfo, map[string]*topo.TabletInfo) {
	slaveMap := make(map[string]*topo.TabletInfo)
	masterMap := make(map[string]*topo.TabletInfo)
	for alias, ti := range tabletMap {
		if ti.Type == topodatapb.TabletType_MASTER {
			masterMap[alias] = ti
		} else {
			slaveMap[alias] = ti
		}
	}
	return slaveMap, masterMap
}

// CopyMapKeys copies keys from map m into a new slice with the
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

// CopyMapValues copies values from map m into a new slice with the
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
