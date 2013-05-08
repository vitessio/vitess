// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"path"
	"reflect"
	"sort"
	"sync"

	"code.google.com/p/vitess/go/relog"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

// If error is not nil, the results in the dictionary are incomplete.
func GetTabletMap(zconn zk.Conn, tabletPaths []string) (map[string]*tm.TabletInfo, error) {
	wg := sync.WaitGroup{}
	mutex := sync.Mutex{}

	tabletMap := make(map[string]*tm.TabletInfo)
	var someError error

	for _, path := range tabletPaths {
		tabletPath := path
		wg.Add(1)
		go func() {
			defer wg.Done()
			tabletInfo, err := tm.ReadTablet(zconn, tabletPath)
			mutex.Lock()
			if err != nil {
				relog.Warning("%v: %v", tabletPath, err)
				// There can be data races removing nodes - ignore them for now.
				if !zookeeper.IsError(err, zookeeper.ZNONODE) {
					someError = err
				}
			} else {
				tabletMap[tabletPath] = tabletInfo
			}
			mutex.Unlock()
		}()
	}
	wg.Wait()
	return tabletMap, someError
}

// If error is not nil, the results in the dictionary are incomplete.
func GetTabletMapForShard(zconn zk.Conn, zkShardPath string) (map[string]*tm.TabletInfo, error) {
	aliases, err := tm.FindAllTabletAliasesInShard(zconn, zkShardPath)
	if err != nil {
		return nil, err
	}

	shardTablets := make([]string, len(aliases))
	for i, alias := range aliases {
		shardTablets[i] = tm.TabletPathForAlias(alias)
	}

	return GetTabletMap(zconn, shardTablets)
}

// Return a sorted list of tablets.
func GetAllTablets(zconn zk.Conn, zkVtPath string) ([]*tm.TabletInfo, error) {
	zkTabletsPath := path.Join(zkVtPath, "tablets")
	children, _, err := zconn.Children(zkTabletsPath)
	if err != nil {
		return nil, err
	}

	sort.Strings(children)
	tabletPaths := make([]string, len(children))
	for i, child := range children {
		tabletPaths[i] = path.Join(zkTabletsPath, child)
	}

	tabletMap, err := GetTabletMap(zconn, tabletPaths)
	if err != nil {
		// we got another error than ZNONODE
		return nil, err
	}
	tablets := make([]*tm.TabletInfo, 0, len(tabletPaths))
	for _, tabletPath := range tabletPaths {
		tabletInfo, ok := tabletMap[tabletPath]
		if !ok {
			// tablet disappeared on us (GetTabletMap ignores
			// ZNONODE), just echo a warning
			relog.Warning("failed to load tablet %v", tabletPath)
		} else {
			tablets = append(tablets, tabletInfo)
		}
	}

	return tablets, nil
}

// GetAllTabletsAccrossCells returns all tablets from known cells.
func GetAllTabletsAccrossCells(zconn zk.Conn) ([]*tm.TabletInfo, error) {
	cells, err := zk.ResolveWildcards(zconn, []string{"/zk/*/vt/tablets"})
	if err != nil {
		return nil, err
	}
	results := make(chan []*tm.TabletInfo)
	errors := make(chan error)
	for _, cell := range cells {
		go func(cell string) {
			tablets, err := GetAllTablets(zconn, path.Dir(cell))
			if err != nil {
				errors <- err
				return
			}
			results <- tablets
		}(cell)
	}

	allTablets := make([]*tm.TabletInfo, 0)
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
