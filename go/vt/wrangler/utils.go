// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkwrangler

import (
	"path"
	"reflect"
	"sort"
	"sync"

	"code.google.com/p/vitess/go/relog"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	"code.google.com/p/vitess/go/zk"
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
				someError = err
			} else {
				tabletMap[tabletPath] = tabletInfo
			}
			mutex.Unlock()
		}()
	}

	wg.Wait()

	mutex.Lock()
	defer mutex.Unlock()
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

	tabletMap, _ := GetTabletMap(zconn, tabletPaths)
	tablets := make([]*tm.TabletInfo, 0, len(tabletPaths))
	for _, tabletPath := range tabletPaths {
		tabletInfo, ok := tabletMap[tabletPath]
		if !ok {
			relog.Warning("failed to load tablet %v", tabletPath)
		}
		tablets = append(tablets, tabletInfo)
	}

	return tablets, nil
}

// copy keys from from map m into slice s - types must match.
func CopyMapKeys(s, m interface{}) {
	keys := reflect.ValueOf(s)
	mapVal := reflect.ValueOf(m)
	for _, kv := range mapVal.MapKeys() {
		keys = reflect.AppendSlice(keys, kv)
	}
}

// copy values from from map m into slice s - types must match.
func CopyMapValues(s, m interface{}) {
	keys := reflect.ValueOf(s)
	mapVal := reflect.ValueOf(m)
	for _, kv := range mapVal.MapKeys() {
		keys = reflect.AppendSlice(keys, mapVal.MapIndex(kv))
	}
}
