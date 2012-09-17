// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkwrangler

import (
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
