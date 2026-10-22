/*
Copyright 2026 The Vitess Authors.

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

package vtcombo

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"vitess.io/vitess/go/vt/grpcclient"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestTabletRegistryConcurrentLifecycle(t *testing.T) {
	tabletMapMu.Lock()
	old := tabletMap
	tabletMap = make(map[uint32]*comboTablet)
	tabletMapMu.Unlock()
	t.Cleanup(func() {
		tabletMapMu.Lock()
		tabletMap = old
		tabletMapMu.Unlock()
	})
	var workers sync.WaitGroup
	for worker := range 8 {
		workers.Go(func() {
			uid := uint32(worker + 1)
			keyspace := fmt.Sprintf("example_%d", worker)
			alias := &topodatapb.TabletAlias{Cell: "test", Uid: uid}
			tablet := &comboTablet{uid: uid, keyspace: keyspace, alias: alias}
			for range 1000 {
				registerTablet(tablet)
				conn, err := dialer(context.Background(), &topodatapb.Tablet{Alias: alias}, grpcclient.FailFast(false))
				if err != nil || conn == nil {
					t.Errorf("registered tablet unavailable: %v", err)
				}
				// Snapshot iteration must be safe while other workers mutate the map.
				for _, current := range snapshotTablets() {
					_, _ = lookupTablet(current.uid)
				}
				unregisterTablet(uid)
				if _, ok := lookupTablet(uid); ok {
					t.Error("detached tablet remains in registry")
				}
			}
		})
	}
	workers.Wait()
}
