/*
Copyright 2023 The Vitess Authors.

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
	"context"
	"encoding/hex"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/faketopo"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

func TestSrvKeyspaceWithNilNewKeyspace(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	cell := "cell"
	keyspace := "testks"
	factory := faketopo.NewFakeTopoFactory()
	factory.AddCell(cell)
	ts := faketopo.NewFakeTopoServer(ctx, factory)
	ts2 := &fakeTopoServer{}
	hc := NewHealthCheck(ctx, 1*time.Millisecond, time.Hour, ts, cell, "", nil)
	defer hc.Close()
	kew := NewKeyspaceEventWatcher(ctx, ts2, hc, cell)
	kss := &keyspaceState{
		kew:      kew,
		keyspace: keyspace,
		shards:   make(map[string]*shardState),
	}
	kss.lastKeyspace = &topodatapb.SrvKeyspace{}
	require.True(t, kss.onSrvKeyspace(nil, nil))
}

// TestKeyspaceEventTypes confirms that the keyspace event watcher determines
// that the unavailability event is caused by the correct scenario. We should
// consider it to be caused by a resharding operation when the following
// conditions are present:
// 1. The keyspace is inconsistent (in the middle of an availability event)
// 2. The target tablet is a primary
// 3. The keyspace has overlapping shards
// 4. The overlapping shard's tablet is serving
// And we should consider the cause to be a primary not serving when the
// following conditions exist:
// 1. The keyspace is inconsistent (in the middle of an availability event)
// 2. The target tablet is a primary
// 3. The target tablet is not serving
// 4. The shard's externallyReparented time is not 0
// 5. The shard's currentPrimary state is not nil
// We should never consider both as a possible cause given the same
// keyspace state.
func TestKeyspaceEventTypes(t *testing.T) {
	utils.EnsureNoLeaks(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cell := "cell"
	keyspace := "testks"
	factory := faketopo.NewFakeTopoFactory()
	factory.AddCell(cell)
	ts := faketopo.NewFakeTopoServer(ctx, factory)
	ts2 := &fakeTopoServer{}
	hc := NewHealthCheck(ctx, 1*time.Millisecond, time.Hour, ts, cell, "", nil)
	defer hc.Close()
	kew := NewKeyspaceEventWatcher(ctx, ts2, hc, cell)

	type testCase struct {
		name               string
		kss                *keyspaceState
		shardToCheck       string
		expectResharding   bool
		expectShouldBuffer bool
	}

	testCases := []testCase{
		{
			name: "one to two resharding in progress",
			kss: &keyspaceState{
				kew:      kew,
				keyspace: keyspace,
				shards: map[string]*shardState{
					"-": {
						target: &querypb.Target{
							Keyspace:   keyspace,
							Shard:      "-",
							TabletType: topodatapb.TabletType_PRIMARY,
						},
						serving: false,
					},
					"-80": {
						target: &querypb.Target{
							Keyspace:   keyspace,
							Shard:      "-80",
							TabletType: topodatapb.TabletType_PRIMARY,
						},
						serving: true,
					},
					"80-": {
						target: &querypb.Target{
							Keyspace:   keyspace,
							Shard:      "80-",
							TabletType: topodatapb.TabletType_PRIMARY,
						},
						serving: false,
					},
				},
				consistent: false,
			},
			shardToCheck:       "-",
			expectResharding:   true,
			expectShouldBuffer: false,
		},
		{
			name: "two to four resharding in progress",
			kss: &keyspaceState{
				kew:      kew,
				keyspace: keyspace,
				shards: map[string]*shardState{
					"-80": {
						target: &querypb.Target{
							Keyspace:   keyspace,
							Shard:      "-80",
							TabletType: topodatapb.TabletType_PRIMARY,
						},
						serving: false,
					},
					"80-": {
						target: &querypb.Target{
							Keyspace:   keyspace,
							Shard:      "80-",
							TabletType: topodatapb.TabletType_PRIMARY,
						},
						serving: true,
					},
					"-40": {
						target: &querypb.Target{
							Keyspace:   keyspace,
							Shard:      "-40",
							TabletType: topodatapb.TabletType_PRIMARY,
						},
						serving: true,
					},
					"40-80": {
						target: &querypb.Target{
							Keyspace:   keyspace,
							Shard:      "40-80",
							TabletType: topodatapb.TabletType_PRIMARY,
						},
						serving: true,
					},
					"80-c0": {
						target: &querypb.Target{
							Keyspace:   keyspace,
							Shard:      "80-c0",
							TabletType: topodatapb.TabletType_PRIMARY,
						},
						serving: false,
					},
					"c0-": {
						target: &querypb.Target{
							Keyspace:   keyspace,
							Shard:      "c0-",
							TabletType: topodatapb.TabletType_PRIMARY,
						},
						serving: false,
					},
				},
				consistent: false,
			},
			shardToCheck:       "-80",
			expectResharding:   true,
			expectShouldBuffer: false,
		},
		{
			name: "unsharded primary not serving",
			kss: &keyspaceState{
				kew:      kew,
				keyspace: keyspace,
				shards: map[string]*shardState{
					"-": {
						target: &querypb.Target{
							Keyspace:   keyspace,
							Shard:      "-",
							TabletType: topodatapb.TabletType_PRIMARY,
						},
						serving:              false,
						externallyReparented: time.Now().UnixNano(),
						currentPrimary: &topodatapb.TabletAlias{
							Cell: cell,
							Uid:  100,
						},
					},
				},
				consistent: false,
			},
			shardToCheck:       "-",
			expectResharding:   false,
			expectShouldBuffer: true,
		},
		{
			name: "sharded primary not serving",
			kss: &keyspaceState{
				kew:      kew,
				keyspace: keyspace,
				shards: map[string]*shardState{
					"-80": {
						target: &querypb.Target{
							Keyspace:   keyspace,
							Shard:      "-80",
							TabletType: topodatapb.TabletType_PRIMARY,
						},
						serving:              false,
						externallyReparented: time.Now().UnixNano(),
						currentPrimary: &topodatapb.TabletAlias{
							Cell: cell,
							Uid:  100,
						},
					},
					"80-": {
						target: &querypb.Target{
							Keyspace:   keyspace,
							Shard:      "80-",
							TabletType: topodatapb.TabletType_PRIMARY,
						},
						serving: true,
					},
				},
				consistent: false,
			},
			shardToCheck:       "-80",
			expectResharding:   false,
			expectShouldBuffer: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kew.mu.Lock()
			kew.keyspaces[keyspace] = tc.kss
			kew.mu.Unlock()

			require.NotNil(t, tc.kss.shards[tc.shardToCheck], "the specified shardToCheck of %q does not exist in the shardState", tc.shardToCheck)

			resharding := kew.TargetIsBeingResharded(ctx, tc.kss.shards[tc.shardToCheck].target)
			require.Equal(t, resharding, tc.expectResharding, "TargetIsBeingResharded should return %t", tc.expectResharding)

			_, shouldBuffer := kew.ShouldStartBufferingForTarget(ctx, tc.kss.shards[tc.shardToCheck].target)
			require.Equal(t, shouldBuffer, tc.expectShouldBuffer, "ShouldStartBufferingForTarget should return %t", tc.expectShouldBuffer)
		})
	}
}

// TestWaitForConsistentKeyspaces tests the behaviour of WaitForConsistent for different scenarios.
func TestWaitForConsistentKeyspaces(t *testing.T) {
	testcases := []struct {
		name        string
		ksMap       map[string]*keyspaceState
		ksList      []string
		errExpected string
	}{
		{
			name:   "Empty keyspace list",
			ksList: nil,
			ksMap: map[string]*keyspaceState{
				"ks1": {},
			},
			errExpected: "",
		},
		{
			name:   "All keyspaces consistent",
			ksList: []string{"ks1", "ks2"},
			ksMap: map[string]*keyspaceState{
				"ks1": {
					consistent: true,
				},
				"ks2": {
					consistent: true,
				},
			},
			errExpected: "",
		},
		{
			name:   "One keyspace inconsistent",
			ksList: []string{"ks1", "ks2"},
			ksMap: map[string]*keyspaceState{
				"ks1": {
					consistent: true,
				},
				"ks2": {
					consistent: false,
				},
			},
			errExpected: "context canceled",
		},
		{
			name:   "One deleted keyspace - consistent",
			ksList: []string{"ks1", "ks2"},
			ksMap: map[string]*keyspaceState{
				"ks1": {
					consistent: true,
				},
				"ks2": {
					deleted: true,
				},
			},
			errExpected: "",
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			// We create a cancelable context and immediately cancel it.
			// We don't want the unit tests to wait, so we only test the first
			// iteration of whether the keyspace event watcher returns
			// that the keyspaces are consistent or not.
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			kew := KeyspaceEventWatcher{
				keyspaces: tt.ksMap,
				mu:        sync.Mutex{},
				ts:        &fakeTopoServer{},
			}
			err := kew.WaitForConsistentKeyspaces(ctx, tt.ksList)
			if tt.errExpected != "" {
				require.ErrorContains(t, err, tt.errExpected)
			} else {
				require.NoError(t, err)
			}

		})
	}
}

func TestOnHealthCheck(t *testing.T) {
	testcases := []struct {
		name                     string
		ss                       *shardState
		th                       *TabletHealth
		wantServing              bool
		wantWaitForReparent      bool
		wantExternallyReparented int64
		wantUID                  uint32
	}{
		{
			name: "Non primary tablet health ignored",
			ss: &shardState{
				serving:              false,
				waitForReparent:      false,
				externallyReparented: 10,
				currentPrimary: &topodatapb.TabletAlias{
					Cell: testCell,
					Uid:  1,
				},
			},
			th: &TabletHealth{
				Target: &querypb.Target{
					TabletType: topodatapb.TabletType_REPLICA,
				},
				Serving: true,
			},
			wantServing:              false,
			wantWaitForReparent:      false,
			wantExternallyReparented: 10,
			wantUID:                  1,
		}, {
			name: "Serving primary seen in non-serving shard",
			ss: &shardState{
				serving:              false,
				waitForReparent:      false,
				externallyReparented: 10,
				currentPrimary: &topodatapb.TabletAlias{
					Cell: testCell,
					Uid:  1,
				},
			},
			th: &TabletHealth{
				Target: &querypb.Target{
					TabletType: topodatapb.TabletType_PRIMARY,
				},
				Serving:              true,
				PrimaryTermStartTime: 20,
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: testCell,
						Uid:  2,
					},
				},
			},
			wantServing:              true,
			wantWaitForReparent:      false,
			wantExternallyReparented: 20,
			wantUID:                  2,
		}, {
			name: "New serving primary seen while waiting for reparent",
			ss: &shardState{
				serving:              false,
				waitForReparent:      true,
				externallyReparented: 10,
				currentPrimary: &topodatapb.TabletAlias{
					Cell: testCell,
					Uid:  1,
				},
			},
			th: &TabletHealth{
				Target: &querypb.Target{
					TabletType: topodatapb.TabletType_PRIMARY,
				},
				Serving:              true,
				PrimaryTermStartTime: 20,
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: testCell,
						Uid:  2,
					},
				},
			},
			wantServing:              true,
			wantWaitForReparent:      false,
			wantExternallyReparented: 20,
			wantUID:                  2,
		}, {
			name: "Old serving primary seen while waiting for reparent",
			ss: &shardState{
				serving:              false,
				waitForReparent:      true,
				externallyReparented: 10,
				currentPrimary: &topodatapb.TabletAlias{
					Cell: testCell,
					Uid:  1,
				},
			},
			th: &TabletHealth{
				Target: &querypb.Target{
					TabletType: topodatapb.TabletType_PRIMARY,
				},
				Serving:              true,
				PrimaryTermStartTime: 10,
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: testCell,
						Uid:  1,
					},
				},
			},
			wantServing:              false,
			wantWaitForReparent:      true,
			wantExternallyReparented: 10,
			wantUID:                  1,
		}, {
			name: "Old non-serving primary seen while waiting for reparent",
			ss: &shardState{
				serving:              false,
				waitForReparent:      true,
				externallyReparented: 10,
				currentPrimary: &topodatapb.TabletAlias{
					Cell: testCell,
					Uid:  1,
				},
			},
			th: &TabletHealth{
				Target: &querypb.Target{
					TabletType: topodatapb.TabletType_PRIMARY,
				},
				Serving:              false,
				PrimaryTermStartTime: 10,
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: testCell,
						Uid:  1,
					},
				},
			},
			wantServing:              false,
			wantWaitForReparent:      false,
			wantExternallyReparented: 10,
			wantUID:                  1,
		}, {
			name: "New serving primary while already serving",
			ss: &shardState{
				serving:              true,
				waitForReparent:      false,
				externallyReparented: 10,
				currentPrimary: &topodatapb.TabletAlias{
					Cell: testCell,
					Uid:  1,
				},
			},
			th: &TabletHealth{
				Target: &querypb.Target{
					TabletType: topodatapb.TabletType_PRIMARY,
				},
				Serving:              true,
				PrimaryTermStartTime: 20,
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: testCell,
						Uid:  2,
					},
				},
			},
			wantServing:              true,
			wantWaitForReparent:      false,
			wantExternallyReparented: 20,
			wantUID:                  2,
		}, {
			name: "Primary goes non serving",
			ss: &shardState{
				serving:              true,
				waitForReparent:      false,
				externallyReparented: 10,
				currentPrimary: &topodatapb.TabletAlias{
					Cell: testCell,
					Uid:  1,
				},
			},
			th: &TabletHealth{
				Target: &querypb.Target{
					TabletType: topodatapb.TabletType_PRIMARY,
				},
				Serving:              false,
				PrimaryTermStartTime: 10,
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: testCell,
						Uid:  1,
					},
				},
			},
			wantServing:              false,
			wantWaitForReparent:      false,
			wantExternallyReparented: 10,
			wantUID:                  1,
		},
	}

	ksName := "ks"
	shard := "-80"
	kss := &keyspaceState{
		mu:       sync.Mutex{},
		keyspace: ksName,
		shards:   make(map[string]*shardState),
	}
	// Adding this so that we don't run any topo calls from ensureConsistentLocked.
	kss.moveTablesState = &MoveTablesState{
		Typ:   MoveTablesRegular,
		State: MoveTablesSwitching,
	}
	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			kss.shards[shard] = tt.ss
			tt.th.Target.Keyspace = ksName
			tt.th.Target.Shard = shard
			kss.onHealthCheck(tt.th)
			require.Equal(t, tt.wantServing, tt.ss.serving)
			require.Equal(t, tt.wantWaitForReparent, tt.ss.waitForReparent)
			require.Equal(t, tt.wantExternallyReparented, tt.ss.externallyReparented)
			require.Equal(t, tt.wantUID, tt.ss.currentPrimary.Uid)
		})
	}
}

type fakeTopoServer struct {
}

// GetTopoServer returns the full topo.Server instance.
func (f *fakeTopoServer) GetTopoServer() (*topo.Server, error) {
	return nil, nil
}

// GetSrvKeyspaceNames returns the list of keyspaces served in
// the provided cell.
func (f *fakeTopoServer) GetSrvKeyspaceNames(ctx context.Context, cell string, staleOK bool) ([]string, error) {
	return []string{"ks1"}, nil
}

// GetSrvKeyspace returns the SrvKeyspace for a cell/keyspace.
func (f *fakeTopoServer) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	zeroHexBytes, _ := hex.DecodeString("")
	eightyHexBytes, _ := hex.DecodeString("80")
	ks := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_PRIMARY,
				ShardReferences: []*topodatapb.ShardReference{
					{Name: "-80", KeyRange: &topodatapb.KeyRange{Start: zeroHexBytes, End: eightyHexBytes}},
					{Name: "80-", KeyRange: &topodatapb.KeyRange{Start: eightyHexBytes, End: zeroHexBytes}},
				},
			},
		},
	}
	return ks, nil
}

func (f *fakeTopoServer) WatchSrvKeyspace(ctx context.Context, cell, keyspace string, callback func(*topodatapb.SrvKeyspace, error) bool) {
	ks, err := f.GetSrvKeyspace(ctx, cell, keyspace)
	callback(ks, err)
}

// WatchSrvVSchema starts watching the SrvVSchema object for
// the provided cell.  It will call the callback when
// a new value or an error occurs.
func (f *fakeTopoServer) WatchSrvVSchema(ctx context.Context, cell string, callback func(*vschemapb.SrvVSchema, error) bool) {

}
