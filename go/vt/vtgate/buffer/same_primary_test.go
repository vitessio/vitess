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

package buffer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// singleShardSrvTopo is a srvtopo.Server that serves keyspace, with its one shard, in every cell.
type singleShardSrvTopo struct{}

func (singleShardSrvTopo) GetTopoServer() (*topo.Server, error) { return nil, nil }

func (singleShardSrvTopo) GetSrvKeyspaceNames(context.Context, string, bool) ([]string, error) {
	return []string{keyspace}, nil
}

func (singleShardSrvTopo) GetSrvKeyspace(context.Context, string, string) (*topodatapb.SrvKeyspace, error) {
	return &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{{
			ServedType:      topodatapb.TabletType_PRIMARY,
			ShardReferences: []*topodatapb.ShardReference{{Name: shard}},
		}},
	}, nil
}

func (s singleShardSrvTopo) WatchSrvKeyspace(ctx context.Context, cell, ks string, callback func(*topodatapb.SrvKeyspace, error) bool) {
	callback(s.GetSrvKeyspace(ctx, cell, ks))
}

func (singleShardSrvTopo) WatchSrvVSchema(context.Context, string, func(*vschemapb.SrvVSchema, error) bool) {
}

// TestBufferingEndsWhenSamePrimaryServesAgain pins how vtgate ends a buffering for a primary that
// stops serving and serves again with the same primary term, as a Group Replication migration
// step does (doc/design-docs/GroupReplication.md, "The migration's pauses and vtgate's buffer"):
// the buffering, which waits for a reparent, ends as soon as the keyspace event watcher sees a
// not-serving health check of the primary after the buffering started, and then a serving one.
// The buffer's maximum duration is longer than the test's wait, so only the health checks can end
// it.
func TestBufferingEndsWhenSamePrimaryServesAgain(t *testing.T) {
	primaryHealth := func(serving bool) *discovery.TabletHealth {
		return &discovery.TabletHealth{
			Tablet:               oldPrimary,
			Target:               &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_PRIMARY},
			Serving:              serving,
			PrimaryTermStartTime: 1000,
		}
	}
	// notServingErr is what the primary returns while it does not serve.
	notServingErr := vterrors.New(vtrpcpb.Code_CLUSTER_EVENT, "operation not allowed in state NOT_SERVING")
	// reparentErr is what the tablet gateway buffers with when it finds no serving primary.
	reparentErr := vterrors.New(vtrpcpb.Code_CLUSTER_EVENT, ClusterEventReparentInProgress)

	testCases := []struct {
		name string
		// run makes the buffering start and the primary serve again, through the health checks
		// sent to hcCh and the requests it issues.
		run func(t *testing.T, b *Buffer, kew *discovery.KeyspaceEventWatcher, hcCh chan *discovery.TabletHealth) chan error
	}{{
		// A request reached the primary after it stopped serving, before vtgate saw it.
		name: "buffering started by the primary's error",
		run: func(t *testing.T, b *Buffer, kew *discovery.KeyspaceEventWatcher, hcCh chan *discovery.TabletHealth) chan error {
			stopped := issueRequestWithWatcher(t, b, kew, notServingErr)
			require.NoError(t, waitForRequestsInFlight(b, 1))
			hcCh <- primaryHealth(false)
			hcCh <- primaryHealth(true)
			return stopped
		},
	}, {
		// vtgate saw the primary stop serving first: the buffering starts after it, and waits for
		// a reparent. The primary announces again that it does not serve before it serves.
		name: "buffering started after the not-serving health check",
		run: func(t *testing.T, b *Buffer, kew *discovery.KeyspaceEventWatcher, hcCh chan *discovery.TabletHealth) chan error {
			hcCh <- primaryHealth(false)
			target := &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_PRIMARY}
			require.Eventually(t, func() bool {
				_, shouldBuffer := kew.ShouldStartBufferingForTarget(t.Context(), target)
				return shouldBuffer
			}, 30*time.Second, time.Millisecond)
			stopped := issueRequestWithWatcher(t, b, kew, reparentErr)
			require.NoError(t, waitForRequestsInFlight(b, 1))
			hcCh <- primaryHealth(false)
			hcCh <- primaryHealth(true)
			return stopped
		},
	}}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resetVariables()
			t.Cleanup(func() { checkVariables(t) })
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)

			hcCh := make(chan *discovery.TabletHealth, 10)
			hc := discovery.NewFakeHealthCheck(hcCh)
			kew := discovery.NewKeyspaceEventWatcher(ctx, singleShardSrvTopo{}, hc, "cell1")
			events := kew.Subscribe()
			t.Cleanup(func() { kew.Unsubscribe(events) })

			// The primary serves: the keyspace is consistent.
			hcCh <- primaryHealth(true)
			select {
			case ev := <-events:
				require.True(t, ev.Shards[0].Serving)
			case <-time.After(30 * time.Second):
				require.FailNow(t, "the keyspace did not become consistent")
			}

			cfg := NewDefaultConfig()
			cfg.Enabled = true
			cfg.Shards = map[string]bool{keyspace + "/" + shard: true}
			cfg.MaxFailoverDuration = 5 * time.Minute
			cfg.Window = 5 * time.Minute
			b := New(cfg)
			t.Cleanup(b.Shutdown)
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case ev := <-events:
						b.HandleKeyspaceEvent(ev)
					}
				}
			}()

			stopped := tc.run(t, b, kew, hcCh)
			select {
			case err := <-stopped:
				require.NoError(t, err)
			case <-time.After(30 * time.Second):
				require.FailNow(t, "the buffering did not end when the primary served again")
			}
			assert.Equal(t, int64(1), stops.Counts()[statsKeyJoinedFailoverEndDetected], "the buffering ends because the primary serves")
			require.NoError(t, waitForState(b, stateIdle))
		})
	}
}

// issueRequestWithWatcher is issueRequest with the keyspace event watcher of vtgate, which the
// buffer marks the shard not serving in when it starts buffering.
func issueRequestWithWatcher(t *testing.T, b *Buffer, kew *discovery.KeyspaceEventWatcher, err error) chan error {
	stopped := make(chan error, 1)
	go func() {
		retryDone, err := b.WaitForFailoverEnd(t.Context(), keyspace, shard, kew, err)
		if retryDone != nil {
			retryDone()
		}
		stopped <- err
	}()
	return stopped
}
