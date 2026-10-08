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

package tabletmanager

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/semisyncmonitor"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type consolidatingFullStatusDaemon struct {
	mysqlctl.MysqlDaemon
	collect func(context.Context) (*replicationdatapb.FullStatus, error)
	calls   atomic.Int32
}

func (d *consolidatingFullStatusDaemon) CollectFullStatusData(ctx context.Context) (*replicationdatapb.FullStatus, error) {
	d.calls.Add(1)
	return d.collect(ctx)
}

type fullStatusController struct {
	tabletserver.Controller
	diskStalled atomic.Bool
}

func (c *fullStatusController) IsDiskStalled() bool {
	return c.diskStalled.Load()
}

func newConsolidatingFullStatusTM(t *testing.T, daemon *consolidatingFullStatusDaemon) *TabletManager {
	t.Helper()
	tm := newTestReplicationTM(newTestTablet(t, 100, "ks", "0", nil), daemon, nil)
	tm.BatchCtx = t.Context()
	tm.QueryServiceControl = &fullStatusController{}
	tm.SemiSyncMonitor = &semisyncmonitor.Monitor{}
	return tm
}

func TestFullStatusConsolidatesConcurrentCalls(t *testing.T) {
	for _, outcome := range []string{"success", "error", "panic"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				collected := &replicationdatapb.FullStatus{
					ServerId:          42,
					ReplicationStatus: &replicationdatapb.Status{SourceHost: "source"},
				}
				collectorErr := vterrors.New(vtrpc.Code_UNAVAILABLE, "collector failed")
				daemon := &consolidatingFullStatusDaemon{
					collect: func(context.Context) (*replicationdatapb.FullStatus, error) {
						<-release
						switch outcome {
						case "error":
							return nil, collectorErr
						case "panic":
							panic("collector panicked")
						default:
							return collected.CloneVT(), nil
						}
					},
				}
				tm := newConsolidatingFullStatusTM(t, daemon)
				const callers = 10
				statuses := make([]*replicationdatapb.FullStatus, callers)
				errs := make([]error, callers)
				for i := range callers {
					go func() {
						assert.NotPanics(t, func() {
							statuses[i], errs[i] = tm.FullStatus(t.Context())
						})
					}()
				}

				// All callers must have joined before the collection is released.
				synctest.Wait()
				assert.EqualValues(t, 1, daemon.calls.Load())

				// A stalled disk must bypass even an already-running collection.
				if outcome == "success" {
					controller := tm.QueryServiceControl.(*fullStatusController)
					controller.diskStalled.Store(true)
					stalled, err := tm.FullStatus(t.Context())
					require.NoError(t, err)
					assert.True(t, stalled.DiskStalled)
					controller.diskStalled.Store(false)
				}

				close(release)
				synctest.Wait()
				for i := range callers {
					switch outcome {
					case "error":
						require.ErrorIs(t, errs[i], collectorErr)
						assert.Equal(t, vtrpc.Code_UNAVAILABLE, vterrors.Code(errs[i]))
						assert.Nil(t, statuses[i])
					case "panic":
						require.ErrorContains(t, errs[i], "collector panicked")
						assert.Equal(t, vtrpc.Code_INTERNAL, vterrors.Code(errs[i]))
						assert.NotContains(t, errs[i].Error(), ".go:", "stack must stay server-side")
						assert.Nil(t, statuses[i])
					default:
						require.NoError(t, errs[i])
						require.NotNil(t, statuses[i])
						assert.Equal(t, collected.ServerId, statuses[i].ServerId)
						assert.Equal(t, topodatapb.TabletType_REPLICA, statuses[i].TabletType)
						assert.Equal(t, "source", statuses[i].ReplicationStatus.SourceHost)
					}
				}
				if outcome == "success" {
					statuses[0].ReplicationStatus.SourceHost = "changed"
					for _, status := range statuses[1:] {
						assert.Equal(t, "source", status.ReplicationStatus.SourceHost)
					}
				}

				// Neither successful results nor failures are cached.
				daemon.collect = func(context.Context) (*replicationdatapb.FullStatus, error) {
					return &replicationdatapb.FullStatus{ServerId: 43}, nil
				}
				status, err := tm.FullStatus(t.Context())
				require.NoError(t, err)
				assert.Equal(t, uint32(43), status.ServerId)
				assert.EqualValues(t, 2, daemon.calls.Load())
			})
		})
	}
}

func TestFullStatusAfterTabletAction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		beforeAction := make(chan struct{})
		afterAction := make(chan struct{})
		mysqlDaemon := &mysqlctl.FakeMysqlDaemon{ReadOnly: true}
		daemon := &consolidatingFullStatusDaemon{
			MysqlDaemon: mysqlDaemon,
			collect: func(ctx context.Context) (*replicationdatapb.FullStatus, error) {
				readOnly, err := mysqlDaemon.IsReadOnly(ctx)
				if err != nil {
					return nil, err
				}
				if readOnly {
					<-beforeAction
				} else {
					<-afterAction
				}
				return &replicationdatapb.FullStatus{ReadOnly: readOnly}, nil
			},
		}
		tm := newConsolidatingFullStatusTM(t, daemon)
		var statuses [4]*replicationdatapb.FullStatus
		var errs [4]error
		startCall := func(i int) {
			go func() {
				statuses[i], errs[i] = tm.FullStatus(t.Context())
			}()
			synctest.Wait()
		}

		startCall(0)
		startCall(1)
		assert.EqualValues(t, 1, daemon.calls.Load())

		// Revalidation after a completed action must not join a pre-action snapshot.
		require.NoError(t, tm.SetReadOnly(t.Context(), false))
		startCall(2)
		assert.EqualValues(t, 2, daemon.calls.Load())

		close(beforeAction)
		synctest.Wait()
		// Finishing the old collection must not remove the newer in-flight one.
		startCall(3)
		assert.EqualValues(t, 2, daemon.calls.Load())
		close(afterAction)
		synctest.Wait()

		for i, status := range statuses {
			require.NoError(t, errs[i])
			require.NotNil(t, status)
			assert.Equal(t, i < 2, status.ReadOnly)
		}
	})
}

func TestFullStatusCancellationDoesNotCancelOtherCallers(t *testing.T) {
	t.Run("already cancelled", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			daemon := &consolidatingFullStatusDaemon{
				collect: func(context.Context) (*replicationdatapb.FullStatus, error) {
					return &replicationdatapb.FullStatus{}, nil
				},
			}
			tm := newConsolidatingFullStatusTM(t, daemon)
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			cancel()
			// Both the grants and cancellation channels are ready, so exercise both select paths.
			for range 100 {
				status, err := tm.FullStatus(ctx)
				require.ErrorIs(t, err, context.Canceled)
				assert.Nil(t, status)
			}
			synctest.Wait()
			assert.Zero(t, daemon.calls.Load())
		})
	})

	for _, cancelledCaller := range []int{0, 1} {
		name := []string{"first caller", "follower"}[cancelledCaller]
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				var collectionCtx context.Context
				daemon := &consolidatingFullStatusDaemon{
					collect: func(ctx context.Context) (*replicationdatapb.FullStatus, error) {
						collectionCtx = ctx
						select {
						case <-release:
							return &replicationdatapb.FullStatus{ServerId: 42}, nil
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					},
				}
				tm := newConsolidatingFullStatusTM(t, daemon)
				cancelledCtx, cancel := context.WithCancel(t.Context())
				t.Cleanup(cancel)
				var statuses [2]*replicationdatapb.FullStatus
				var errs [2]error
				var done [2]bool
				for i := range 2 {
					ctx := t.Context()
					if i == cancelledCaller {
						ctx = cancelledCtx
					}
					go func() {
						statuses[i], errs[i] = tm.FullStatus(ctx)
						done[i] = true
					}()
					// Start the first collection before the follower joins it.
					synctest.Wait()
				}

				cancel()
				synctest.Wait()
				assert.True(t, done[cancelledCaller])
				require.ErrorIs(t, errs[cancelledCaller], context.Canceled)
				assert.Nil(t, statuses[cancelledCaller])
				assert.False(t, done[1-cancelledCaller])
				require.NoError(t, collectionCtx.Err())
				assert.EqualValues(t, 1, daemon.calls.Load())

				close(release)
				synctest.Wait()
				require.NoError(t, errs[1-cancelledCaller])
				require.NotNil(t, statuses[1-cancelledCaller])
				assert.Equal(t, uint32(42), statuses[1-cancelledCaller].ServerId)
			})
		})
	}
}

func TestFullStatusSharedCollectionLifetime(t *testing.T) {
	for _, stop := range []string{"timeout", "shutdown"} {
		t.Run(stop, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				finished := make(chan error, 1)
				daemon := &consolidatingFullStatusDaemon{
					collect: func(ctx context.Context) (*replicationdatapb.FullStatus, error) {
						<-ctx.Done()
						finished <- ctx.Err()
						return nil, ctx.Err()
					},
				}
				tm := newConsolidatingFullStatusTM(t, daemon)
				batchCtx, shutdown := context.WithCancel(t.Context())
				t.Cleanup(shutdown)
				tm.BatchCtx = batchCtx
				ctx, cancel := context.WithCancel(t.Context())
				t.Cleanup(cancel)
				var rpcErr error
				go func() {
					_, rpcErr = tm.FullStatus(ctx)
				}()
				synctest.Wait()
				cancel()
				synctest.Wait()
				require.ErrorIs(t, rpcErr, context.Canceled)
				assert.Empty(t, finished, "shared collection outlives the caller")

				wantErr := context.DeadlineExceeded
				start := time.Now()
				if stop == "shutdown" {
					shutdown()
					wantErr = context.Canceled
				}
				require.ErrorIs(t, <-finished, wantErr)
				if stop == "timeout" {
					assert.Equal(t, topo.RemoteOperationTimeout, time.Since(start))
				}
			})
		})
	}
}
