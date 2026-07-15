/*
Copyright 2025 The Vitess Authors.

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

package vstreamclient

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/vstreamclient"
)

// TestVStreamClientGracefulShutdownChanStopsActiveRun verifies a configured
// shutdown channel can trigger GracefulShutdown without a manual call.
func TestVStreamClientGracefulShutdownChanStopsActiveRun(t *testing.T) {
	te := newTestEnv(t)

	rowSeen := make(chan struct{}, 1)
	shutdownCh := make(chan struct{})
	vstreamClient := te.newDefaultClient(
		t, t.Name(), []vstreamclient.TableConfig{{
			Keyspace:        "customer",
			Table:           "customer",
			Query:           "select * from customer where id between 2600 and 2699",
			MaxRowsPerFlush: 10,
			DataType:        &Customer{},
			FlushFn:         func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error { return nil },
		}},
		vstreamclient.WithMinFlushDuration(10*time.Second),
		vstreamclient.WithHeartbeatSeconds(5),
		vstreamclient.WithGracefulShutdownChan(shutdownCh, 6*time.Second),
		vstreamclient.WithEventFunc(func(_ context.Context, _ *binlogdatapb.VEvent) error {
			select {
			case rowSeen <- struct{}{}:
			default:
			}
			return nil
		}, binlogdatapb.VEventType_ROW),
	)

	te.exec(t, "insert into customer.customer(id, email) values (2601, 'graceful-chan@domain.com')", nil)

	runCtx, cancelRun, runErrCh := te.runAsync(vstreamClient, 30*time.Second)
	recvOrFail(t, rowSeen, "row event")
	close(shutdownCh)

	err := <-runErrCh
	if !isExpectedRunStop(err, runCtx) && !errors.Is(err, context.Canceled) {
		require.NoError(t, err, "failed to run vstreamclient")
	}
	cancelRun()
	err = vstreamClient.Run(t.Context())
	require.Error(t, err)
	assert.ErrorContains(t, err, "client is closed")
	_ = runCtx
}

// TestVStreamClientGracefulShutdownChanStopsOnThresholdFlush verifies shutdown
// completes on the next safe flush boundary even when that flush was already
// going to happen because MaxRowsPerFlush was reached.
func TestVStreamClientGracefulShutdownChanStopsOnThresholdFlush(t *testing.T) {
	te := newTestEnv(t)

	rowSeen := make(chan struct{}, 1)
	shutdownCh := make(chan struct{})
	vstreamClient := te.newDefaultClient(
		t, t.Name(), []vstreamclient.TableConfig{{
			Keyspace:        "customer",
			Table:           "customer",
			Query:           "select * from customer where id between 2650 and 2699",
			MaxRowsPerFlush: 1,
			DataType:        &Customer{},
			FlushFn:         func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error { return nil },
		}},
		vstreamclient.WithMinFlushDuration(10*time.Second),
		vstreamclient.WithHeartbeatSeconds(5),
		vstreamclient.WithGracefulShutdownChan(shutdownCh, 5*time.Second),
		vstreamclient.WithEventFunc(func(_ context.Context, _ *binlogdatapb.VEvent) error {
			select {
			case rowSeen <- struct{}{}:
			default:
			}
			return nil
		}, binlogdatapb.VEventType_ROW),
	)

	te.exec(t, "insert into customer.customer(id, email) values (2651, 'threshold-shutdown@domain.com')", nil)

	runCtx, cancelRun, runErrCh := te.runAsync(vstreamClient, 30*time.Second)
	defer cancelRun()
	recvOrFail(t, rowSeen, "row event")
	close(shutdownCh)

	err := <-runErrCh
	assert.NotErrorIs(t, runCtx.Err(), context.DeadlineExceeded)
	if err != nil && !errors.Is(err, context.Canceled) {
		require.NoError(t, err, "failed to run vstreamclient")
	}
}

// TestVStreamClientIgnoresNoOpTransactions verifies transactions in the streamed keyspace whose
// rows are all filtered out (here: a write to a table the stream doesn't match) still stream
// BEGIN/VGTID/COMMIT events but never invoke FlushFn. The sentinel row is written after the no-op
// transaction, so binlog order guarantees that once the sentinel arrives, any phantom delivery
// from the no-op transaction would already have been observed.
func TestVStreamClientIgnoresNoOpTransactions(t *testing.T) {
	te := newTestEnv(t)

	newClient := func(flushFn vstreamclient.FlushFunc) *vstreamclient.VStreamClient {
		return te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
			Keyspace:        "customer",
			Table:           "customer",
			Query:           "select * from customer where id between 2900 and 2999",
			MaxRowsPerFlush: 10,
			DataType:        &Customer{},
			FlushFn:         flushFn,
		}})
	}

	// complete the copy phase first, so the writes below arrive through streaming
	te.runUntilCopyCompleted(t, newClient(func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error { return nil }), t.Name())

	var got []*Customer
	var flushCalls atomic.Int32
	var sentinelSeen atomic.Bool
	sentinel := &Customer{ID: 2902, Email: "noop-sentinel@domain.com"}
	client := newClient(func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
		flushCalls.Add(1)
		for _, row := range rows {
			customer := row.Data.(*Customer)
			got = append(got, customer)
			if customer.ID == sentinel.ID {
				sentinelSeen.Store(true)
			}
		}
		return nil
	})

	// a no-op transaction for this stream: same keyspace, but a table the filter doesn't match
	te.exec(t, "insert into customer.purchases(id, customer_id, note) values (2901, 2901, 'noop-tx')", nil)
	te.exec(t, "insert into customer.customer(id, email) values(:id, :email)", customerBindVars(sentinel.ID, sentinel.Email))

	te.runUntil(t, client, sentinelSeen.Load)

	assert.Equal(t, []*Customer{sentinel}, got)
	assert.Equal(t, int32(1), flushCalls.Load())
}

// TestVStreamClientGracefulShutdownClosesMultiTableClient verifies
// GracefulShutdown can stop a multi-table stream after both tables have started
// emitting rows, and that the client is closed afterward.
func TestVStreamClientGracefulShutdownClosesMultiTableClient(t *testing.T) {
	te := newTestEnv(t)

	var rowTables []string
	rowTableSeen := make(chan string, 4)
	newClient := func() *vstreamclient.VStreamClient {
		return te.newDefaultClient(
			t, t.Name(), []vstreamclient.TableConfig{
				{
					Keyspace:        "customer",
					Table:           "customer",
					Query:           "select * from customer where id between 1950 and 1959",
					MaxRowsPerFlush: 100,
					DataType:        &Customer{},
					FlushFn:         func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error { return nil },
				},
				{
					Keyspace:        "customer",
					Table:           "purchases",
					Query:           "select * from purchases where id between 1950 and 1959",
					MaxRowsPerFlush: 100,
					DataType:        &Order{},
					FlushFn:         func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error { return nil },
				},
			},
			vstreamclient.WithMinFlushDuration(10*time.Second),
			vstreamclient.WithHeartbeatSeconds(5),
			vstreamclient.WithEventFunc(func(_ context.Context, ev *binlogdatapb.VEvent) error {
				rowTables = append(rowTables, ev.RowEvent.TableName)
				select {
				case rowTableSeen <- ev.RowEvent.TableName:
				default:
				}
				return nil
			}, binlogdatapb.VEventType_ROW),
		)
	}

	te.exec(t, "insert into customer.customer(id, email) values (1950, 'close-prime@domain.com')", nil)
	te.exec(t, "insert into customer.purchases(id, customer_id, note) values (1950, 1950, 'close-prime-order')", nil)

	vstreamClient := newClient()
	_, cancelRun, runErrCh := te.runAsync(vstreamClient, 30*time.Second)
	seen := map[string]bool{}
	deadline := time.After(30 * time.Second)
	for !(seen["customer.customer"] && seen["customer.purchases"]) {
		select {
		case tableName := <-rowTableSeen:
			seen[tableName] = true
		case <-deadline:
			require.FailNow(t, "timed out waiting for both prime row event tables")
		}
	}

	vstreamClient.GracefulShutdown(6 * time.Second)
	cancelRun()
	err := <-runErrCh
	if !isExpectedRunStop(err, nil) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		require.NoError(t, err, "failed to run vstreamclient")
	}

	err = vstreamClient.Run(t.Context())
	require.Error(t, err)
	assert.ErrorContains(t, err, "client is closed")
	assert.ElementsMatch(t, []string{"customer.customer", "customer.purchases"}, rowTables)
}

// replayAfterShutdown inserts a sentinel row and runs a fresh client until the sentinel arrives.
// Binlog order guarantees the tested row would be delivered before the sentinel, so once the
// sentinel shows up, whether the tested row replayed is already decided — no timing window needed
// for the negative cases.
func replayAfterShutdown(t *testing.T, te *testEnv, newClient func(int, vstreamclient.FlushFunc, ...vstreamclient.Option) *vstreamclient.VStreamClient, sentinel *Customer) []*Customer {
	t.Helper()

	te.exec(t, "insert into customer.customer(id, email) values(:id, :email)", customerBindVars(sentinel.ID, sentinel.Email))

	var replayed []*Customer
	var sentinelSeen atomic.Bool
	client := newClient(1, func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
		for _, row := range rows {
			customer := row.Data.(*Customer)
			replayed = append(replayed, customer)
			if customer.ID == sentinel.ID {
				sentinelSeen.Store(true)
			}
		}
		return nil
	})

	te.runUntil(t, client, sentinelSeen.Load)
	return replayed
}

func TestVStreamClientGracefulShutdownReplayMatrix(t *testing.T) {
	testCases := []struct {
		name       string
		streamName string
		id         int64
		run        func(t *testing.T, te *testEnv, newClient func(int, vstreamclient.FlushFunc, ...vstreamclient.Option) *vstreamclient.VStreamClient, want, sentinel *Customer)
	}{
		{
			name:       "before safe boundary replays",
			streamName: "shutdown_before_boundary",
			id:         3001,
			run: func(t *testing.T, te *testEnv, newClient func(int, vstreamclient.FlushFunc, ...vstreamclient.Option) *vstreamclient.VStreamClient, want, sentinel *Customer) {
				var client *vstreamclient.VStreamClient
				flushCount := 0
				client = newClient(
					10, func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
						flushCount += len(rows)
						return nil
					},
					vstreamclient.WithEventFunc(func(_ context.Context, _ *binlogdatapb.VEvent) error {
						// the hook runs on the event loop, so this GracefulShutdown call blocks
						// event processing until the wait expires; the cancel therefore always
						// lands before the transaction's COMMIT can trigger a flush
						client.GracefulShutdown(100 * time.Millisecond)
						return nil
					}, binlogdatapb.VEventType_ROW),
				)

				runCtx, cancelRun, runErrCh := te.runAsync(client, 30*time.Second)
				defer cancelRun()
				te.exec(t, "insert into customer.customer(id, email) values(:id, :email)", customerBindVars(want.ID, want.Email))

				err := <-runErrCh
				require.Error(t, err)
				assert.ErrorIs(t, err, context.Canceled)
				assert.Zero(t, flushCount)

				replayed := replayAfterShutdown(t, te, newClient, sentinel)
				assert.Equal(t, []*Customer{want, sentinel}, replayed)
				_ = runCtx
			},
		},
		{
			name:       "after safe boundary does not replay",
			streamName: "shutdown_after_boundary",
			id:         3011,
			run: func(t *testing.T, te *testEnv, newClient func(int, vstreamclient.FlushFunc, ...vstreamclient.Option) *vstreamclient.VStreamClient, want, sentinel *Customer) {
				flushSeen := make(chan struct{}, 1)
				var active []*Customer
				client := newClient(1, func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
					for _, row := range rows {
						active = append(active, row.Data.(*Customer))
					}
					select {
					case flushSeen <- struct{}{}:
					default:
					}
					return nil
				})

				runCtx, cancelRun, runErrCh := te.runAsync(client, 30*time.Second)
				defer cancelRun()
				te.exec(t, "insert into customer.customer(id, email) values(:id, :email)", customerBindVars(want.ID, want.Email))

				recvOrFail(t, flushSeen, "flush")
				go client.GracefulShutdown(15 * time.Second)

				err := <-runErrCh
				if err != nil && !errors.Is(err, context.Canceled) && !isExpectedRunStop(err, runCtx) {
					require.NoError(t, err, "failed to run vstreamclient")
				}
				assert.Equal(t, []*Customer{want}, active)

				replayed := replayAfterShutdown(t, te, newClient, sentinel)
				assert.Equal(t, []*Customer{sentinel}, replayed)
			},
		},
		{
			name:       "during slow flush does not replay",
			streamName: "shutdown_slow_flush",
			id:         3021,
			run: func(t *testing.T, te *testEnv, newClient func(int, vstreamclient.FlushFunc, ...vstreamclient.Option) *vstreamclient.VStreamClient, want, sentinel *Customer) {
				flushStarted := make(chan struct{}, 1)
				flushGate := make(chan struct{})
				var client *vstreamclient.VStreamClient
				var active []*Customer
				client = newClient(1, func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
					select {
					case flushStarted <- struct{}{}:
					default:
					}
					// block the flush until the test has issued GracefulShutdown, so the
					// shutdown request lands while this flush is still in progress
					<-flushGate
					for _, row := range rows {
						active = append(active, row.Data.(*Customer))
					}
					return nil
				})

				runCtx, cancelRun, runErrCh := te.runAsync(client, 30*time.Second)
				defer cancelRun()
				te.exec(t, "insert into customer.customer(id, email) values(:id, :email)", customerBindVars(want.ID, want.Email))

				recvOrFail(t, flushStarted, "slow flush start")
				go client.GracefulShutdown(15 * time.Second)
				// give the GracefulShutdown goroutine time to record the shutdown request before
				// the flush resumes; if it ever lost this race, the case would degrade to
				// "after safe boundary", which expects the same outcome
				time.Sleep(500 * time.Millisecond)
				close(flushGate)

				err := <-runErrCh
				if err != nil && !errors.Is(err, context.Canceled) && !isExpectedRunStop(err, runCtx) {
					require.NoError(t, err, "failed to run vstreamclient")
				}
				assert.Equal(t, []*Customer{want}, active)

				replayed := replayAfterShutdown(t, te, newClient, sentinel)
				assert.Equal(t, []*Customer{sentinel}, replayed)
			},
		},
		{
			name:       "wait zero replays",
			streamName: "shutdown_wait_zero",
			id:         3031,
			run: func(t *testing.T, te *testEnv, newClient func(int, vstreamclient.FlushFunc, ...vstreamclient.Option) *vstreamclient.VStreamClient, want, sentinel *Customer) {
				var client *vstreamclient.VStreamClient
				flushCount := 0
				client = newClient(
					10, func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
						flushCount += len(rows)
						return nil
					},
					vstreamclient.WithEventFunc(func(_ context.Context, _ *binlogdatapb.VEvent) error {
						client.GracefulShutdown(0)
						return nil
					}, binlogdatapb.VEventType_ROW),
				)

				runCtx, cancelRun, runErrCh := te.runAsync(client, 30*time.Second)
				defer cancelRun()
				te.exec(t, "insert into customer.customer(id, email) values(:id, :email)", customerBindVars(want.ID, want.Email))

				err := <-runErrCh
				require.Error(t, err)
				assert.ErrorIs(t, err, context.Canceled)
				assert.Zero(t, flushCount)

				replayed := replayAfterShutdown(t, te, newClient, sentinel)
				assert.Equal(t, []*Customer{want, sentinel}, replayed)
				_ = runCtx
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			te := newTestEnv(t)
			want := &Customer{ID: tc.id, Email: tc.streamName + "@domain.com"}
			sentinel := &Customer{ID: tc.id + 1, Email: tc.streamName + "-sentinel@domain.com"}
			query := fmt.Sprintf("select * from customer where id between %d and %d", tc.id, tc.id+1)

			newClient := func(maxRows int, flushFn vstreamclient.FlushFunc, opts ...vstreamclient.Option) *vstreamclient.VStreamClient {
				return te.newDefaultClient(t, tc.streamName, []vstreamclient.TableConfig{{
					Keyspace:        "customer",
					Table:           "customer",
					Query:           query,
					MaxRowsPerFlush: maxRows,
					DataType:        &Customer{},
					FlushFn:         flushFn,
				}}, append([]vstreamclient.Option{
					vstreamclient.WithMinFlushDuration(10 * time.Second),
					vstreamclient.WithHeartbeatSeconds(1),
				}, opts...)...)
			}

			te.runUntilCopyCompleted(t, newClient(1, func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error { return nil }), tc.streamName)
			tc.run(t, te, newClient, want, sentinel)
		})
	}
}
