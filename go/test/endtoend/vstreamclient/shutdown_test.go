package vstreamclient

import (
	"context"
	"errors"
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
	vstreamClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
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

	runCtx, cancelRun, runErrCh := te.runAsync(vstreamClient, 5*time.Second)
	select {
	case <-rowSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for row event")
	}
	close(shutdownCh)

	err := <-runErrCh
	if !isExpectedRunStop(err, runCtx) && !errors.Is(err, context.Canceled) {
		t.Fatalf("failed to run vstreamclient: %v", err)
	}
	cancelRun()
	err = vstreamClient.Run(context.Background())
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
	vstreamClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
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

	runCtx, cancelRun, runErrCh := te.runAsync(vstreamClient, time.Second)
	defer cancelRun()
	select {
	case <-rowSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for row event")
	}
	close(shutdownCh)

	err := <-runErrCh
	assert.NotErrorIs(t, runCtx.Err(), context.DeadlineExceeded)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("failed to run vstreamclient: %v", err)
	}
}

// TestVStreamClientIgnoresNoOpTransactions verifies unrelated writes do not
// trigger flushes for a filtered stream, which avoids noisy false-positive work.
func TestVStreamClientIgnoresNoOpTransactions(t *testing.T) {
	te := newTestEnv(t)

	flushCount := 0
	vstreamClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		Query:           "select * from customer where id between 2900 and 2999",
		MaxRowsPerFlush: 10,
		DataType:        &Customer{},
		FlushFn: func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
			flushCount++
			return nil
		},
	}})

	runCtx, cancelRun, runErrCh := te.runAsync(vstreamClient, 2*time.Second)
	defer cancelRun()

	te.exec(t, "insert into accounting.customer(id, email) values (2901, 'unrelated@domain.com')", nil)

	assert.Never(t, func() bool {
		return flushCount > 0
	}, 1500*time.Millisecond, 100*time.Millisecond)

	cancelRun()
	err := <-runErrCh
	if err != nil && runCtx.Err() == nil {
		t.Fatalf("failed to run vstreamclient: %v", err)
	}
	assert.Zero(t, flushCount)
}

// TestVStreamClientGracefulShutdownClosesMultiTableClient verifies
// GracefulShutdown can stop a multi-table stream after both tables have started
// emitting rows, and that the client is closed afterward.
func TestVStreamClientGracefulShutdownClosesMultiTableClient(t *testing.T) {
	te := newTestEnv(t)

	var rowTables []string
	rowTableSeen := make(chan string, 4)
	newClient := func() *vstreamclient.VStreamClient {
		return te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{
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
	_, cancelRun, runErrCh := te.runAsync(vstreamClient, 5*time.Second)
	seen := map[string]bool{}
	deadline := time.After(3 * time.Second)
	for !(seen["customer.customer"] && seen["customer.purchases"]) {
		select {
		case tableName := <-rowTableSeen:
			seen[tableName] = true
		case <-deadline:
			t.Fatal("timed out waiting for both prime row event tables")
		}
	}

	vstreamClient.GracefulShutdown(6 * time.Second)
	cancelRun()
	err := <-runErrCh
	if !isExpectedRunStop(err, nil) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("failed to run vstreamclient: %v", err)
	}

	err = vstreamClient.Run(context.Background())
	require.Error(t, err)
	assert.ErrorContains(t, err, "client is closed")
	assert.ElementsMatch(t, []string{"customer.customer", "customer.purchases"}, rowTables)
}
