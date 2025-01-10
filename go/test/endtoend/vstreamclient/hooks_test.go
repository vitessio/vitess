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

// TestVStreamClientEventHooks verifies event callbacks receive real FIELD, ROW,
// and COMMIT events, which is important for observability and custom processing.
func TestVStreamClientEventHooks(t *testing.T) {
	te := newTestEnv(t)

	fieldCount := 0
	rowCount := 0
	commitCount := 0
	var rowTableNames []string

	vstreamClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		Query:           "select * from customer where id between 900 and 999",
		MaxRowsPerFlush: 10,
		DataType:        &Customer{},
		FlushFn:         func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error { return nil },
	}},
		vstreamclient.WithEventFunc(func(_ context.Context, ev *binlogdatapb.VEvent) error {
			fieldCount++
			assert.Equal(t, "customer.customer", ev.FieldEvent.TableName)
			return nil
		}, binlogdatapb.VEventType_FIELD),
		vstreamclient.WithEventFunc(func(_ context.Context, ev *binlogdatapb.VEvent) error {
			rowCount += len(ev.RowEvent.RowChanges)
			rowTableNames = append(rowTableNames, ev.RowEvent.TableName)
			return nil
		}, binlogdatapb.VEventType_ROW),
		vstreamclient.WithEventFunc(func(_ context.Context, _ *binlogdatapb.VEvent) error {
			commitCount++
			return nil
		}, binlogdatapb.VEventType_COMMIT),
	)

	te.exec(t, "insert into customer.customer(id, email) values (901, 'event-hook@domain.com')", nil)
	te.runUntilTimeout(t, vstreamClient, 2*time.Second)

	assert.Positive(t, fieldCount)
	assert.Equal(t, 1, rowCount)
	assert.Positive(t, commitCount)
	assert.Contains(t, rowTableNames, "customer.customer")
}

// TestVStreamClientFlushFnError verifies flush failures are returned from Run,
// which is important so downstream write errors are never silently ignored.
func TestVStreamClientFlushFnError(t *testing.T) {
	te := newTestEnv(t)

	flushErr := errors.New("flush failed")
	vstreamClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		Query:           "select * from customer where id between 1000 and 1099",
		MaxRowsPerFlush: 10,
		DataType:        &Customer{},
		FlushFn:         func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error { return flushErr },
	}})

	te.exec(t, "insert into customer.customer(id, email) values (1001, 'flush-error@domain.com')", nil)

	runCtx, cancelRun := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelRun()
	err := vstreamClient.Run(runCtx)
	require.Error(t, err)
	assert.ErrorContains(t, err, "error flushing table customer")
	assert.ErrorContains(t, err, flushErr.Error())
}

// TestVStreamClientEventHookError verifies callback failures abort the stream,
// which is important when hooks are part of correctness-sensitive workflows.
func TestVStreamClientEventHookError(t *testing.T) {
	te := newTestEnv(t)

	hookErr := errors.New("field hook failed")
	vstreamClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		Query:           "select * from customer where id between 1700 and 1799",
		MaxRowsPerFlush: 10,
		DataType:        &Customer{},
		FlushFn:         func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error { return nil },
	}},
		vstreamclient.WithEventFunc(func(_ context.Context, _ *binlogdatapb.VEvent) error { return hookErr }, binlogdatapb.VEventType_FIELD),
	)

	te.exec(t, "insert into customer.customer(id, email) values (1701, 'hook-error@domain.com')", nil)

	runCtx, cancelRun := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelRun()
	err := vstreamClient.Run(runCtx)
	require.Error(t, err)
	assert.ErrorContains(t, err, "user error processing FIELD event")
	assert.ErrorContains(t, err, hookErr.Error())
}
