package vstreamclient

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/vstreamclient"
)

// TestVStreamClient verifies the core insert, update, and delete stream flow,
// which is important because it exercises the basic end-to-end contract users rely on.
func TestVStreamClient(t *testing.T) {
	te := newTestEnv(t)

	flushCount := 0
	gotCustomers := make([]*Customer, 0)
	tables := []vstreamclient.TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		Query:           "select * from customer where id between 1 and 9",
		MaxRowsPerFlush: 7,
		DataType:        &Customer{},
		FlushFn: func(ctx context.Context, rows []vstreamclient.Row, meta vstreamclient.FlushMeta) error {
			flushCount++

			t.Logf("upserting %d customers\n", len(rows))
			for i, row := range rows {
				switch {
				case row.RowChange.After == nil:
					customer := row.Data.(*Customer)
					customer.DeletedAt = time.Now()
					gotCustomers = append(gotCustomers, customer)
					t.Logf("deleting customer %d: %v\n", i, row)
				case row.RowChange.Before == nil:
					gotCustomers = append(gotCustomers, row.Data.(*Customer))
					t.Logf("inserting customer %d: %v\n", i, row)
				case row.RowChange.Before != nil:
					gotCustomers = append(gotCustomers, row.Data.(*Customer))
					t.Logf("updating customer %d: %v\n", i, row)
				}
			}
			return nil
		},
	}}
	newClient := func(t *testing.T) *vstreamclient.VStreamClient {
		t.Helper()
		return te.newDefaultClient(t, "bob", tables)
	}

	t.Run("inserting rows", func(t *testing.T) {
		wantCustomers := []*Customer{{ID: 1, Email: "alice@domain.com"}, {ID: 2, Email: "bob@domain.com"}, {ID: 3, Email: "charlie@domain.com"}, {ID: 4, Email: "dan@domain.com"}, {ID: 5, Email: "eve@domain.com"}}
		for _, customer := range wantCustomers {
			te.exec(t, "insert into customer.customer(id, email) values(:id, :email)", customerBindVars(customer.ID, customer.Email))
		}
		te.runUntilTimeout(t, newClient(t), 2*time.Second)
		assert.Positive(t, flushCount)
		assert.ElementsMatch(t, gotCustomers, wantCustomers)
	})

	t.Run("updating rows", func(t *testing.T) {
		gotCustomers = nil
		updateCustomers := []*Customer{{ID: 1, Email: "alice_new@domain.com"}, {ID: 5, Email: "eve_new@domain.com"}}
		for _, customer := range updateCustomers {
			te.exec(t, "update customer.customer set email=:email where id=:id", customerBindVars(customer.ID, customer.Email))
		}
		te.runUntilTimeout(t, newClient(t), 2*time.Second)
		assert.ElementsMatch(t, gotCustomers, updateCustomers)
	})

	t.Run("deleting rows", func(t *testing.T) {
		gotCustomers = nil
		deleteCustomerIDs := []int{1, 5}
		for _, id := range deleteCustomerIDs {
			te.exec(t, "delete from customer.customer where id=:id", idBindVar(int64(id)))
		}
		te.runUntilTimeout(t, newClient(t), 2*time.Second)
		assert.Len(t, gotCustomers, len(deleteCustomerIDs))
		for _, gotCustomer := range gotCustomers {
			assert.NotEmpty(t, gotCustomer.DeletedAt)
		}
	})
}

// TestVStreamClientFlushChunking verifies large batches are split according to
// MaxRowsPerFlush, which is important for bounded memory and downstream writes.
func TestVStreamClientFlushChunking(t *testing.T) {
	te := newTestEnv(t)

	var chunkSizes []int
	var got []*Customer
	vstreamClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		Query:           "select * from customer where id between 400 and 499",
		MaxRowsPerFlush: 2,
		DataType:        &Customer{},
		FlushFn: func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
			chunkSizes = append(chunkSizes, len(rows))
			for _, row := range rows {
				got = append(got, row.Data.(*Customer))
			}
			return nil
		},
	}})

	te.exec(t, "insert into customer.customer(id, email) values (401, 'chunk-1@domain.com'), (402, 'chunk-2@domain.com'), (403, 'chunk-3@domain.com'), (404, 'chunk-4@domain.com'), (405, 'chunk-5@domain.com')", nil)
	te.runUntilTimeout(t, vstreamClient, 2*time.Second)

	assert.Equal(t, []int{2, 2, 1}, chunkSizes)
	assert.ElementsMatch(t, []*Customer{{ID: 401, Email: "chunk-1@domain.com"}, {ID: 402, Email: "chunk-2@domain.com"}, {ID: 403, Email: "chunk-3@domain.com"}, {ID: 404, Email: "chunk-4@domain.com"}, {ID: 405, Email: "chunk-5@domain.com"}}, got)
}

// TestVStreamClientFlushesOnHeartbeat verifies heartbeat timing flushes buffered
// rows even when the batch size is not reached, which prevents data from stalling.
func TestVStreamClientFlushesOnHeartbeat(t *testing.T) {
	te := newTestEnv(t)

	flushes := make(chan []*Customer, 2)
	vstreamClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		Query:           "select * from customer where id between 500 and 599",
		MaxRowsPerFlush: 10,
		DataType:        &Customer{},
		FlushFn: func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
			batch := make([]*Customer, 0, len(rows))
			for _, row := range rows {
				batch = append(batch, row.Data.(*Customer))
			}
			flushes <- batch
			return nil
		},
	}}, vstreamclient.WithMinFlushDuration(1500*time.Millisecond))

	te.exec(t, "insert into customer.customer(id, email) values (501, 'heartbeat-initial@domain.com')", nil)
	runCtx, cancelRun, runErrCh := te.runAsync(vstreamClient, 4*time.Second)
	defer cancelRun()

	firstFlush := <-flushes
	assert.Equal(t, []*Customer{{ID: 501, Email: "heartbeat-initial@domain.com"}}, firstFlush)

	te.exec(t, "insert into customer.customer(id, email) values (502, 'heartbeat-late@domain.com')", nil)
	insertedAt := time.Now()

	secondFlush := <-flushes
	assert.Equal(t, []*Customer{{ID: 502, Email: "heartbeat-late@domain.com"}}, secondFlush)
	assert.Greater(t, time.Since(insertedAt), time.Second)

	cancelRun()
	err := <-runErrCh
	if err != nil && runCtx.Err() == nil {
		t.Fatalf("failed to run vstreamclient: %v", err)
	}
}

// TestVStreamClientTransactionBoundaries verifies rows from one transaction are
// flushed together after COMMIT, which preserves transactional consistency.
func TestVStreamClientTransactionBoundaries(t *testing.T) {
	te := newTestEnv(t)

	flushes := make(chan []*Customer, 1)
	vstreamClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		Query:           "select * from customer where id between 1300 and 1399",
		MaxRowsPerFlush: 10,
		DataType:        &Customer{},
		FlushFn: func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
			batch := make([]*Customer, 0, len(rows))
			for _, row := range rows {
				batch = append(batch, row.Data.(*Customer))
			}
			flushes <- batch
			return nil
		},
	}})

	runCtx, cancelRun, runErrCh := te.runAsync(vstreamClient, 3*time.Second)
	defer cancelRun()

	te.exec(t, "begin", nil)
	te.exec(t, "insert into customer.customer(id, email) values (1301, 'tx-a@domain.com'), (1302, 'tx-b@domain.com')", nil)
	te.exec(t, "commit", nil)

	batch := <-flushes
	assert.ElementsMatch(t, []*Customer{{ID: 1301, Email: "tx-a@domain.com"}, {ID: 1302, Email: "tx-b@domain.com"}}, batch)

	cancelRun()
	err := <-runErrCh
	if err != nil && runCtx.Err() == nil {
		t.Fatalf("failed to run vstreamclient: %v", err)
	}
}

// TestVStreamClientHandlesDDL verifies schema changes do not stop the stream
// from continuing to deliver matching rows in the normal tolerant mode.
func TestVStreamClientHandlesDDL(t *testing.T) {
	te := newTestEnv(t)

	gotCh := make(chan *Customer, 1)
	vstreamClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		Query:           "select * from customer where id between 1400 and 1499",
		MaxRowsPerFlush: 10,
		DataType:        &Customer{},
		FlushFn: func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
			for _, row := range rows {
				select {
				case gotCh <- row.Data.(*Customer):
				default:
				}
			}
			return nil
		},
	}})

	runCtx, cancelRun, runErrCh := te.runAsync(vstreamClient, 4*time.Second)
	defer cancelRun()
	time.Sleep(200 * time.Millisecond)

	te.exec(t, "alter table customer.customer add column ddl_note varchar(64) null", nil)
	te.exec(t, "insert into customer.customer(id, email, ddl_note) values (1401, 'ddl@domain.com', 'ok')", nil)

	select {
	case got := <-gotCh:
		assert.Equal(t, &Customer{ID: 1401, Email: "ddl@domain.com"}, got)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for row after DDL")
	}

	cancelRun()
	err := <-runErrCh
	if err != nil && runCtx.Err() == nil {
		t.Fatalf("failed to run vstreamclient: %v", err)
	}
}

// TestVStreamClientReuseBatchSlice verifies the opt-in batch reuse mode actually
// reuses batch backing storage, which matters for allocation-sensitive consumers.
func TestVStreamClientReuseBatchSlice(t *testing.T) {
	te := newTestEnv(t)

	batchPointers := make(chan string, 2)
	newClient := func(t *testing.T) *vstreamclient.VStreamClient {
		t.Helper()
		return te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
			Keyspace:        "customer",
			Table:           "customer",
			Query:           "select * from customer where id between 1960 and 1969",
			MaxRowsPerFlush: 2,
			ReuseBatchSlice: true,
			DataType:        &Customer{},
			FlushFn: func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
				pointer := ""
				if len(rows) > 0 {
					pointer = fmt.Sprintf("%p", &rows[0])
				}
				select {
				case batchPointers <- pointer:
				default:
				}
				return nil
			},
		}})
	}

	te.exec(t, "insert into customer.customer(id, email) values (1961, 'reuse-a@domain.com'), (1962, 'reuse-b@domain.com')", nil)
	vstreamClient := newClient(t)
	runCtx, cancelRun, runErrCh := te.runAsync(vstreamClient, 4*time.Second)
	defer cancelRun()
	var firstPointer string
	select {
	case firstPointer = <-batchPointers:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for first batch flush")
	}

	te.exec(t, "insert into customer.customer(id, email) values (1963, 'reuse-c@domain.com'), (1964, 'reuse-d@domain.com')", nil)
	var secondPointer string
	select {
	case secondPointer = <-batchPointers:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for second batch flush")
	}

	cancelRun()
	err := <-runErrCh
	if err != nil && runCtx.Err() == nil {
		t.Fatalf("failed to run vstreamclient: %v", err)
	}

	assert.Equal(t, firstPointer, secondPointer)
}
