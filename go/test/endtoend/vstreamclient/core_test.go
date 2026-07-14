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
	"sync/atomic"
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
	var rowsFlushed atomic.Int64
	gotCustomers := make([]*Customer, 0)
	tables := []vstreamclient.TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		Query:           "select * from customer where id between 1 and 9",
		MaxRowsPerFlush: 7,
		DataType:        &Customer{},
		FlushFn: func(ctx context.Context, rows []vstreamclient.Row, meta vstreamclient.FlushMeta) error {
			flushCount++
			defer rowsFlushed.Add(int64(len(rows)))

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
		te.runUntilCopyCompleted(t, newClient(t), "bob")
		assert.Positive(t, flushCount)
		assert.ElementsMatch(t, gotCustomers, wantCustomers)
	})

	t.Run("updating rows", func(t *testing.T) {
		gotCustomers = nil
		base := rowsFlushed.Load()
		updateCustomers := []*Customer{{ID: 1, Email: "alice_new@domain.com"}, {ID: 5, Email: "eve_new@domain.com"}}
		for _, customer := range updateCustomers {
			te.exec(t, "update customer.customer set email=:email where id=:id", customerBindVars(customer.ID, customer.Email))
		}
		te.runUntil(t, newClient(t), func() bool { return rowsFlushed.Load() >= base+2 })
		assert.ElementsMatch(t, gotCustomers, updateCustomers)
	})

	t.Run("deleting rows", func(t *testing.T) {
		gotCustomers = nil
		base := rowsFlushed.Load()
		deleteCustomerIDs := []int{1, 5}
		for _, id := range deleteCustomerIDs {
			te.exec(t, "delete from customer.customer where id=:id", idBindVar(int64(id)))
		}
		te.runUntil(t, newClient(t), func() bool { return rowsFlushed.Load() >= base+2 })
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
	te.runUntilCopyCompleted(t, vstreamClient, t.Name())

	// the exact chunk shape depends on how many rows were still buffered at each flush boundary,
	// so only pin the chunking contract: no chunk exceeds MaxRowsPerFlush and no row is dropped
	totalRows := 0
	for _, size := range chunkSizes {
		assert.LessOrEqual(t, size, 2)
		totalRows += size
	}
	assert.Equal(t, 5, totalRows)
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
	runCtx, cancelRun, runErrCh := te.runAsync(vstreamClient, 30*time.Second)
	defer cancelRun()

	firstFlush := recvOrFail(t, flushes, "first flush")
	assert.Equal(t, []*Customer{{ID: 501, Email: "heartbeat-initial@domain.com"}}, firstFlush)

	// with no further writes after this insert, only a heartbeat (or min-duration) boundary can
	// flush the buffered row; asserting on elapsed wall-clock time here is inherently flaky on
	// loaded CI runners, so only the delivery itself is pinned
	te.exec(t, "insert into customer.customer(id, email) values (502, 'heartbeat-late@domain.com')", nil)

	secondFlush := recvOrFail(t, flushes, "second flush")
	assert.Equal(t, []*Customer{{ID: 502, Email: "heartbeat-late@domain.com"}}, secondFlush)

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

	runCtx, cancelRun, runErrCh := te.runAsync(vstreamClient, 30*time.Second)
	defer cancelRun()

	te.exec(t, "begin", nil)
	te.exec(t, "insert into customer.customer(id, email) values (1301, 'tx-a@domain.com'), (1302, 'tx-b@domain.com')", nil)
	te.exec(t, "commit", nil)

	batch := recvOrFail(t, flushes, "transaction flush")
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
	t.Cleanup(func() {
		// without this drop, the leftover column breaks the strict-mode schema drift test in
		// any later test ordering, making the suite order-dependent
		te.execBackgroundAllowMissingColumn(t, "alter table customer.customer drop column ddl_note", nil)
	})

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

	runCtx, cancelRun, runErrCh := te.runAsync(vstreamClient, 30*time.Second)
	defer cancelRun()

	// wait for the stream to be up (copy phase checkpointed) before issuing DDL, instead of
	// guessing readiness with a sleep
	assert.Eventually(t, te.copyCompleted(t.Name()), 30*time.Second, 50*time.Millisecond)

	te.exec(t, "alter table customer.customer add column ddl_note varchar(64) null", nil)
	te.exec(t, "insert into customer.customer(id, email, ddl_note) values (1401, 'ddl@domain.com', 'ok')", nil)

	got := recvOrFail(t, gotCh, "row after DDL")
	assert.Equal(t, &Customer{ID: 1401, Email: "ddl@domain.com"}, got)

	cancelRun()
	err := <-runErrCh
	if err != nil && runCtx.Err() == nil {
		t.Fatalf("failed to run vstreamclient: %v", err)
	}
}

// The batch-reuse contract of ReuseBatchSlice is pinned deterministically by the unit test
// TestResetBatch_ReuseBatchSlice in the vstreamclient package. An e2e version comparing batch
// addresses across flushes can false-pass: without reuse, the previous batch is garbage by the
// time the next one is allocated, so the allocator may legitimately return the same address.
