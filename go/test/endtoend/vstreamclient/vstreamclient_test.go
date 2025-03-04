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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/vstreamclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Customer is the concrete type that will be built from the stream
type Customer struct {
	ID        int64     `vstream:"id"`
	Email     string    `vstream:"email"`
	DeletedAt time.Time `vstream:"-"`
}

func TestVStreamClient(t *testing.T) {
	flushCount := 0
	gotCustomers := make([]*Customer, 0)
	tables := []vstreamclient.TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		MaxRowsPerFlush: 7,
		DataType:        &Customer{},
		FlushFn: func(ctx context.Context, rows []vstreamclient.Row, meta vstreamclient.FlushMeta) error {
			flushCount++

			t.Logf("upserting %d customers\n", len(rows))
			for i, row := range rows {
				switch {
				// delete event
				case row.RowChange.After == nil:
					customer := row.Data.(*Customer)
					customer.DeletedAt = time.Now()

					gotCustomers = append(gotCustomers, customer)
					t.Logf("deleting customer %d: %v\n", i, row)

				// insert event
				case row.RowChange.Before == nil:
					gotCustomers = append(gotCustomers, row.Data.(*Customer))
					t.Logf("inserting customer %d: %v\n", i, row)

				// update event
				case row.RowChange.Before != nil:
					gotCustomers = append(gotCustomers, row.Data.(*Customer))
					t.Logf("updating customer %d: %v\n", i, row)
				}
			}

			// a real implementation would do something more meaningful here. For a data warehouse type workload,
			// it would probably look like streaming rows into the data warehouse, or for more complex versions,
			// write newline delimited json or a parquet file to object storage, then trigger a load job.
			return nil
		},
	}}

	ctx := context.Background()
	conn, err := cluster.DialVTGate(ctx, t.Name(), vtgateGrpcAddress, "test_user", "")
	require.NoError(t, err)
	defer conn.Close()

	vtgateSession := conn.Session("", nil)
	qCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vstreamClient, err := vstreamclient.New(ctx, "bob", conn, tables,
		vstreamclient.WithMinFlushDuration(500*time.Millisecond),
		vstreamclient.WithHeartbeatSeconds(1),
		vstreamclient.WithStateTable("commerce", "vstreams"),
		vstreamclient.WithEventFunc(func(ctx context.Context, ev *binlogdatapb.VEvent) error {
			t.Logf("** FIELD EVENT: %v\n", ev)
			return nil
		}, binlogdatapb.VEventType_FIELD),
	)
	require.NoError(t, err)

	t.Run("inserting rows", func(t *testing.T) {
		wantCustomers := []*Customer{
			{ID: 1, Email: "alice@domain.com"},
			{ID: 2, Email: "bob@domain.com"},
			{ID: 3, Email: "charlie@domain.com"},
			{ID: 4, Email: "dan@domain.com"},
			{ID: 5, Email: "eve@domain.com"},
		}

		insertQuery := "insert into customer(id, email) values(:id, :email)"
		for _, customer := range wantCustomers {
			bindVariables := map[string]*querypb.BindVariable{
				"id":    {Type: querypb.Type_UINT64, Value: []byte(strconv.FormatInt(int64(customer.ID), 10))},
				"email": {Type: querypb.Type_VARCHAR, Value: []byte(customer.Email)},
			}
			_, err = vtgateSession.Execute(qCtx, insertQuery, bindVariables)
			require.NoError(t, err)
		}

		ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err = vstreamClient.Run(ctx)
		if err != nil && ctx.Err() == nil {
			t.Fatalf("failed to run vstreamclient: %v", err)
		}

		t.Logf("flush count: %d", flushCount)

		assert.Positive(t, flushCount)
		assert.ElementsMatch(t, gotCustomers, wantCustomers)
	})

	t.Run("updating rows", func(t *testing.T) {
		// Clear out gotCustomers
		gotCustomers = nil

		updateCustomers := []*Customer{
			{
				ID:    1,
				Email: "alice_new@domain.com",
			},
			{
				ID:    5,
				Email: "eve_new@domain.com",
			},
		}
		updateQuery := "update customer set email=:email where id=:id"
		for _, customer := range updateCustomers {
			bindVariables := map[string]*querypb.BindVariable{
				"id":    {Type: querypb.Type_UINT64, Value: []byte(strconv.FormatInt(int64(customer.ID), 10))},
				"email": {Type: querypb.Type_VARCHAR, Value: []byte(customer.Email)},
			}
			_, err = vtgateSession.Execute(qCtx, updateQuery, bindVariables)
			require.NoError(t, err)
		}

		ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err = vstreamClient.Run(ctx)
		if err != nil && ctx.Err() == nil {
			t.Fatalf("failed to run vstreamclient: %v", err)
		}

		t.Logf("flush count: %d", flushCount)

		assert.ElementsMatch(t, gotCustomers, updateCustomers)
	})

	t.Run("deleting rows", func(t *testing.T) {
		// Clear out gotCustomers
		gotCustomers = nil

		deleteCustomerIDs := []int{1, 5}
		deleteQuery := "delete from customer where id=:id"
		for _, id := range deleteCustomerIDs {
			bindVariables := map[string]*querypb.BindVariable{
				"id": {Type: querypb.Type_UINT64, Value: []byte(strconv.FormatInt(int64(id), 10))},
			}
			_, err = vtgateSession.Execute(qCtx, deleteQuery, bindVariables)
			require.NoError(t, err)
		}

		ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err = vstreamClient.Run(ctx)
		if err != nil && ctx.Err() == nil {
			t.Fatalf("failed to run vstreamclient: %v", err)
		}

		t.Logf("flush count: %d", flushCount)

		assert.Len(t, gotCustomers, len(deleteCustomerIDs))
		// Expect non-zero `DeletedAt` field, as we setting it to
		// time.Now() in the flushFunc.
		for _, gotCustomer := range gotCustomers {
			assert.NotEmpty(t, gotCustomer.DeletedAt)
		}
	})
}
