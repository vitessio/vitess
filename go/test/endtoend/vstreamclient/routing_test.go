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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vstreamclient"
)

// TestVStreamClientDisambiguatesSameTableNames verifies that streaming two
// keyspaces with the same table name routes events to the correct configured
// table using the qualified keyspace.table names from VTGate.
func TestVStreamClientDisambiguatesSameTableNames(t *testing.T) {
	te := newTestEnv(t)
	var gotFieldEvents []string
	var gotRowEvents []string
	vstreamClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{
		{
			Keyspace:        "customer",
			Table:           "customer",
			Query:           "select * from customer where id between 200 and 399",
			MaxRowsPerFlush: 2,
			DataType:        &Customer{},
			FlushFn:         func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error { return nil },
		},
		{
			Keyspace:        "accounting",
			Table:           "customer",
			Query:           "select * from customer where id between 200 and 399",
			MaxRowsPerFlush: 2,
			DataType:        &Customer{},
			FlushFn:         func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error { return nil },
		},
	},
		vstreamclient.WithEventFunc(func(_ context.Context, ev *binlogdatapb.VEvent) error {
			gotFieldEvents = append(gotFieldEvents, fmt.Sprintf("%s:%s", ev.Keyspace, ev.FieldEvent.TableName))
			return nil
		}, binlogdatapb.VEventType_FIELD),
		vstreamclient.WithEventFunc(func(_ context.Context, ev *binlogdatapb.VEvent) error {
			gotRowEvents = append(gotRowEvents, fmt.Sprintf("%s:%s", ev.Keyspace, ev.RowEvent.TableName))
			return nil
		}, binlogdatapb.VEventType_ROW),
	)

	te.exec(t, "insert into customer.customer(id, email) values(:id, :email)", map[string]*querypb.BindVariable{
		"id":    {Type: querypb.Type_UINT64, Value: []byte("201")},
		"email": {Type: querypb.Type_VARCHAR, Value: []byte("multi-customer@domain.com")},
	})
	te.exec(t, "insert into accounting.customer(id, email) values(:id, :email)", map[string]*querypb.BindVariable{
		"id":    {Type: querypb.Type_UINT64, Value: []byte("301")},
		"email": {Type: querypb.Type_VARCHAR, Value: []byte("multi-accounting@domain.com")},
	})

	te.runUntilTimeout(t, vstreamClient, 5*time.Second)

	assert.Contains(t, gotFieldEvents, "customer:customer.customer")
	assert.Contains(t, gotFieldEvents, "accounting:accounting.customer")
	assert.Contains(t, gotRowEvents, "customer:customer.customer")
	assert.Contains(t, gotRowEvents, "accounting:accounting.customer")
}

// TestVStreamClientStreamsMultipleTablesInOneKeyspace verifies one client can
// stream multiple tables in the same keyspace through normal Run-time flushes,
// without depending on shutdown-time flushing.
func TestVStreamClientStreamsMultipleTablesInOneKeyspace(t *testing.T) {
	te := newTestEnv(t)

	var gotCustomers []*Customer
	var gotOrders []*Order
	var fieldTables []string
	var rowTables []string
	rowTableSeen := make(chan string, 4)
	vstreamClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{
		{
			Keyspace:        "customer",
			Table:           "customer",
			Query:           "select * from customer where id between 1800 and 1899",
			MaxRowsPerFlush: 10,
			DataType:        &Customer{},
			FlushFn: func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
				for _, row := range rows {
					gotCustomers = append(gotCustomers, row.Data.(*Customer))
				}
				return nil
			},
		},
		{
			Keyspace:        "customer",
			Table:           "purchases",
			Query:           "select * from `purchases` where id between 1800 and 1899",
			MaxRowsPerFlush: 10,
			DataType:        &Order{},
			FlushFn: func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
				for _, row := range rows {
					gotOrders = append(gotOrders, row.Data.(*Order))
				}
				return nil
			},
		},
	},
		vstreamclient.WithEventFunc(func(_ context.Context, ev *binlogdatapb.VEvent) error {
			fieldTables = append(fieldTables, ev.FieldEvent.TableName)
			return nil
		}, binlogdatapb.VEventType_FIELD),
		vstreamclient.WithEventFunc(func(_ context.Context, ev *binlogdatapb.VEvent) error {
			rowTables = append(rowTables, ev.RowEvent.TableName)
			select {
			case rowTableSeen <- ev.RowEvent.TableName:
			default:
			}
			return nil
		}, binlogdatapb.VEventType_ROW),
	)

	te.exec(t, "insert into customer.customer(id, email) values (1801, 'multi-table@domain.com')", nil)
	te.exec(t, "insert into customer.purchases(id, customer_id, note) values (1801, 1801, 'first-order')", nil)
	te.runUntilTimeout(t, vstreamClient, 5*time.Second)

	assert.ElementsMatch(t, []string{"customer.customer", "customer.purchases"}, fieldTables)
	assert.ElementsMatch(t, []string{"customer.customer", "customer.purchases"}, rowTables)
	assert.Equal(t, []*Customer{{ID: 1801, Email: "multi-table@domain.com"}}, gotCustomers)
	assert.Equal(t, []*Order{{ID: 1801, CustomerID: 1801, Note: "first-order"}}, gotOrders)
}

// TestVStreamClientBareTableNamesWithCustomFlags verifies bare table-name events
// still resolve correctly when callers explicitly opt out of keyspace prefixes.
func TestVStreamClientBareTableNamesWithCustomFlags(t *testing.T) {
	te := newTestEnv(t)

	var fieldTables []string
	var rowTables []string
	var got []*Customer
	vstreamClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		Query:           "select * from customer where id between 2200 and 2299",
		MaxRowsPerFlush: 10,
		DataType:        &Customer{},
		FlushFn: func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
			for _, row := range rows {
				got = append(got, row.Data.(*Customer))
			}
			return nil
		},
	}},
		vstreamclient.WithFlags(&vtgatepb.VStreamFlags{HeartbeatInterval: 1, ExcludeKeyspaceFromTableName: true}),
		vstreamclient.WithEventFunc(func(_ context.Context, ev *binlogdatapb.VEvent) error {
			fieldTables = append(fieldTables, ev.FieldEvent.TableName)
			return nil
		}, binlogdatapb.VEventType_FIELD),
		vstreamclient.WithEventFunc(func(_ context.Context, ev *binlogdatapb.VEvent) error {
			rowTables = append(rowTables, ev.RowEvent.TableName)
			return nil
		}, binlogdatapb.VEventType_ROW),
	)

	te.exec(t, "insert into customer.customer(id, email) values (2201, 'bare-table@domain.com')", nil)
	te.runUntilTimeout(t, vstreamClient, 2*time.Second)

	assert.Contains(t, fieldTables, "customer")
	assert.Contains(t, rowTables, "customer")
	assert.Equal(t, []*Customer{{ID: 2201, Email: "bare-table@domain.com"}}, got)
}

// TestVStreamClientBareTableNamesAmbiguousAcrossKeyspaces verifies ambiguous bare
// table names are rejected during construction, which prevents silent misrouting.
func TestVStreamClientBareTableNamesAmbiguousAcrossKeyspaces(t *testing.T) {
	te := newTestEnv(t)

	_, err := vstreamclient.New(te.ctx, t.Name(), te.conn, []vstreamclient.TableConfig{
		{
			Keyspace:        "customer",
			Table:           "customer",
			Query:           "select * from customer where id between 2300 and 2499",
			MaxRowsPerFlush: 10,
			DataType:        &Customer{},
			FlushFn:         func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error { return nil },
		},
		{
			Keyspace:        "accounting",
			Table:           "customer",
			Query:           "select * from customer where id between 2300 and 2499",
			MaxRowsPerFlush: 10,
			DataType:        &Customer{},
			FlushFn:         func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error { return nil },
		},
	},
		vstreamclient.WithMinFlushDuration(500*time.Millisecond),
		vstreamclient.WithHeartbeatSeconds(1),
		vstreamclient.WithStateTable("commerce", "vstreams"),
		vstreamclient.WithFlags(&vtgatepb.VStreamFlags{HeartbeatInterval: 1, ExcludeKeyspaceFromTableName: true}),
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, "ExcludeKeyspaceFromTableName")
	assert.ErrorContains(t, err, "customer")
}
