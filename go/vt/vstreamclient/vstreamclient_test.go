package vstreamclient

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

// Customer is the concrete type that will be built from the stream
type Customer struct {
	ID        int64     `vstream:"customer_id"`
	Email     string    `vstream:"email"`
	DeletedAt time.Time `vstream:"-"`
}

func getConn(t *testing.T, ctx context.Context) *vtgateconn.VTGateConn {
	t.Helper()
	conn, err := vtgateconn.Dial(ctx, "localhost:15991")
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

// To run the tests, this currently expects the local example to be running
// ./101_initial_cluster.sh; mysql < ../common/insert_commerce_data.sql; ./201_customer_tablets.sh; ./202_move_tables.sh; ./203_switch_reads.sh; ./204_switch_writes.sh; ./205_clean_commerce.sh; ./301_customer_sharded.sh; ./302_new_shards.sh; ./303_reshard.sh; ./304_switch_reads.sh; ./305_switch_writes.sh; ./306_down_shard_0.sh; ./307_delete_shard_0.sh
func TestVStreamClient(t *testing.T) {
	conn := getConn(t, context.Background())
	defer conn.Close()

	flushCount := 0
	gotCustomers := make([]*Customer, 0)

	tables := []TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		MaxRowsPerFlush: 7,
		DataType:        &Customer{},
		FlushFn: func(ctx context.Context, rows []Row, meta FlushMeta) error {
			flushCount++

			fmt.Printf("upserting %d customers\n", len(rows))
			for i, row := range rows {
				switch {
				// delete event
				case row.RowChange.After == nil:
					customer := row.Data.(*Customer)
					customer.DeletedAt = time.Now()

					gotCustomers = append(gotCustomers, customer)
					fmt.Printf("deleting customer %d: %v\n", i, row)

				// insert event
				case row.RowChange.Before == nil:
					gotCustomers = append(gotCustomers, row.Data.(*Customer))
					fmt.Printf("inserting customer %d: %v\n", i, row)

				// update event
				case row.RowChange.Before != nil:
					gotCustomers = append(gotCustomers, row.Data.(*Customer))
					fmt.Printf("updating customer %d: %v\n", i, row)
				}
			}

			// a real implementation would do something more meaningful here. For a data warehouse type workload,
			// it would probably look like streaming rows into the data warehouse, or for more complex versions,
			// write newline delimited json or a parquet file to object storage, then trigger a load job.
			return nil
		},
	}}

	t.Run("first vstream run, should succeed", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		vstreamClient, err := New(ctx, "bob", conn, tables,
			WithMinFlushDuration(500*time.Millisecond),
			WithHeartbeatSeconds(1),
			WithStateTable("commerce", "vstreams"),
			WithEventFunc(func(ctx context.Context, ev *binlogdatapb.VEvent) error {
				fmt.Printf("** FIELD EVENT: %v\n", ev)
				return nil
			}, binlogdatapb.VEventType_FIELD),
		)
		if err != nil {
			t.Fatalf("failed to create VStreamClient: %v", err)
		}

		err = vstreamClient.Run(ctx)
		if err != nil && ctx.Err() == nil {
			t.Fatalf("failed to run vstreamclient: %v", err)
		}

		slices.SortFunc(gotCustomers, func(a, b *Customer) int {
			return int(a.ID - b.ID)
		})

		wantCustomers := []*Customer{
			{ID: 1, Email: "alice@domain.com"},
			{ID: 2, Email: "bob@domain.com"},
			{ID: 3, Email: "charlie@domain.com"},
			{ID: 4, Email: "dan@domain.com"},
			{ID: 5, Email: "eve@domain.com"},
		}

		fmt.Printf("got %d customers | flushed %d times\n", len(gotCustomers), flushCount)
		if !reflect.DeepEqual(gotCustomers, wantCustomers) {
			t.Fatalf("got %d customers, want %d", len(gotCustomers), len(wantCustomers))
		}
	})

	// this should fail because we're going to restart the stream, but with an additional table
	t.Run("second vstream run, should fail", func(t *testing.T) {
		withAdditionalTable := append(tables, TableConfig{
			Keyspace: "customer",
			Table:    "corder",
			DataType: &Customer{},
		})

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := New(ctx, "bob", conn, withAdditionalTable,
			WithStateTable("commerce", "vstreams"),
		)
		if err == nil {
			t.Fatalf("expected VStreamClient error, got nil")
		} else if err.Error() != "vstreamclient: provided tables do not match stored tables" {
			t.Fatalf("expected error 'vstreamclient: provided tables do not match stored tables', got '%v'", err)
		}
	})
}

// Customer is the concrete type that will be built from the stream. This version implements
// the VStreamScanner interface to do custom mapping of fields.
type CustomerWithScan struct {
	ID    int64
	Email string

	// the fields below aren't actually in the schema, but are added for illustrative purposes
	EmailConfirmed bool
	Details        map[string]any
	CreatedAt      time.Time
}

var _ VStreamScanner = (*CustomerWithScan)(nil)

func (customer *CustomerWithScan) VStreamScan(fields []*querypb.Field, row []sqltypes.Value, rowEvent *binlogdatapb.RowEvent, rowChange *binlogdatapb.RowChange) error {
	var err error

	for i := range row {
		if row[i].IsNull() {
			continue
		}

		switch fields[i].Name {
		case "customer_id":
			customer.ID, err = row[i].ToCastInt64()

		case "email":
			customer.Email = row[i].ToString()

		// the fields below aren't actually in the example schema, but are added to
		// show how you should handle different data types

		case "email_confirmed":
			customer.EmailConfirmed, err = row[i].ToBool()

		case "details":
			// assume the details field is a json blob
			var b []byte
			b, err = row[i].ToBytes()
			if err == nil {
				err = json.Unmarshal(b, &customer.Details)
			}

		case "created_at":
			customer.CreatedAt, err = row[i].ToTime(time.UTC)
		}
		if err != nil {
			return fmt.Errorf("error processing field %s: %w", fields[i].Name, err)
		}
	}

	return nil
}

// To run the tests, this currently expects the local example to be running
// ./101_initial_cluster.sh; mysql < ../common/insert_commerce_data.sql; ./201_customer_tablets.sh; ./202_move_tables.sh; ./203_switch_reads.sh; ./204_switch_writes.sh; ./205_clean_commerce.sh; ./301_customer_sharded.sh; ./302_new_shards.sh; ./303_reshard.sh; ./304_switch_reads.sh; ./305_switch_writes.sh; ./306_down_shard_0.sh; ./307_delete_shard_0.sh
func TestVStreamClientWithScan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := vtgateconn.Dial(ctx, "localhost:15991")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	flushCount := 0
	gotCustomers := make([]*CustomerWithScan, 0)

	tables := []TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		MaxRowsPerFlush: 7,
		FlushFn: func(ctx context.Context, rows []Row, meta FlushMeta) error {
			flushCount++

			fmt.Printf("upserting %d customers\n", len(rows))
			for i, row := range rows {
				gotCustomers = append(gotCustomers, row.Data.(*CustomerWithScan))
				fmt.Printf("upserting customer %d: %v\n", i, row)
			}

			// a real implementation would do something more meaningful here. For a data warehouse type workload,
			// it would probably look like streaming rows into the data warehouse, or for more complex versions,
			// write newline delimited json or a parquet file to object storage, then trigger a load job.
			return nil
		},
		DataType: &CustomerWithScan{},
	}}

	vstreamClient, err := New(ctx, "bob2", conn, tables,
		WithMinFlushDuration(500*time.Millisecond),
		WithHeartbeatSeconds(1),
		WithStateTable("commerce", "vstreams"),
		WithEventFunc(func(ctx context.Context, ev *binlogdatapb.VEvent) error {
			fmt.Printf("** FIELD EVENT: %v\n", ev)
			return nil
		}, binlogdatapb.VEventType_FIELD),
	)
	if err != nil {
		t.Fatalf("failed to create VStreamClient: %v", err)
	}

	err = vstreamClient.Run(ctx)
	if err != nil && ctx.Err() == nil {
		t.Fatalf("failed to run vstreamclient: %v", err)
	}

	slices.SortFunc(gotCustomers, func(a, b *CustomerWithScan) int {
		return int(a.ID - b.ID)
	})

	wantCustomers := []*CustomerWithScan{
		{ID: 1, Email: "alice@domain.com"},
		{ID: 2, Email: "bob@domain.com"},
		{ID: 3, Email: "charlie@domain.com"},
		{ID: 4, Email: "dan@domain.com"},
		{ID: 5, Email: "eve@domain.com"},
	}

	fmt.Printf("got %d customers | flushed %d times\n", len(gotCustomers), flushCount)
	if !reflect.DeepEqual(gotCustomers, wantCustomers) {
		t.Fatalf("got %d customers, want %d", len(gotCustomers), len(wantCustomers))
	}
}
