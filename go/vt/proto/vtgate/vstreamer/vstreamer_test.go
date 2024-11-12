package vstreamer

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

// Customer is the concrete type that will be built from the stream
type Customer struct {
	ID    int64  `vstream:"customer_id"`
	Email string `vstream:"email"`

	// the fields below aren't actually in the schema, but are added for illustrative purposes
	EmailConfirmed bool           `vstream:"email_confirmed"`
	Details        map[string]any `vstream:"details"`
	CreatedAt      time.Time      `vstream:"created_at"`
}

// To run the tests, this currently expects the local example to be running
// ./101_initial_cluster.sh; mysql < ../common/insert_commerce_data.sql; ./201_customer_tablets.sh; ./202_move_tables.sh; ./203_switch_reads.sh; ./204_switch_writes.sh; ./205_clean_commerce.sh; ./301_customer_sharded.sh; ./302_new_shards.sh; ./303_reshard.sh; ./304_switch_reads.sh; ./305_switch_writes.sh; ./306_down_shard_0.sh; ./307_delete_shard_0.sh
func TestVStreamer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := vtgateconn.Dial(ctx, "localhost:15991")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	flushCount := 0
	gotCustomers := make([]*Customer, 0)

	tables := []TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		MaxRowsPerFlush: 7,
		FlushFn: func(ctx context.Context, tx *sql.Tx, rows any, meta FlushMeta) error {
			flushCount++
			chunkCustomers := rows.([]*Customer)
			fmt.Printf("upserting %d customers\n", len(chunkCustomers))
			for i, customer := range chunkCustomers {
				fmt.Printf("upserting customer %d: %v\n", i, customer)
			}

			gotCustomers = append(gotCustomers, chunkCustomers...)
			return nil
		},
		DataType: Customer{},
	}}

	vstreamClient, err := New(ctx, conn, tables,
		WithMinFlushDuration(500*time.Millisecond),
		WithHeartbeatSeconds(1),
		WithStartingVGtid(&binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: "customer",
				Shard:    "-80",
				// Gtid "" is to stream from the start, "current" is to stream from the current gtid
				// you can also specify a gtid to start with.
				Gtid: "", // "current"  // "MySQL56/36a89abd-978f-11eb-b312-04ed332e05c2:1-265"
			}, {
				Keyspace: "customer",
				Shard:    "80-",
				Gtid:     "",
			}},
		}),
		WithEventFunc(func(ctx context.Context, ev *binlogdatapb.VEvent) error {
			fmt.Printf("** FIELD EVENT: %v\n", ev)
			return nil
		}, binlogdatapb.VEventType_FIELD),
	)
	if err != nil {
		t.Fatalf("failed to create vstreamer client: %v", err)
	}

	err = vstreamClient.Run(ctx)
	if err != nil && ctx.Err() == nil {
		t.Fatalf("failed to run vstreamer: %v", err)
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
}
