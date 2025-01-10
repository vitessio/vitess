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

// TestVStreamClientResumesFromCheckpoint verifies that persisted stream state is
// used on a fresh client, so a second run only emits rows written after the
// first run checkpointed successfully.
func TestVStreamClientResumesFromCheckpoint(t *testing.T) {
	te := newTestEnv(t)
	streamName := t.Name()

	newClient := func(flushFn vstreamclient.FlushFunc) *vstreamclient.VStreamClient {
		return te.newDefaultClient(t, streamName, []vstreamclient.TableConfig{{
			Keyspace:        "customer",
			Table:           "customer",
			Query:           "select * from customer where id between 100 and 199",
			MaxRowsPerFlush: 2,
			DataType:        &Customer{},
			FlushFn:         flushFn,
		}})
	}

	insertCustomers := func(customers []*Customer) {
		for _, customer := range customers {
			te.exec(t, "insert into customer.customer(id, email) values(:id, :email)", customerBindVars(customer.ID, customer.Email))
		}
	}

	firstBatch := []*Customer{{ID: 101, Email: "resume-alice@domain.com"}, {ID: 102, Email: "resume-bob@domain.com"}}
	secondBatch := []*Customer{{ID: 103, Email: "resume-charlie@domain.com"}, {ID: 104, Email: "resume-dan@domain.com"}}

	insertCustomers(firstBatch)

	var firstRunCustomers []*Customer
	firstClient := newClient(func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
		for _, row := range rows {
			firstRunCustomers = append(firstRunCustomers, row.Data.(*Customer))
		}
		return nil
	})
	te.runUntilTimeout(t, firstClient, 2*time.Second)
	assert.ElementsMatch(t, firstBatch, firstRunCustomers)

	insertCustomers(secondBatch)

	var secondRunCustomers []*Customer
	secondClient := newClient(func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
		for _, row := range rows {
			secondRunCustomers = append(secondRunCustomers, row.Data.(*Customer))
		}
		return nil
	})
	te.runUntilTimeout(t, secondClient, 2*time.Second)
	assert.ElementsMatch(t, secondBatch, secondRunCustomers)
}

// TestVStreamClientResumesUpdateDeleteFromCheckpoint verifies checkpoint resume
// works for mutations after the initial copy, not just for new inserts.
func TestVStreamClientResumesUpdateDeleteFromCheckpoint(t *testing.T) {
	te := newTestEnv(t)

	newClient := func(flushFn vstreamclient.FlushFunc) *vstreamclient.VStreamClient {
		return te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
			Keyspace:        "customer",
			Table:           "customer",
			Query:           "select * from customer where id between 800 and 899",
			MaxRowsPerFlush: 1,
			DataType:        &Customer{},
			FlushFn:         flushFn,
		}})
	}

	te.exec(t, "insert into customer.customer(id, email) values (801, 'restart-update@domain.com'), (802, 'restart-delete@domain.com')", nil)
	te.runUntilTimeout(t, newClient(func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error { return nil }), 2*time.Second)

	te.exec(t, "update customer.customer set email = 'restart-update-new@domain.com' where id = 801", nil)
	te.exec(t, "delete from customer.customer where id = 802", nil)

	type mutation struct {
		ID      int64
		Email   string
		Deleted bool
	}
	var got []mutation
	client := newClient(func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
		for _, row := range rows {
			customer := row.Data.(*Customer)
			got = append(got, mutation{ID: customer.ID, Email: customer.Email, Deleted: row.RowChange.After == nil})
		}
		return nil
	})
	te.runUntilTimeout(t, client, 2*time.Second)

	assert.ElementsMatch(t, []mutation{{ID: 801, Email: "restart-update-new@domain.com", Deleted: false}, {ID: 802, Email: "restart-delete@domain.com", Deleted: true}}, got)
}

// TestVStreamClientRestartsInterruptedCopy verifies an interrupted copy phase is
// restarted from the beginning, which protects against partial initial loads.
func TestVStreamClientRestartsInterruptedCopy(t *testing.T) {
	te := newTestEnv(t)

	te.exec(t, "insert into customer.customer(id, email) values (1101, 'copy-a@domain.com'), (1102, 'copy-b@domain.com'), (1103, 'copy-c@domain.com')", nil)

	interruptErr := errors.New("interrupt copy before final completion")
	interruptedClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		Query:           "select * from customer where id between 1100 and 1199",
		MaxRowsPerFlush: 10,
		DataType:        &Customer{},
		FlushFn:         func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error { return nil },
	}},
		vstreamclient.WithEventFunc(func(_ context.Context, ev *binlogdatapb.VEvent) error {
			if ev.Keyspace != "" {
				return interruptErr
			}
			return nil
		}, binlogdatapb.VEventType_COPY_COMPLETED),
	)

	runCtx, cancelRun := context.WithTimeout(context.Background(), 2*time.Second)
	err := interruptedClient.Run(runCtx)
	cancelRun()
	require.Error(t, err)
	assert.ErrorContains(t, err, interruptErr.Error())

	var got []*Customer
	restartedClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		Query:           "select * from customer where id between 1100 and 1199",
		MaxRowsPerFlush: 10,
		DataType:        &Customer{},
		FlushFn: func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
			for _, row := range rows {
				got = append(got, row.Data.(*Customer))
			}
			return nil
		},
	}})

	te.runUntilTimeout(t, restartedClient, 2*time.Second)
	assert.ElementsMatch(t, []*Customer{{ID: 1101, Email: "copy-a@domain.com"}, {ID: 1102, Email: "copy-b@domain.com"}, {ID: 1103, Email: "copy-c@domain.com"}}, got)
}

// TestVStreamClientStartingVGtidOverridesState verifies an explicit starting
// VGtid wins over stored checkpoint state, which is important for replay control.
func TestVStreamClientStartingVGtidOverridesState(t *testing.T) {
	te := newTestEnv(t)

	newClient := func(flushFn vstreamclient.FlushFunc, opts ...vstreamclient.Option) *vstreamclient.VStreamClient {
		return te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
			Keyspace:        "customer",
			Table:           "customer",
			Query:           "select * from customer where id between 1500 and 1599",
			MaxRowsPerFlush: 1,
			DataType:        &Customer{},
			FlushFn:         flushFn,
		}}, opts...)
	}

	te.exec(t, "insert into customer.customer(id, email) values (1501, 'override-a@domain.com')", nil)
	te.runUntilTimeout(t, newClient(func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error { return nil }), 2*time.Second)
	vgtid1 := queryLatestVGtid(t, te.ctx, te.session, t.Name())

	te.exec(t, "insert into customer.customer(id, email) values (1502, 'override-b@domain.com')", nil)
	te.runUntilTimeout(t, newClient(func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error { return nil }), 2*time.Second)

	var replayed []*Customer
	te.runUntilTimeout(t, newClient(func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
		for _, row := range rows {
			replayed = append(replayed, row.Data.(*Customer))
		}
		return nil
	}, vstreamclient.WithStartingVGtid(vgtid1)), 2*time.Second)

	assert.Equal(t, []*Customer{{ID: 1502, Email: "override-b@domain.com"}}, replayed)
}
