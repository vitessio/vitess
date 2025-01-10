package vstreamclient

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vstreamclient"
)

type customerPayload struct {
	Source string `json:"source"`
}

type customerWithPayload struct {
	ID      int64            `vstream:"id"`
	Email   string           `vstream:"email"`
	Payload *customerPayload `vstream:"payload,json"`
}

// TestVStreamClientSchemaDriftFailsWithUnknownFields verifies strict configs
// fail after schema drift, which is important for consumers that require exact mappings.
func TestVStreamClientSchemaDriftFailsWithUnknownFields(t *testing.T) {
	te := newTestEnv(t)
	t.Cleanup(func() {
		te.execBackground(t, "alter table customer.customer drop column strict_extra", nil)
	})

	newClient := func() *vstreamclient.VStreamClient {
		return te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
			Keyspace:             "customer",
			Table:                "customer",
			Query:                "select * from customer where id between 2500 and 2599",
			MaxRowsPerFlush:      10,
			ErrorOnUnknownFields: true,
			DataType:             &Customer{},
			FlushFn:              func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error { return nil },
		}})
	}

	te.exec(t, "insert into customer.customer(id, email) values (2501, 'strict-before@domain.com')", nil)

	runCtx, cancelRun := context.WithTimeout(context.Background(), 2*time.Second)
	err := newClient().Run(runCtx)
	cancelRun()
	if err != nil && runCtx.Err() == nil {
		t.Fatalf("failed to run vstreamclient: %v", err)
	}

	te.exec(t, "alter table customer.customer add column strict_extra varchar(64) null", nil)
	te.exec(t, "insert into customer.customer(id, email, strict_extra) values (2502, 'strict-after@domain.com', 'extra')", nil)

	runCtx, cancelRun = context.WithTimeout(context.Background(), 2*time.Second)
	err = newClient().Run(runCtx)
	cancelRun()
	require.Error(t, err)
	assert.ErrorContains(t, err, "not found in provided data type")
}

// TestVStreamClientJSONDecodeFailure verifies a live streamed row fails when a
// field tagged with ,json contains invalid JSON, which is important because the
// stream should stop rather than silently checkpoint bad data.
func TestVStreamClientJSONDecodeFailure(t *testing.T) {
	te := newTestEnv(t)
	t.Cleanup(func() {
		te.execBackground(t, "alter table customer.customer drop column payload", nil)
	})

	te.exec(t, "alter table customer.customer add column payload text null", nil)
	te.exec(t, "insert into customer.customer(id, email, payload) values (2701, 'bad-json@domain.com', 'not-json')", nil)

	vstreamClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		Query:           "select id, email, payload from customer where id between 2700 and 2799",
		MaxRowsPerFlush: 10,
		DataType:        &customerWithPayload{},
		FlushFn:         func(_ context.Context, _ []vstreamclient.Row, _ vstreamclient.FlushMeta) error { return nil },
	}})

	runCtx, cancelRun := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelRun()

	err := vstreamClient.Run(runCtx)
	require.Error(t, err)
	assert.ErrorContains(t, err, "error unmarshalling JSON for field payload")
}
