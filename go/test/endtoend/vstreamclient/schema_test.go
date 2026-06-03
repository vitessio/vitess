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

type customerWithProjectedString struct {
	ID        int64  `vstream:"id"`
	Projected string `vstream:"projected_col"`
}

type customerWithProjectedPayload struct {
	ID      int64            `vstream:"id"`
	Payload *customerPayload `vstream:"projected_payload,json"`
}

type customerWithRemapColumns struct {
	ID     int64  `vstream:"id"`
	Email  string `vstream:"email"`
	RemapA string `vstream:"remap_a"`
	RemapB string `vstream:"remap_b"`
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

func TestVStreamClientSchemaDriftFailsWhenProjectedColumnDropped(t *testing.T) {
	te := newTestEnv(t)
	te.exec(t, "alter table customer.customer add column projected_col varchar(128) null", nil)
	t.Cleanup(func() {
		te.execBackgroundAllowMissingColumn(t, "alter table customer.customer drop column projected_col", nil)
	})

	var got []*customerWithProjectedString
	vstreamClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		Query:           "select id, projected_col from customer where id between 3300 and 3399",
		MaxRowsPerFlush: 1,
		DataType:        &customerWithProjectedString{},
		FlushFn: func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
			for _, row := range rows {
				got = append(got, row.Data.(*customerWithProjectedString))
			}
			return nil
		},
	}})

	runCtx, cancelRun, runErrCh := te.runAsync(vstreamClient, 4*time.Second)
	defer cancelRun()

	te.exec(t, "insert into customer.customer(id, email, projected_col) values (3301, 'projected-before-drop@domain.com', 'before-drop')", nil)
	assert.Eventually(t, func() bool {
		return len(got) == 1
	}, 3*time.Second, 50*time.Millisecond)
	assert.Equal(t, []*customerWithProjectedString{{ID: 3301, Projected: "before-drop"}}, got)

	te.exec(t, "alter table customer.customer drop column projected_col", nil)
	te.exec(t, "insert into customer.customer(id, email) values (3302, 'projected-after-drop@domain.com')", nil)

	err := <-runErrCh
	require.Error(t, err)
	assert.ErrorContains(t, err, "projected_col")
	_ = runCtx
}

func TestVStreamClientSchemaDriftFailsWhenProjectedColumnRenamed(t *testing.T) {
	te := newTestEnv(t)
	te.exec(t, "alter table customer.customer add column projected_col varchar(128) null", nil)
	t.Cleanup(func() {
		te.execBackgroundAllowMissingColumn(t, "alter table customer.customer drop column projected_col", nil)
		te.execBackgroundAllowMissingColumn(t, "alter table customer.customer drop column projected_col_renamed", nil)
	})

	var got []*customerWithProjectedString
	vstreamClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		Query:           "select id, projected_col from customer where id between 3400 and 3499",
		MaxRowsPerFlush: 1,
		DataType:        &customerWithProjectedString{},
		FlushFn: func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
			for _, row := range rows {
				got = append(got, row.Data.(*customerWithProjectedString))
			}
			return nil
		},
	}})

	runCtx, cancelRun, runErrCh := te.runAsync(vstreamClient, 4*time.Second)
	defer cancelRun()

	te.exec(t, "insert into customer.customer(id, email, projected_col) values (3401, 'projected-before-rename@domain.com', 'before-rename')", nil)
	assert.Eventually(t, func() bool {
		return len(got) == 1
	}, 3*time.Second, 50*time.Millisecond)
	assert.Equal(t, []*customerWithProjectedString{{ID: 3401, Projected: "before-rename"}}, got)

	te.exec(t, "alter table customer.customer change column projected_col projected_col_renamed varchar(128) null", nil)
	te.exec(t, "insert into customer.customer(id, email, projected_col_renamed) values (3402, 'projected-after-rename@domain.com', 'after-rename')", nil)

	err := <-runErrCh
	require.Error(t, err)
	assert.ErrorContains(t, err, "projected_col")
	_ = runCtx
}

func TestVStreamClientSchemaDriftFailsWhenProjectedColumnTypeChanges(t *testing.T) {
	te := newTestEnv(t)
	te.exec(t, "alter table customer.customer add column projected_payload text null", nil)
	t.Cleanup(func() {
		te.execBackgroundAllowMissingColumn(t, "alter table customer.customer drop column projected_payload", nil)
	})

	var got []*customerWithProjectedPayload
	vstreamClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		Query:           "select id, projected_payload from customer where id between 3500 and 3599",
		MaxRowsPerFlush: 1,
		DataType:        &customerWithProjectedPayload{},
		FlushFn: func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
			for _, row := range rows {
				got = append(got, row.Data.(*customerWithProjectedPayload))
			}
			return nil
		},
	}})

	runCtx, cancelRun, runErrCh := te.runAsync(vstreamClient, 4*time.Second)
	defer cancelRun()

	te.exec(t, `insert into customer.customer(id, email, projected_payload) values (3501, 'projected-before-type@domain.com', '{"source":"before-type"}')`, nil)
	assert.Eventually(t, func() bool {
		return len(got) == 1
	}, 3*time.Second, 50*time.Millisecond)
	assert.Equal(t, []*customerWithProjectedPayload{{ID: 3501, Payload: &customerPayload{Source: "before-type"}}}, got)

	te.exec(t, "update customer.customer set projected_payload = null where id = 3501", nil)
	te.exec(t, "alter table customer.customer modify column projected_payload bigint null", nil)
	te.exec(t, "insert into customer.customer(id, email, projected_payload) values (3502, 'projected-after-type@domain.com', 123)", nil)

	err := <-runErrCh
	require.Error(t, err)
	assert.ErrorContains(t, err, "projected_payload")
	assert.ErrorContains(t, err, "error unmarshalling JSON")
	_ = runCtx
}

func TestVStreamClientRemapsFieldsAfterDDL(t *testing.T) {
	te := newTestEnv(t)
	te.exec(t, "alter table customer.customer add column remap_a varchar(128) null, add column remap_b varchar(128) null", nil)
	t.Cleanup(func() {
		te.execBackgroundAllowMissingColumn(t, "alter table customer.customer drop column remap_a", nil)
		te.execBackgroundAllowMissingColumn(t, "alter table customer.customer drop column remap_b", nil)
	})

	var got []*customerWithRemapColumns
	vstreamClient := te.newDefaultClient(t, t.Name(), []vstreamclient.TableConfig{{
		Keyspace:        "customer",
		Table:           "customer",
		Query:           "select * from customer where id between 3600 and 3699",
		MaxRowsPerFlush: 1,
		DataType:        &customerWithRemapColumns{},
		FlushFn: func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
			for _, row := range rows {
				got = append(got, row.Data.(*customerWithRemapColumns))
			}
			return nil
		},
	}})

	runCtx, cancelRun, runErrCh := te.runAsync(vstreamClient, 4*time.Second)
	defer cancelRun()

	te.exec(t, "insert into customer.customer(id, email, remap_a, remap_b) values (3601, 'remap-before@domain.com', 'A1', 'B1')", nil)
	assert.Eventually(t, func() bool {
		return len(got) == 1
	}, 3*time.Second, 50*time.Millisecond)

	te.exec(t, "alter table customer.customer modify column remap_b varchar(128) null after id, modify column remap_a varchar(128) null after remap_b", nil)
	te.exec(t, "insert into customer.customer(id, email, remap_a, remap_b) values (3602, 'remap-after@domain.com', 'A2', 'B2')", nil)
	assert.Eventually(t, func() bool {
		return len(got) == 2
	}, 3*time.Second, 50*time.Millisecond)
	assert.Equal(t, []*customerWithRemapColumns{
		{ID: 3601, Email: "remap-before@domain.com", RemapA: "A1", RemapB: "B1"},
		{ID: 3602, Email: "remap-after@domain.com", RemapA: "A2", RemapB: "B2"},
	}, got)

	cancelRun()
	err := <-runErrCh
	if err != nil && runCtx.Err() == nil {
		t.Fatalf("failed to run vstreamclient: %v", err)
	}
}
