/*
Copyright 2019 The Vitess Authors.

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

package schema

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func TestLoadTable(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	mockLoadTableQueries(db)
	table, err := newTestLoadTable("USER_TABLE", "test table", db)
	if err != nil {
		t.Fatal(err)
	}
	want := &Table{
		Name: sqlparser.NewIdentifierCS("test_table"),
		Fields: []*querypb.Field{{
			Name: "pk",
			Type: sqltypes.Int32,
		}, {
			Name: "name",
			Type: sqltypes.Int32,
		}, {
			Name: "addr",
			Type: sqltypes.Int32,
		}},
	}
	assert.Equal(t, want, table)
}

func TestLoadTableSequence(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	mockLoadTableQueries(db)
	table, err := newTestLoadTable("USER_TABLE", "vitess_sequence", db)
	if err != nil {
		t.Fatal(err)
	}
	want := &Table{
		Name:         sqlparser.NewIdentifierCS("test_table"),
		Type:         Sequence,
		SequenceInfo: &SequenceInfo{},
	}
	table.Fields = nil
	table.PKColumns = nil
	if !reflect.DeepEqual(table, want) {
		t.Errorf("Table:\n%#v, want\n%#v", table, want)
	}
}

func TestLoadTableMessage(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	mockMessageTableQueries(db)
	table, err := newTestLoadTable("USER_TABLE", "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30", db)
	if err != nil {
		t.Fatal(err)
	}
	want := &Table{
		Name: sqlparser.NewIdentifierCS("test_table"),
		Type: Message,
		Fields: []*querypb.Field{{
			Name: "id",
			Type: sqltypes.Int64,
		}, {
			Name: "priority",
			Type: sqltypes.Int64,
		}, {
			Name: "time_next",
			Type: sqltypes.Int64,
		}, {
			Name: "epoch",
			Type: sqltypes.Int64,
		}, {
			Name: "time_acked",
			Type: sqltypes.Int64,
		}, {
			Name: "message",
			Type: sqltypes.VarBinary,
		}},
		MessageInfo: &MessageInfo{
			Fields: []*querypb.Field{{
				Name: "id",
				Type: sqltypes.Int64,
			}, {
				Name: "message",
				Type: sqltypes.VarBinary,
			}},
			AckWaitDuration:    30 * time.Second,
			PurgeAfterDuration: 120 * time.Second,
			MinBackoff:         30 * time.Second,
			BatchSize:          1,
			CacheSize:          10,
			PollInterval:       30 * time.Second,
		},
	}
	assert.Equal(t, want, table)

	// Test loading min/max backoff
	table, err = newTestLoadTable("USER_TABLE", "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30,vt_min_backoff=10,vt_max_backoff=100", db)
	require.NoError(t, err)
	want.MessageInfo.MinBackoff = 10 * time.Second
	want.MessageInfo.MaxBackoff = 100 * time.Second
	assert.Equal(t, want, table)

	//
	// multiple tests for vt_message_cols
	//
	origFields := want.MessageInfo.Fields

	// Test loading id column from vt_message_cols
	table, err = newTestLoadTable("USER_TABLE", "vitess_message,vt_message_cols=id,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30,vt_min_backoff=10,vt_max_backoff=100", db)
	require.NoError(t, err)
	want.MessageInfo.Fields = []*querypb.Field{{
		Name: "id",
		Type: sqltypes.Int64,
	}}
	assert.Equal(t, want, table)

	// Test loading message column from vt_message_cols
	_, err = newTestLoadTable("USER_TABLE", "vitess_message,vt_message_cols=message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30,vt_min_backoff=10,vt_max_backoff=100", db)
	require.Equal(t, errors.New("vt_message_cols must begin with id: test_table"), err)

	// Test loading id & message columns from vt_message_cols
	table, err = newTestLoadTable("USER_TABLE", "vitess_message,vt_message_cols=id|message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30,vt_min_backoff=10,vt_max_backoff=100", db)
	require.NoError(t, err)
	want.MessageInfo.Fields = []*querypb.Field{{
		Name: "id",
		Type: sqltypes.Int64,
	}, {
		Name: "message",
		Type: sqltypes.VarBinary,
	}}
	assert.Equal(t, want, table)

	// Test loading id & message columns in reverse order from vt_message_cols
	_, err = newTestLoadTable("USER_TABLE", "vitess_message,vt_message_cols=message|id,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30,vt_min_backoff=10,vt_max_backoff=100", db)
	require.Equal(t, errors.New("vt_message_cols must begin with id: test_table"), err)

	// Test setting zero columns on vt_message_cols, which is ignored and loads the default columns
	table, err = newTestLoadTable("USER_TABLE", "vitess_message,vt_message_cols,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30,vt_min_backoff=10,vt_max_backoff=100", db)
	require.NoError(t, err)
	want.MessageInfo.Fields = []*querypb.Field{{
		Name: "id",
		Type: sqltypes.Int64,
	}, {
		Name: "message",
		Type: sqltypes.VarBinary,
	}}
	assert.Equal(t, want, table)

	// reset fields after all vt_message_cols tests
	want.MessageInfo.Fields = origFields

	//
	// end vt_message_cols tests
	//

	// Missing property
	_, err = newTestLoadTable("USER_TABLE", "vitess_message,vt_ack_wait=30", db)
	wanterr := "not specified for message table"
	if err == nil || !strings.Contains(err.Error(), wanterr) {
		t.Errorf("newTestLoadTable: %v, want %s", err, wanterr)
	}

	mockLoadTableQueries(db)
	_, err = newTestLoadTable("USER_TABLE", "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30", db)
	wanterr = "missing from message table: test_table"
	if err == nil || !strings.Contains(err.Error(), wanterr) {
		t.Errorf("newTestLoadTable: %v, must contain %s", err, wanterr)
	}
}

func newTestLoadTable(tableType string, comment string, db *fakesqldb.DB) (*Table, error) {
	ctx := context.Background()
	appParams := db.ConnParams()
	dbaParams := db.ConnParams()
	connPool := connpool.NewPool(tabletenv.NewEnv(nil, "SchemaTest"), "", tabletenv.ConnPoolConfig{
		Size:               2,
		IdleTimeoutSeconds: 10,
	})
	connPool.Open(appParams, dbaParams, appParams)
	conn, err := connPool.Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	return LoadTable(conn, "fakesqldb", "test_table", comment)
}

func mockLoadTableQueries(db *fakesqldb.DB) {
	db.ClearQueryPattern()
	db.MockQueriesForTable("test_table", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "pk",
			Type: sqltypes.Int32,
		}, {
			Name: "name",
			Type: sqltypes.Int32,
		}, {
			Name: "addr",
			Type: sqltypes.Int32,
		}},
	})
}

func mockMessageTableQueries(db *fakesqldb.DB) {
	db.ClearQueryPattern()
	db.MockQueriesForTable("test_table", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "id",
			Type: sqltypes.Int64,
		}, {
			Name: "priority",
			Type: sqltypes.Int64,
		}, {
			Name: "time_next",
			Type: sqltypes.Int64,
		}, {
			Name: "epoch",
			Type: sqltypes.Int64,
		}, {
			Name: "time_acked",
			Type: sqltypes.Int64,
		}, {
			Name: "message",
			Type: sqltypes.VarBinary,
		}},
	})
}
