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
	"reflect"
	"strings"
	"testing"
	"time"

	"context"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestLoadTable(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getTestLoadTableQueries() {
		db.AddQuery(query, result)
	}
	table, err := newTestLoadTable("USER_TABLE", "test table", db)
	if err != nil {
		t.Fatal(err)
	}
	want := &Table{
		Name: sqlparser.NewTableIdent("test_table"),
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
	for query, result := range getTestLoadTableQueries() {
		db.AddQuery(query, result)
	}
	table, err := newTestLoadTable("USER_TABLE", "vitess_sequence", db)
	if err != nil {
		t.Fatal(err)
	}
	want := &Table{
		Name:         sqlparser.NewTableIdent("test_table"),
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
	for query, result := range getMessageTableQueries() {
		db.AddQuery(query, result)
	}
	table, err := newTestLoadTable("USER_TABLE", "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30", db)
	if err != nil {
		t.Fatal(err)
	}
	want := &Table{
		Name: sqlparser.NewTableIdent("test_table"),
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

	// Missing property
	_, err = newTestLoadTable("USER_TABLE", "vitess_message,vt_ack_wait=30", db)
	wanterr := "not specified for message table"
	if err == nil || !strings.Contains(err.Error(), wanterr) {
		t.Errorf("newTestLoadTable: %v, want %s", err, wanterr)
	}

	for query, result := range getTestLoadTableQueries() {
		db.AddQuery(query, result)
	}
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
	conn, err := connPool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	return LoadTable(conn, "test_table", comment)
}

func getTestLoadTableQueries() map[string]*sqltypes.Result {
	return map[string]*sqltypes.Result{
		"select * from test_table where 1 != 1": {
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
		},
	}
}

func getMessageTableQueries() map[string]*sqltypes.Result {
	return map[string]*sqltypes.Result{
		"select * from test_table where 1 != 1": {
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
		},
	}
}
