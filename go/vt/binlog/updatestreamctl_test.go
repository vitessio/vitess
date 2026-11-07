/*
Copyright 2026 The Vitess Authors.

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

package binlog

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// TestStreamTablesLogs checks the update stream's log records when the
// binlog connection fails: positions are encoded and the error is its own
// attribute.
func TestStreamTablesLogs(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)

	updateStream := NewUpdateStream(nil, "ks", "cell", schema.NewEngineForTests(), sqlparser.NewTestParser())
	updateStream.cp = dbconfigs.New(&mysql.ConnParams{UnixSocket: "/nonexistent/mysql.sock", DbName: "vt_test_keyspace"})
	updateStream.Enable()
	t.Cleanup(updateStream.Disable)

	const position = "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-23"
	err := updateStream.StreamTables(t.Context(), position, []string{"t1"}, nil, func(*binlogdatapb.BinlogTransaction) error { return nil })
	require.Error(t, err)

	assert.Equal(t, "vt_test_keyspace", logs.Record(t, "Enabling update stream")["db_name"])
	assert.Equal(t, position, logs.Record(t, "ServeUpdateStream starting")["position"])
	ended := logs.Record(t, "stream ended")
	assert.Equal(t, position, ended["position"])
	assert.Equal(t, err.Error(), ended["error"])
}
