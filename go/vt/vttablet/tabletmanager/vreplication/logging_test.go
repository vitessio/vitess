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

package vreplication

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/log/logtest"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
	qh "vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication/queryhistory"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// TestPlayerApplyErrorLogsValuesAsAttributes checks that an error applying a
// row event logs the row's values only in attributes, with and without
// batching.
func TestPlayerApplyErrorLogsValuesAsAttributes(t *testing.T) {
	for _, batching := range []bool{true, false} {
		t.Run(fmt.Sprintf("batching=%t", batching), func(t *testing.T) {
			flags := vttablet.DefaultVReplicationConfig.ExperimentalFlags &^ vttablet.VReplicationExperimentalFlagVPlayerBatching
			if batching {
				flags |= vttablet.VReplicationExperimentalFlagVPlayerBatching
			}
			defer setExperimentalFlags(flags)()
			testPlayerApplyErrorLogs(t, batching)
		})
	}
}

func testPlayerApplyErrorLogs(t *testing.T, batching bool) {
	defer deleteTablet(addTablet(100))

	const value = "zq7applyerrorvalue"
	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.t1(id int, val varbinary(128), primary key(id), unique key(val))", vrepldb),
		fmt.Sprintf("insert into %s.t1 values (99, '%s')", vrepldb, value),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})

	logs := logtest.Capture(t, slog.LevelInfo)
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{Match: "t1"}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, id := startVReplication(t, bls, "")
	defer cancel()

	execStatements(t, []string{fmt.Sprintf("insert into t1 values (1, '%s')", value)})
	if batching {
		expectDBClientQueries(t, qh.Expect(
			"begin",
			"/insert into t1",
			"/update _vt.vreplication set message='error applying event: Duplicate",
			"/update _vt.vreplication set state='Error'",
		), "rollback")
	} else {
		expectDBClientQueries(t, qh.Expect(
			"begin",
			"/insert into t1",
			"rollback",
			"/update _vt.vreplication set message='error applying event for table t1 while processing position",
			"/update _vt.vreplication set state='Error'",
		))
	}

	record := logs.Wait(t, "Error applying event")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "test", record["workflow"])
	assert.EqualValues(t, id, record["stream_id"])
	assert.Contains(t, record["error"], value)
	if batching {
		// The error comes back from the commit, which is not a row event.
		assert.NotContains(t, record, "table")
	} else {
		assert.Equal(t, "t1", record["table"])
		assert.Regexp(t, `^MySQL56/.+`, record["processing_position"])
	}

	for _, record := range logs.Records(t) {
		msg, _ := record["msg"].(string)
		// execStatements runs through mysqlctl, which logs its queries.
		if strings.HasPrefix(msg, "exec ") {
			continue
		}
		assert.NotContains(t, msg, value)
	}
}
