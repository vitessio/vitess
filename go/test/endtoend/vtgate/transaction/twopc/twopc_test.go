/*
Copyright 2024 The Vitess Authors.

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

package transaction

import (
	"context"
	_ "embed"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

// TestDTCommit tests transaction commit using twopc mode
func TestDTCommit(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "fk_user", "")
	require.NoError(t, err)
	defer vtgateConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *binlogdatapb.VEvent)
	runVStream(t, ctx, ch, vtgateConn)

	// Insert into multiple shards
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into twopc_user(id, name) values(7,'foo')")
	utils.Exec(t, conn, "insert into twopc_user(id, name) values(8,'bar')")
	utils.Exec(t, conn, "insert into twopc_user(id, name) values(9,'baz')")
	utils.Exec(t, conn, "insert into twopc_user(id, name) values(10,'apa')")
	utils.Exec(t, conn, "commit")

	// Verify the values are present in multiple shards
	utils.Exec(t, conn, "use `ks/-80`")
	utils.AssertMatches(t, conn,
		"select id, name from twopc_user order by id",
		`[[INT64(8) VARCHAR("bar")] [INT64(10) VARCHAR("apa")]]`)
	utils.Exec(t, conn, "use `ks/80-`")
	utils.AssertMatches(t, conn,
		"select id, name from twopc_user order by id",
		`[[INT64(7) VARCHAR("foo")] [INT64(9) VARCHAR("baz")]]`)
	utils.Exec(t, conn, "use `ks`")

	// Update from multiple shard
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "update twopc_user set name='newfoo' where id = 7")
	utils.Exec(t, conn, "update twopc_user set name='newfoo' where id = 8")
	utils.Exec(t, conn, "commit")

	// VERIFY that values are updated
	utils.AssertMatches(t, conn,
		"select id, name from twopc_user order by id",
		`[[INT64(7) VARCHAR("newfoo")] [INT64(8) VARCHAR("newfoo")] [INT64(9) VARCHAR("baz")] [INT64(10) VARCHAR("apa")]]`)

	// DELETE from multiple shard
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "delete from twopc_user where id = 9")
	utils.Exec(t, conn, "delete from twopc_user where id = 10")
	utils.Exec(t, conn, "commit")

	// VERIFY that values are deleted
	utils.AssertMatches(t, conn, "select id, name from twopc_user order by id", `[[INT64(7) VARCHAR("newfoo")] [INT64(8) VARCHAR("newfoo")]]`)

	logTable := retrieveTransitions(t, ch)
	expectations := map[string][]string{
		"ks.twopc_user:-80": {
			`inserted:[INT64(8) VARCHAR("bar")]`,
			`inserted:[INT64(10) VARCHAR("apa")]`,
			`updated:[INT64(8) VARCHAR("newfoo")]`,
			`deleted:[INT64(10) VARCHAR("apa")]`,
		},
		"ks.twopc_user:80-": {
			`inserted:[INT64(7) VARCHAR("foo")]`,
			`inserted:[INT64(9) VARCHAR("baz")]`,
			`updated:[INT64(7) VARCHAR("newfoo")]`,
			`deleted:[INT64(9) VARCHAR("baz")]`,
		},
	}
	validateTransitions(t, logTable, expectations)

	log.Errorf("logTable: %+v", logTable)

}

func validateTransitions(t *testing.T, logTable map[string][]string, expectations map[string][]string) {
	for key, exp := range expectations {
		actual, exists := logTable[key]
		require.Truef(t, exists, "key %s not found in logTable", key)
		require.Equal(t, exp, actual)
	}
}

func retrieveTransitions(t *testing.T, ch chan *binlogdatapb.VEvent) map[string][]string {
	logTable := make(map[string][]string)

	tableMap := make(map[string][]*querypb.Field)
	keepWaiting := true
	for keepWaiting {
		select {
		case re := <-ch:
			if re.RowEvent != nil {
				shard := re.RowEvent.Shard
				tableName := re.RowEvent.TableName
				fields, ok := tableMap[tableName]
				require.Truef(t, ok, "table %s not found in fields map", tableName)
				for _, rc := range re.RowEvent.RowChanges {
					logEvent(logTable, shard, tableName, fields, rc)
				}
			}
			if re.FieldEvent != nil {
				tableMap[re.FieldEvent.TableName] = re.FieldEvent.Fields
			}
		case <-time.After(1 * time.Second):
			log.Errorf("%s", "stopping vstream")
			keepWaiting = false
		}
	}
	return logTable
}

func logEvent(logTable map[string][]string, shard string, tbl string, fields []*querypb.Field, rc *binlogdatapb.RowChange) {
	key := fmt.Sprintf("%s:%s", tbl, shard)
	logs := logTable[key]
	switch {
	case rc.Before == nil && rc.After == nil:
		panic("do not expect row event with both before and after nil")
	case rc.Before == nil:
		logs = append(logs, fmt.Sprintf("inserted:%v", sqltypes.MakeRowTrusted(fields, rc.After)))
	case rc.After == nil:
		logs = append(logs, fmt.Sprintf("deleted:%v", sqltypes.MakeRowTrusted(fields, rc.Before)))
	default:
		logs = append(logs, fmt.Sprintf("updated:%v", sqltypes.MakeRowTrusted(fields, rc.After)))
	}
	logTable[key] = logs
}

func runVStream(t *testing.T, ctx context.Context, ch chan *binlogdatapb.VEvent, vtgateConn *vtgateconn.VTGateConn) {
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{
			{Keyspace: keyspaceName, Shard: "-80", Gtid: "current"},
			{Keyspace: keyspaceName, Shard: "80-", Gtid: "current"},
		}}
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*/",
		}},
	}
	vReader, err := vtgateConn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, nil)
	require.NoError(t, err)

	// Use a channel to signal that the first VGTID event has been processed
	firstEventProcessed := make(chan struct{})
	var once sync.Once

	go func() {
		for {
			evs, err := vReader.Recv()
			if err == io.EOF || ctx.Err() != nil {
				return
			}
			require.NoError(t, err)

			for _, ev := range evs {
				// Signal the first event has been processed using sync.Once
				if ev.Type == binlogdatapb.VEventType_VGTID {
					once.Do(func() { close(firstEventProcessed) })
				}
				if ev.Type == binlogdatapb.VEventType_ROW || ev.Type == binlogdatapb.VEventType_FIELD {
					ch <- ev
				}
			}
		}
	}()

	// Wait for the first event to be processed
	<-firstEventProcessed
}
