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

package wrangler

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestShardMigrate tests table mode migrations.
// This has to be kept in sync with TestTableMigrate.
func TestStreamMigrateSimple(t *testing.T) {
	ctx := context.Background()
	tme := newTestShardMigrater(ctx, t)
	defer tme.stopTablets(t)

	sourceShards := []string{"-40", "40-"}
	targetShards := []string{"-80", "80-"}

	// Migrate reads
	err := tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_RDONLY, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}
	err = tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_REPLICA, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}

	// mi.checkJournals
	tme.dbSourceClients[0].addQuery("select val from _vt.resharding_journal where id=6432976123657117098", &sqltypes.Result{}, nil)
	tme.dbSourceClients[1].addQuery("select val from _vt.resharding_journal where id=6432976123657117098", &sqltypes.Result{}, nil)

	// sm.stopStreams->sm.readSourceStreams->readTabletStreams('Stopped')
	tme.dbSourceClients[0].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)
	tme.dbSourceClients[1].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)

	var sourceRows [][]string
	for _, sourceTargetShard := range sourceShards {
		var rows []string
		for j, sourceShard := range sourceShards {
			bls := &binlogdatapb.BinlogSource{
				Keyspace: "ks1",
				Shard:    sourceShard,
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: fmt.Sprintf("select * from t1 where in_keyrange('%s')", sourceTargetShard),
					}, {
						Match:  "t2",
						Filter: fmt.Sprintf("select * from t2 where in_keyrange('%s')", sourceTargetShard),
					}},
				},
			}
			rows = append(rows, fmt.Sprintf("%d|t1t2|%v|MariaDB/5-456-888", j+1, bls))
		}
		sourceRows = append(sourceRows, rows)
	}

	for i, dbclient := range tme.dbSourceClients {
		// sm.stopStreams->sm.readSourceStreams->readTabletStreams('') and VReplicationExec(_vt.copy_state)
		dbclient.addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"id|workflow|source|pos",
			"int64|varbinary|varchar|varbinary"),
			sourceRows[i]...),
			nil)
		dbclient.addQuery("select vrepl_id from _vt.copy_state where vrepl_id in (1, 2)", &sqltypes.Result{}, nil)

		// sm.stopStreams->sm.stopSourceStreams->VReplicationExec('Stopped')
		dbclient.addQuery("select id from _vt.vreplication where id in (1, 2)", resultid12, nil)
		dbclient.addQuery("update _vt.vreplication set state = 'Stopped', message = 'for cutover' where id in (1, 2)", &sqltypes.Result{}, nil)
		dbclient.addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		dbclient.addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)

		// sm.stopStreams->sm.stopSourceStreams->sm.readTabletStreams('id in...')
		dbclient.addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and id in (1, 2)", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"id|workflow|source|pos",
			"int64|varbinary|varchar|varbinary"),
			sourceRows[i]...),
			nil)
	}

	// mi.waitForCatchup-> mi.wr.tmc.VReplicationWaitForPos
	state := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"pos|state|message",
		"varchar|varchar|varchar"),
		"MariaDB/5-456-892|Running",
	)
	tme.dbTargetClients[0].addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
	tme.dbTargetClients[1].addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
	tme.dbTargetClients[0].addQuery("select pos, state, message from _vt.vreplication where id=2", state, nil)

	// mi.waitForCatchup-> mi.wr.tmc.VReplicationExec('stopped for cutover')
	tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where id = 1", resultid1, nil)
	tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (1)", &sqltypes.Result{}, nil)
	tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where id = 2", resultid2, nil)
	tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (2)", &sqltypes.Result{}, nil)
	tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where id = 1", resultid1, nil)
	tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (1)", &sqltypes.Result{}, nil)
	tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
	tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
	tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)

	// sm.migrateStreams->sm.createTargetStreams
	for i, targetShard := range targetShards {
		buf := &strings.Builder{}
		buf.WriteString("insert into.*vreplication")
		for _, sourceShard := range sourceShards {
			fmt.Fprintf(buf, ".*t1t2.*ks1.*%s.*t1.*in_keyrange.*%s.*t2.*in_keyrange.*%s.*MariaDB/5-456-888.*Stopped", sourceShard, targetShard, targetShard)
		}
		// Insert id is 3 so it doesn't overlap with the existing streams.
		tme.dbTargetClients[i].addQueryRE(buf.String(), &sqltypes.Result{InsertID: 3}, nil)
		tme.dbTargetClients[i].addQuery("select * from _vt.vreplication where id = 3", stoppedResult(3), nil)
		tme.dbTargetClients[i].addQuery("select * from _vt.vreplication where id = 4", stoppedResult(4), nil)
	}

	// mi.createJournals
	journal1 := "insert into _vt.resharding_journal.*6432976123657117098.*migration_type:SHARDS.*local_position.*MariaDB/5-456-892.*shard_gtids.*-80.*MariaDB/5-456-893.*participants.*40.*40"
	tme.dbSourceClients[0].addQueryRE(journal1, &sqltypes.Result{}, nil)
	journal2 := "insert into _vt.resharding_journal.*6432976123657117098.*migration_type:SHARDS.*local_position.*MariaDB/5-456-892.*shard_gtids.*80.*MariaDB/5-456-893.*shard_gtids.*80.*MariaDB/5-456-893.*participants.*40.*40"
	tme.dbSourceClients[1].addQueryRE(journal2, &sqltypes.Result{}, nil)

	// mi.createReverseReplication
	tme.dbSourceClients[0].addQueryRE("insert into _vt.vreplication.*-80.*-40.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
	tme.dbSourceClients[1].addQueryRE("insert into _vt.vreplication.*-80.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
	tme.dbSourceClients[1].addQueryRE("insert into _vt.vreplication.*80-.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 2}, nil)
	tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
	tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
	tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)

	// sm.finalize->Target
	tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1t2')", resultid34, nil)
	tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Running' where id in (3, 4)", &sqltypes.Result{}, nil)
	tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Running' where id in (3, 4)", &sqltypes.Result{}, nil)
	tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1t2')", resultid34, nil)
	tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Running' where id in (3, 4)", &sqltypes.Result{}, nil)

	// sm.finalize->Source
	tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1t2')", resultid12, nil)
	tme.dbSourceClients[0].addQuery("delete from _vt.vreplication where id in (1, 2)", &sqltypes.Result{}, nil)
	tme.dbSourceClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (1, 2)", &sqltypes.Result{}, nil)
	tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1t2')", resultid12, nil)
	tme.dbSourceClients[1].addQuery("delete from _vt.vreplication where id in (1, 2)", &sqltypes.Result{}, nil)
	tme.dbSourceClients[1].addQuery("delete from _vt.copy_state where vrepl_id in (1, 2)", &sqltypes.Result{}, nil)

	// mi.deleteTargetVReplication
	tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test'", resultid12, nil)
	tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test'", resultid1, nil)
	tme.dbTargetClients[0].addQuery("delete from _vt.vreplication where id in (1, 2)", &sqltypes.Result{}, nil)
	tme.dbTargetClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (1, 2)", &sqltypes.Result{}, nil)
	tme.dbTargetClients[1].addQuery("delete from _vt.vreplication where id in (1)", &sqltypes.Result{}, nil)
	tme.dbTargetClients[1].addQuery("delete from _vt.copy_state where vrepl_id in (1)", &sqltypes.Result{}, nil)

	journalID, err := tme.wr.MigrateWrites(ctx, tme.targetKeyspace, "test", 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if journalID != 6432976123657117098 {
		t.Errorf("journal id: %d, want 6432976123657117098", journalID)
	}

	checkServedTypes(t, tme.ts, "ks:-40", 0)
	checkServedTypes(t, tme.ts, "ks:40-", 0)
	checkServedTypes(t, tme.ts, "ks:-80", 3)
	checkServedTypes(t, tme.ts, "ks:80-", 3)

	checkIsMasterServing(t, tme.ts, "ks:-40", false)
	checkIsMasterServing(t, tme.ts, "ks:40-", false)
	checkIsMasterServing(t, tme.ts, "ks:-80", true)
	checkIsMasterServing(t, tme.ts, "ks:80-", true)

	verifyQueries(t, tme.allDBClients)
}
