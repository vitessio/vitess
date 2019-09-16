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
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

func TestStreamMigrateMainflow(t *testing.T) {
	ctx := context.Background()
	tme := newTestShardMigrater(ctx, t, []string{"-40", "40-"}, []string{"-80", "80-"})
	defer tme.stopTablets(t)

	// Migrate reads
	err := tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_RDONLY, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}
	err = tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_REPLICA, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}

	tme.expectCheckJournals()

	stopStreams := func() {
		// sm.stopStreams->sm.readSourceStreams->readTabletStreams('Stopped')
		tme.dbSourceClients[0].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)

		// pre-compute sourceRows because they're re-read multiple times.
		var sourceRows [][]string
		for _, sourceTargetShard := range tme.sourceShards {
			var rows []string
			for j, sourceShard := range tme.sourceShards {
				bls := &binlogdatapb.BinlogSource{
					Keyspace: "ks",
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

			// sm.stopStreams->sm.verifyStreamPositions->sm.readTabletStreams('id in...')
			dbclient.addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and id in (1, 2)", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"id|workflow|source|pos",
				"int64|varbinary|varchar|varbinary"),
				sourceRows[i]...),
				nil)
		}
	}
	stopStreams()

	tme.expectWaitForCatchup()

	migrateStreams := func() {
		// sm.migrateStreams->->sm.deleteTargetStreams (no previously migrated streams)
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1t2')", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1t2')", &sqltypes.Result{}, nil)

		// sm.migrateStreams->sm.createTargetStreams
		for i, targetShard := range tme.targetShards {
			buf := &strings.Builder{}
			buf.WriteString("insert into.*vreplication")
			for _, sourceShard := range tme.sourceShards {
				fmt.Fprintf(buf, ".*t1t2.*ks.*%s.*t1.*in_keyrange.*%s.*t2.*in_keyrange.*%s.*MariaDB/5-456-888.*Stopped", sourceShard, targetShard, targetShard)
			}
			// Insert id is 3 so it doesn't overlap with the existing streams.
			tme.dbTargetClients[i].addQueryRE(buf.String(), &sqltypes.Result{InsertID: 3}, nil)
			tme.dbTargetClients[i].addQuery("select * from _vt.vreplication where id = 3", stoppedResult(3), nil)
			tme.dbTargetClients[i].addQuery("select * from _vt.vreplication where id = 4", stoppedResult(4), nil)
		}
	}
	migrateStreams()

	// mi.createJouranls (verify workflows are in the insert)
	journal := "insert into _vt.resharding_journal.*source_workflows.*t1t2"
	tme.dbSourceClients[0].addQueryRE(journal, &sqltypes.Result{}, nil)
	tme.dbSourceClients[1].addQueryRE(journal, &sqltypes.Result{}, nil)
	tme.expectCreateReverseReplication()

	finalize := func() {
		// sm.finalize->Target
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1t2')", resultid34, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1t2')", resultid34, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Running' where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Running' where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 3", stoppedResult(3), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 3", stoppedResult(3), nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 4", stoppedResult(4), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 4", stoppedResult(4), nil)

		// sm.finalize->Source
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1t2')", resultid12, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.vreplication where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1t2')", resultid12, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.vreplication where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.copy_state where vrepl_id in (1, 2)", &sqltypes.Result{}, nil)
	}
	finalize()

	tme.expectDeleteTargetVReplication()

	if _, err := tme.wr.MigrateWrites(ctx, tme.targetKeyspace, "test", 1*time.Second); err != nil {
		t.Fatal(err)
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

func TestStreamMigrateTwoStreams(t *testing.T) {
	ctx := context.Background()
	tme := newTestShardMigrater(ctx, t, []string{"-40", "40-"}, []string{"-80", "80-"})
	defer tme.stopTablets(t)

	// Migrate reads
	err := tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_RDONLY, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}
	err = tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_REPLICA, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}

	tme.expectCheckJournals()

	stopStreams := func() {
		// sm.stopStreams->sm.readSourceStreams->readTabletStreams('Stopped')
		tme.dbSourceClients[0].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)

		// pre-compute sourceRows because they're re-read multiple times.
		var sourceRows [][]string
		for _, sourceTargetShard := range tme.sourceShards {
			var rows []string
			for j, sourceShard := range tme.sourceShards {
				bls := &binlogdatapb.BinlogSource{
					Keyspace: "ks",
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
			for j, sourceShard := range tme.sourceShards {
				bls := &binlogdatapb.BinlogSource{
					Keyspace: "ks",
					Shard:    sourceShard,
					Filter: &binlogdatapb.Filter{
						Rules: []*binlogdatapb.Rule{{
							Match:  "t3",
							Filter: fmt.Sprintf("select * from t1 where in_keyrange('%s')", sourceTargetShard),
						}},
					},
				}
				rows = append(rows, fmt.Sprintf("%d|t3|%v|MariaDB/5-456-888", j+3, bls))
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
			dbclient.addQuery("select vrepl_id from _vt.copy_state where vrepl_id in (1, 2, 3, 4)", &sqltypes.Result{}, nil)

			// sm.stopStreams->sm.stopSourceStreams->VReplicationExec('Stopped')
			dbclient.addQuery("select id from _vt.vreplication where id in (1, 2, 3, 4)", resultid1234, nil)
			dbclient.addQuery("update _vt.vreplication set state = 'Stopped', message = 'for cutover' where id in (1, 2, 3, 4)", &sqltypes.Result{}, nil)
			dbclient.addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
			dbclient.addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
			dbclient.addQuery("select * from _vt.vreplication where id = 3", stoppedResult(3), nil)
			dbclient.addQuery("select * from _vt.vreplication where id = 4", stoppedResult(3), nil)

			// sm.stopStreams->sm.stopSourceStreams->sm.readTabletStreams('id in...')
			dbclient.addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and id in (1, 2, 3, 4)", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"id|workflow|source|pos",
				"int64|varbinary|varchar|varbinary"),
				sourceRows[i]...),
				nil)

			// sm.stopStreams->sm.verifyStreamPositions->sm.readTabletStreams('id in...')
			dbclient.addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and id in (1, 2, 3, 4)", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"id|workflow|source|pos",
				"int64|varbinary|varchar|varbinary"),
				sourceRows[i]...),
				nil)
		}
	}
	stopStreams()

	tme.expectWaitForCatchup()

	migrateStreams := func() {
		// sm.migrateStreams->->sm.deleteTargetStreams (no previously migrated streams)
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1t2', 't3')", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1t2', 't3')", &sqltypes.Result{}, nil)

		// sm.migrateStreams->sm.createTargetStreams
		for i, targetShard := range tme.targetShards {
			buf := &strings.Builder{}
			buf.WriteString("insert into.*vreplication")
			for _, sourceShard := range tme.sourceShards {
				fmt.Fprintf(buf, ".*t1t2.*ks.*%s.*t1.*in_keyrange.*%s.*t2.*in_keyrange.*%s.*MariaDB/5-456-888.*Stopped", sourceShard, targetShard, targetShard)
			}
			for _, sourceShard := range tme.sourceShards {
				fmt.Fprintf(buf, ".*t3.*ks.*%s.*t3.*in_keyrange.*%s.*MariaDB/5-456-888.*Stopped", sourceShard, targetShard)
			}
			// Insert id is 3 so it doesn't overlap with the existing streams.
			tme.dbTargetClients[i].addQueryRE(buf.String(), &sqltypes.Result{InsertID: 3}, nil)
			tme.dbTargetClients[i].addQuery("select * from _vt.vreplication where id = 3", stoppedResult(3), nil)
			tme.dbTargetClients[i].addQuery("select * from _vt.vreplication where id = 4", stoppedResult(4), nil)
			tme.dbTargetClients[i].addQuery("select * from _vt.vreplication where id = 5", stoppedResult(5), nil)
			tme.dbTargetClients[i].addQuery("select * from _vt.vreplication where id = 6", stoppedResult(6), nil)
		}
	}
	migrateStreams()

	tme.expectCreateJournals()
	tme.expectCreateReverseReplication()

	finalize := func() {
		// sm.finalize->Target
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1t2', 't3')", resultid3456, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1t2', 't3')", resultid3456, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Running' where id in (3, 4, 5, 6)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Running' where id in (3, 4, 5, 6)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 3", stoppedResult(3), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 3", stoppedResult(3), nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 4", stoppedResult(4), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 4", stoppedResult(4), nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 5", stoppedResult(5), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 5", stoppedResult(5), nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 6", stoppedResult(6), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 6", stoppedResult(6), nil)

		// sm.finalize->Source
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1t2', 't3')", resultid1234, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.vreplication where id in (1, 2, 3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (1, 2, 3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1t2', 't3')", resultid1234, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.vreplication where id in (1, 2, 3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.copy_state where vrepl_id in (1, 2, 3, 4)", &sqltypes.Result{}, nil)
	}
	finalize()

	tme.expectDeleteTargetVReplication()

	if _, err := tme.wr.MigrateWrites(ctx, tme.targetKeyspace, "test", 1*time.Second); err != nil {
		t.Fatal(err)
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

func TestStreamMigrateOneToMany(t *testing.T) {
	ctx := context.Background()
	tme := newTestShardMigrater(ctx, t, []string{"0"}, []string{"-80", "80-"})
	defer tme.stopTablets(t)

	// Migrate reads
	err := tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_RDONLY, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}
	err = tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_REPLICA, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}

	tme.expectCheckJournals()

	stopStreams := func() {
		// sm.stopStreams->sm.readSourceStreams->readTabletStreams('Stopped')
		tme.dbSourceClients[0].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)

		// pre-compute sourceRows because they're re-read multiple times.
		var sourceRows [][]string
		for _, sourceTargetShard := range tme.sourceShards {
			var rows []string
			for j, sourceShard := range tme.sourceShards {
				bls := &binlogdatapb.BinlogSource{
					Keyspace: "ks",
					Shard:    sourceShard,
					Filter: &binlogdatapb.Filter{
						Rules: []*binlogdatapb.Rule{{
							Match:  "t1",
							Filter: fmt.Sprintf("select * from t1 where in_keyrange('%s')", sourceTargetShard),
						}},
					},
				}
				rows = append(rows, fmt.Sprintf("%d|t1|%v|MariaDB/5-456-888", j+1, bls))
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
			dbclient.addQuery("select vrepl_id from _vt.copy_state where vrepl_id in (1)", &sqltypes.Result{}, nil)

			// sm.stopStreams->sm.stopSourceStreams->VReplicationExec('Stopped')
			dbclient.addQuery("select id from _vt.vreplication where id in (1)", resultid1, nil)
			dbclient.addQuery("update _vt.vreplication set state = 'Stopped', message = 'for cutover' where id in (1)", &sqltypes.Result{}, nil)
			dbclient.addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)

			// sm.stopStreams->sm.stopSourceStreams->sm.readTabletStreams('id in...')
			dbclient.addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and id in (1)", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"id|workflow|source|pos",
				"int64|varbinary|varchar|varbinary"),
				sourceRows[i]...),
				nil)

			// sm.stopStreams->sm.verifyStreamPositions->sm.readTabletStreams('id in...')
			dbclient.addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and id in (1)", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"id|workflow|source|pos",
				"int64|varbinary|varchar|varbinary"),
				sourceRows[i]...),
				nil)
		}
	}
	stopStreams()

	tme.expectWaitForCatchup()

	migrateStreams := func() {
		// sm.migrateStreams->->sm.deleteTargetStreams (no previously migrated streams)
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", &sqltypes.Result{}, nil)

		// sm.migrateStreams->sm.createTargetStreams
		for i, targetShard := range tme.targetShards {
			buf := &strings.Builder{}
			buf.WriteString("insert into.*vreplication")
			for _, sourceShard := range tme.sourceShards {
				fmt.Fprintf(buf, ".*t1.*ks.*%s.*t1.*in_keyrange.*%s.*MariaDB/5-456-888.*Stopped", sourceShard, targetShard)
			}
			// Insert id is 3 so it doesn't overlap with the existing streams.
			tme.dbTargetClients[i].addQueryRE(buf.String(), &sqltypes.Result{InsertID: 3}, nil)
			tme.dbTargetClients[i].addQuery("select * from _vt.vreplication where id = 3", stoppedResult(3), nil)
		}
	}
	migrateStreams()

	tme.expectCreateJournals()
	tme.expectCreateReverseReplication()

	finalize := func() {
		// sm.finalize->Target
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", resultid3, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", resultid3, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Running' where id in (3)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Running' where id in (3)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 3", stoppedResult(3), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 3", stoppedResult(3), nil)

		// sm.finalize->Source
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", resultid1, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.vreplication where id in (1)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (1)", &sqltypes.Result{}, nil)
	}
	finalize()

	tme.expectDeleteTargetVReplication()

	if _, err := tme.wr.MigrateWrites(ctx, tme.targetKeyspace, "test", 1*time.Second); err != nil {
		t.Fatal(err)
	}

	checkServedTypes(t, tme.ts, "ks:0", 0)
	checkServedTypes(t, tme.ts, "ks:-80", 3)
	checkServedTypes(t, tme.ts, "ks:80-", 3)

	checkIsMasterServing(t, tme.ts, "ks:0", false)
	checkIsMasterServing(t, tme.ts, "ks:-80", true)
	checkIsMasterServing(t, tme.ts, "ks:80-", true)

	verifyQueries(t, tme.allDBClients)
}

func TestStreamMigrateManyToOne(t *testing.T) {
	ctx := context.Background()
	// Interesting tidbit: you cannot create a shard "0" for an already sharded keyspace.
	tme := newTestShardMigrater(ctx, t, []string{"-80", "80-"}, []string{"-"})
	defer tme.stopTablets(t)

	// Migrate reads
	err := tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_RDONLY, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}
	err = tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_REPLICA, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}

	tme.expectCheckJournals()

	stopStreams := func() {
		// sm.stopStreams->sm.readSourceStreams->readTabletStreams('Stopped')
		tme.dbSourceClients[0].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)

		// pre-compute sourceRows because they're re-read multiple times.
		var sourceRows [][]string
		for _, sourceTargetShard := range tme.sourceShards {
			var rows []string
			for j, sourceShard := range tme.sourceShards {
				bls := &binlogdatapb.BinlogSource{
					Keyspace: "ks",
					Shard:    sourceShard,
					Filter: &binlogdatapb.Filter{
						Rules: []*binlogdatapb.Rule{{
							Match:  "t1",
							Filter: fmt.Sprintf("select * from t1 where in_keyrange('%s')", sourceTargetShard),
						}},
					},
				}
				rows = append(rows, fmt.Sprintf("%d|t1|%v|MariaDB/5-456-888", j+1, bls))
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

			// sm.stopStreams->sm.verifyStreamPositions->sm.readTabletStreams('id in...')
			dbclient.addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and id in (1, 2)", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"id|workflow|source|pos",
				"int64|varbinary|varchar|varbinary"),
				sourceRows[i]...),
				nil)
		}
	}
	stopStreams()

	tme.expectWaitForCatchup()

	migrateStreams := func() {
		// sm.migrateStreams->->sm.deleteTargetStreams (no previously migrated streams)
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", &sqltypes.Result{}, nil)

		// sm.migrateStreams->sm.createTargetStreams
		for i, targetShard := range tme.targetShards {
			buf := &strings.Builder{}
			buf.WriteString("insert into.*vreplication")
			for _, sourceShard := range tme.sourceShards {
				fmt.Fprintf(buf, ".*t1.*ks.*%s.*t1.*in_keyrange.*%s.*MariaDB/5-456-888.*Stopped", sourceShard, targetShard)
			}
			// Insert id is 3 so it doesn't overlap with the existing streams.
			tme.dbTargetClients[i].addQueryRE(buf.String(), &sqltypes.Result{InsertID: 3}, nil)
			tme.dbTargetClients[i].addQuery("select * from _vt.vreplication where id = 3", stoppedResult(3), nil)
			tme.dbTargetClients[i].addQuery("select * from _vt.vreplication where id = 4", stoppedResult(4), nil)
		}
	}
	migrateStreams()

	tme.expectCreateJournals()
	tme.expectCreateReverseReplication()

	finalize := func() {
		// sm.finalize->Target
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", resultid34, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Running' where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 3", stoppedResult(3), nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 4", stoppedResult(4), nil)

		// sm.finalize->Source
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", resultid12, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.vreplication where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", resultid12, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.vreplication where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.copy_state where vrepl_id in (1, 2)", &sqltypes.Result{}, nil)
	}
	finalize()

	tme.expectDeleteTargetVReplication()

	if _, err := tme.wr.MigrateWrites(ctx, tme.targetKeyspace, "test", 1*time.Second); err != nil {
		t.Fatal(err)
	}

	checkServedTypes(t, tme.ts, "ks:-80", 0)
	checkServedTypes(t, tme.ts, "ks:80-", 0)
	checkServedTypes(t, tme.ts, "ks:-", 3)

	checkIsMasterServing(t, tme.ts, "ks:-80", false)
	checkIsMasterServing(t, tme.ts, "ks:80-", false)
	checkIsMasterServing(t, tme.ts, "ks:-", true)

	verifyQueries(t, tme.allDBClients)
}

func TestStreamMigrateSyncSuccess(t *testing.T) {
	ctx := context.Background()
	tme := newTestShardMigrater(ctx, t, []string{"-40", "40-"}, []string{"-80", "80-"})
	defer tme.stopTablets(t)

	// Migrate reads
	err := tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_RDONLY, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}
	err = tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_REPLICA, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}

	tme.expectCheckJournals()

	stopStreams := func() {
		// sm.stopStreams->sm.readSourceStreams->readTabletStreams('Stopped')
		tme.dbSourceClients[0].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)

		var sourceRows [][]string
		for i, sourceTargetShard := range tme.sourceShards {
			var rows []string
			for j, sourceShard := range tme.sourceShards {
				bls := &binlogdatapb.BinlogSource{
					Keyspace: "ks",
					Shard:    sourceShard,
					Filter: &binlogdatapb.Filter{
						Rules: []*binlogdatapb.Rule{{
							Match:  "t1",
							Filter: fmt.Sprintf("select * from t1 where in_keyrange('%s')", sourceTargetShard),
						}},
					},
				}
				switch i {
				case 0:
					switch j {
					case 0:
						rows = append(rows, fmt.Sprintf("%d|t1|%v|MariaDB/5-456-887", j+1, bls))
					case 1:
						rows = append(rows, fmt.Sprintf("%d|t1|%v|MariaDB/5-456-888", j+1, bls))
					}
				case 1:
					switch j {
					case 0:
						rows = append(rows, fmt.Sprintf("%d|t1|%v|MariaDB/5-456-888", j+1, bls))
					case 1:
						rows = append(rows, fmt.Sprintf("%d|t1|%v|MariaDB/5-456-887", j+1, bls))
					}
				}
			}
			sourceRows = append(sourceRows, rows)
		}
		var finalSources [][]string
		for _, sourceTargetShard := range tme.sourceShards {
			var rows []string
			for j, sourceShard := range tme.sourceShards {
				bls := &binlogdatapb.BinlogSource{
					Keyspace: "ks",
					Shard:    sourceShard,
					Filter: &binlogdatapb.Filter{
						Rules: []*binlogdatapb.Rule{{
							Match:  "t1",
							Filter: fmt.Sprintf("select * from t1 where in_keyrange('%s')", sourceTargetShard),
						}},
					},
				}
				rows = append(rows, fmt.Sprintf("%d|t1|%v|MariaDB/5-456-888", j+1, bls))
			}
			finalSources = append(finalSources, rows)
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

			// sm.stopStreams->sm.verifyStreamPositions->sm.readTabletStreams('id in...')
			dbclient.addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and id in (1, 2)", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"id|workflow|source|pos",
				"int64|varbinary|varchar|varbinary"),
				finalSources[i]...),
				nil)
		}
	}
	stopStreams()

	// sm.stopStreams->sm.syncSourceStreams: Note that this happens inside stopStreams before verifyStreamPositions.
	syncSourceStreams := func() {
		reached := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"pos|state|message",
			"varbinary|varbinary|varbinary"),
			"MariaDB/5-456-888|Running|",
		)
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where id = 1", resultid1, nil)
		tme.dbSourceClients[0].addQuery("update _vt.vreplication set state = 'Running', stop_pos = 'MariaDB/5-456-888', message = 'synchronizing for cutover' where id in (1)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("select pos, state, message from _vt.vreplication where id=1", reached, nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where id = 2", resultid2, nil)
		tme.dbSourceClients[1].addQuery("update _vt.vreplication set state = 'Running', stop_pos = 'MariaDB/5-456-888', message = 'synchronizing for cutover' where id in (2)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select pos, state, message from _vt.vreplication where id=2", reached, nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	}
	syncSourceStreams()

	tme.expectWaitForCatchup()

	migrateStreams := func() {
		// sm.migrateStreams->->sm.deleteTargetStreams (no previously migrated streams)
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", &sqltypes.Result{}, nil)

		// sm.migrateStreams->sm.createTargetStreams
		for i, targetShard := range tme.targetShards {
			buf := &strings.Builder{}
			buf.WriteString("insert into.*vreplication")
			for _, sourceShard := range tme.sourceShards {
				fmt.Fprintf(buf, ".*t1.*ks.*%s.*t1.*in_keyrange.*%s.*MariaDB/5-456-888.*Stopped", sourceShard, targetShard)
			}
			// Insert id is 3 so it doesn't overlap with the existing streams.
			tme.dbTargetClients[i].addQueryRE(buf.String(), &sqltypes.Result{InsertID: 3}, nil)
			tme.dbTargetClients[i].addQuery("select * from _vt.vreplication where id = 3", stoppedResult(3), nil)
			tme.dbTargetClients[i].addQuery("select * from _vt.vreplication where id = 4", stoppedResult(4), nil)
		}
	}
	migrateStreams()

	tme.expectCreateJournals()
	tme.expectCreateReverseReplication()

	finalize := func() {
		// sm.finalize->Target
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", resultid34, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", resultid34, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Running' where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Running' where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 3", stoppedResult(3), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 3", stoppedResult(3), nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 4", stoppedResult(4), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 4", stoppedResult(4), nil)

		// sm.finalize->Source
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", resultid12, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.vreplication where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", resultid12, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.vreplication where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.copy_state where vrepl_id in (1, 2)", &sqltypes.Result{}, nil)
	}
	finalize()

	tme.expectDeleteTargetVReplication()

	if _, err := tme.wr.MigrateWrites(ctx, tme.targetKeyspace, "test", 1*time.Second); err != nil {
		t.Fatal(err)
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

func TestStreamMigrateSyncFail(t *testing.T) {
	ctx := context.Background()
	tme := newTestShardMigrater(ctx, t, []string{"-40", "40-"}, []string{"-80", "80-"})
	defer tme.stopTablets(t)

	// Migrate reads
	err := tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_RDONLY, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}
	err = tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_REPLICA, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}

	tme.expectCheckJournals()

	stopStreams := func() {
		// sm.stopStreams->sm.readSourceStreams->readTabletStreams('Stopped')
		tme.dbSourceClients[0].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)

		var sourceRows [][]string
		for i, sourceTargetShard := range tme.sourceShards {
			var rows []string
			for j, sourceShard := range tme.sourceShards {
				bls := &binlogdatapb.BinlogSource{
					Keyspace: "ks",
					Shard:    sourceShard,
					Filter: &binlogdatapb.Filter{
						Rules: []*binlogdatapb.Rule{{
							Match:  "t1",
							Filter: fmt.Sprintf("select * from t1 where in_keyrange('%s')", sourceTargetShard),
						}},
					},
				}
				switch i {
				case 0:
					switch j {
					case 0:
						rows = append(rows, fmt.Sprintf("%d|t1|%v|MariaDB/5-456-887", j+1, bls))
					case 1:
						rows = append(rows, fmt.Sprintf("%d|t1|%v|MariaDB/5-456-888", j+1, bls))
					}
				case 1:
					switch j {
					case 0:
						rows = append(rows, fmt.Sprintf("%d|t1|%v|MariaDB/5-456-888", j+1, bls))
					case 1:
						rows = append(rows, fmt.Sprintf("%d|t1|%v|MariaDB/5-456-887", j+1, bls))
					}
				}
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

			// sm.stopStreams->sm.verifyStreamPositions->sm.readTabletStreams('id in...')
			dbclient.addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and id in (1, 2)", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"id|workflow|source|pos",
				"int64|varbinary|varchar|varbinary"),
				sourceRows[i]...),
				nil)

			// sm.cancelMigration->sm.readSourceStreamsForCancel: this is not actually stopStream, but we're reusing the bls here.
			dbclient.addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"id|workflow|source|pos",
				"int64|varbinary|varchar|varbinary"),
				sourceRows[i]...),
				nil)
		}
	}
	stopStreams()

	// sm.stopStreams->sm.syncSourceStreams: Note that this happens inside stopStreams before verifyStreamPositions.
	syncSourceStreams := func() {
		reached := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"pos|state|message",
			"varbinary|varbinary|varbinary"),
			"MariaDB/5-456-888|Running|",
		)
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where id = 1", resultid1, nil)
		tme.dbSourceClients[0].addQuery("update _vt.vreplication set state = 'Running', stop_pos = 'MariaDB/5-456-888', message = 'synchronizing for cutover' where id in (1)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("select pos, state, message from _vt.vreplication where id=1", reached, nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where id = 2", resultid2, nil)
		tme.dbSourceClients[1].addQuery("update _vt.vreplication set state = 'Running', stop_pos = 'MariaDB/5-456-888', message = 'synchronizing for cutover' where id in (2)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select pos, state, message from _vt.vreplication where id=2", reached, nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	}
	syncSourceStreams()

	tme.expectWaitForCatchup()

	smCancelMigration := func() {
		// sm.migrateStreams->sm.deleteTargetStreams
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", resultid34, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", resultid34, nil)
		tme.dbTargetClients[0].addQuery("delete from _vt.vreplication where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("delete from _vt.vreplication where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("delete from _vt.copy_state where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)

		// sm.migrateStreams->->restart source streams
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", resultid12, nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", resultid12, nil)
		tme.dbSourceClients[0].addQuery("update _vt.vreplication set state = 'Running', stop_pos = null, message = '' where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("update _vt.vreplication set state = 'Running', stop_pos = null, message = '' where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 1", runningResult(1), nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 1", runningResult(1), nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 2", runningResult(2), nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 2", runningResult(2), nil)
	}
	smCancelMigration()

	tme.expectCancelMigration()

	_, err = tme.wr.MigrateWrites(ctx, tme.targetKeyspace, "test", 1*time.Second)
	want := "does not match"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("MigrateWrites err: %v, want %s", err, want)
	}
}

func TestStreamMigrateCancel(t *testing.T) {
	ctx := context.Background()
	tme := newTestShardMigrater(ctx, t, []string{"-40", "40-"}, []string{"-80", "80-"})
	defer tme.stopTablets(t)

	// Migrate reads
	err := tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_RDONLY, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}
	err = tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_REPLICA, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}

	tme.expectCheckJournals()

	stopStreamsFail := func() {
		// sm.stopStreams->sm.readSourceStreams->readTabletStreams('Stopped')
		tme.dbSourceClients[0].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)

		// pre-compute sourceRows because they're re-read multiple times.
		var sourceRows [][]string
		for _, sourceTargetShard := range tme.sourceShards {
			var rows []string
			for j, sourceShard := range tme.sourceShards {
				bls := &binlogdatapb.BinlogSource{
					Keyspace: "ks",
					Shard:    sourceShard,
					Filter: &binlogdatapb.Filter{
						Rules: []*binlogdatapb.Rule{{
							Match:  "t1",
							Filter: fmt.Sprintf("select * from t1 where in_keyrange('%s')", sourceTargetShard),
						}},
					},
				}
				rows = append(rows, fmt.Sprintf("%d|t1|%v|MariaDB/5-456-888", j+1, bls))
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

			// sm.stopStreams->sm.stopSourceStreams->VReplicationExec('Stopped'): fail this
			dbclient.addQuery("select id from _vt.vreplication where id in (1, 2)", nil, fmt.Errorf("intentionally failed"))

			// sm.cancelMigration->sm.readSourceStreamsForCancel: this is not actually stopStream, but we're reusing the bls here.
			dbclient.addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"id|workflow|source|pos",
				"int64|varbinary|varchar|varbinary"),
				sourceRows[i]...),
				nil)
		}
	}
	stopStreamsFail()

	smCancelMigration := func() {
		// sm.migrateStreams->sm.deleteTargetStreams
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", resultid34, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", resultid34, nil)
		tme.dbTargetClients[0].addQuery("delete from _vt.vreplication where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("delete from _vt.vreplication where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("delete from _vt.copy_state where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)

		// sm.migrateStreams->->restart source streams
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", resultid12, nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow in ('t1')", resultid12, nil)
		tme.dbSourceClients[0].addQuery("update _vt.vreplication set state = 'Running', stop_pos = null, message = '' where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("update _vt.vreplication set state = 'Running', stop_pos = null, message = '' where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 1", runningResult(1), nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 1", runningResult(1), nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 2", runningResult(2), nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 2", runningResult(2), nil)
	}
	smCancelMigration()

	tme.expectCancelMigration()

	_, err = tme.wr.MigrateWrites(ctx, tme.targetKeyspace, "test", 1*time.Second)
	want := "intentionally failed"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("MigrateWrites err: %v, want %s", err, want)
	}

	checkServedTypes(t, tme.ts, "ks:-40", 1)
	checkServedTypes(t, tme.ts, "ks:40-", 1)
	checkServedTypes(t, tme.ts, "ks:-80", 2)
	checkServedTypes(t, tme.ts, "ks:80-", 2)

	checkIsMasterServing(t, tme.ts, "ks:-40", true)
	checkIsMasterServing(t, tme.ts, "ks:40-", true)
	checkIsMasterServing(t, tme.ts, "ks:-80", false)
	checkIsMasterServing(t, tme.ts, "ks:80-", false)

	verifyQueries(t, tme.allDBClients)
}

func TestStreamMigrateStoppedStreams(t *testing.T) {
	ctx := context.Background()
	tme := newTestShardMigrater(ctx, t, []string{"0"}, []string{"-80", "80-"})
	defer tme.stopTablets(t)

	// Migrate reads
	err := tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_RDONLY, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}
	err = tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_REPLICA, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}

	tme.expectCheckJournals()

	stopStreams := func() {
		// sm.stopStreams->sm.readSourceStreams->readTabletStreams('Stopped'): returns non-empty
		var sourceRows [][]string
		for _, sourceTargetShard := range tme.sourceShards {
			var rows []string
			for j, sourceShard := range tme.sourceShards {
				bls := &binlogdatapb.BinlogSource{
					Keyspace: "ks",
					Shard:    sourceShard,
					Filter: &binlogdatapb.Filter{
						Rules: []*binlogdatapb.Rule{{
							Match:  "t1",
							Filter: fmt.Sprintf("select * from t1 where in_keyrange('%s')", sourceTargetShard),
						}},
					},
				}
				rows = append(rows, fmt.Sprintf("%d|t1|%v|MariaDB/5-456-888", j+1, bls))
			}
			sourceRows = append(sourceRows, rows)
		}

		for i, dbclient := range tme.dbSourceClients {
			// sm.stopStreams->sm.readSourceStreams->readTabletStreams('') and VReplicationExec(_vt.copy_state)
			dbclient.addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"id|workflow|source|pos",
				"int64|varbinary|varchar|varbinary"),
				sourceRows[i]...),
				nil)
			dbclient.addQuery("select vrepl_id from _vt.copy_state where vrepl_id in (1)", &sqltypes.Result{}, nil)
		}
	}
	stopStreams()

	_, err = tme.wr.MigrateWrites(ctx, tme.targetKeyspace, "test", 1*time.Second)
	want := "cannot migrate until all strems are running: 0"
	if err == nil || err.Error() != want {
		t.Errorf("MigrateWrites err: %v, want %v", err, want)
	}
}

func TestStreamMigrateStillCopying(t *testing.T) {
	ctx := context.Background()
	tme := newTestShardMigrater(ctx, t, []string{"0"}, []string{"-80", "80-"})
	defer tme.stopTablets(t)

	// Migrate reads
	err := tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_RDONLY, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}
	err = tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_REPLICA, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}

	tme.expectCheckJournals()

	stopStreams := func() {
		// sm.stopStreams->sm.readSourceStreams->readTabletStreams('Stopped')
		tme.dbSourceClients[0].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)

		// pre-compute sourceRows because they're re-read multiple times.
		var sourceRows [][]string
		for _, sourceTargetShard := range tme.sourceShards {
			var rows []string
			for j, sourceShard := range tme.sourceShards {
				bls := &binlogdatapb.BinlogSource{
					Keyspace: "ks",
					Shard:    sourceShard,
					Filter: &binlogdatapb.Filter{
						Rules: []*binlogdatapb.Rule{{
							Match:  "t1",
							Filter: fmt.Sprintf("select * from t1 where in_keyrange('%s')", sourceTargetShard),
						}},
					},
				}
				rows = append(rows, fmt.Sprintf("%d|t1|%v|MariaDB/5-456-888", j+1, bls))
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
			dbclient.addQuery("select vrepl_id from _vt.copy_state where vrepl_id in (1)", resultid1, nil)
		}
	}
	stopStreams()

	_, err = tme.wr.MigrateWrites(ctx, tme.targetKeyspace, "test", 1*time.Second)
	want := "cannot migrate while vreplication streams in source shards are still copying: 0"
	if err == nil || err.Error() != want {
		t.Errorf("MigrateWrites err: %v, want %v", err, want)
	}
}

func TestStreamMigrateEmptyWorflow(t *testing.T) {
	ctx := context.Background()
	tme := newTestShardMigrater(ctx, t, []string{"0"}, []string{"-80", "80-"})
	defer tme.stopTablets(t)

	// Migrate reads
	err := tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_RDONLY, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}
	err = tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_REPLICA, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}

	tme.expectCheckJournals()

	stopStreams := func() {
		// sm.stopStreams->sm.readSourceStreams->readTabletStreams('Stopped')
		tme.dbSourceClients[0].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)

		// pre-compute sourceRows because they're re-read multiple times.
		var sourceRows [][]string
		for _, sourceTargetShard := range tme.sourceShards {
			var rows []string
			for j, sourceShard := range tme.sourceShards {
				bls := &binlogdatapb.BinlogSource{
					Keyspace: "ks",
					Shard:    sourceShard,
					Filter: &binlogdatapb.Filter{
						Rules: []*binlogdatapb.Rule{{
							Match:  "t1",
							Filter: fmt.Sprintf("select * from t1 where in_keyrange('%s')", sourceTargetShard),
						}},
					},
				}
				rows = append(rows, fmt.Sprintf("%d||%v|MariaDB/5-456-888", j+1, bls))
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
		}
	}
	stopStreams()

	_, err = tme.wr.MigrateWrites(ctx, tme.targetKeyspace, "test", 1*time.Second)
	want := "VReplication streams must have named workflows for migration: shard: ks:0, stream: 1"
	if err == nil || err.Error() != want {
		t.Errorf("MigrateWrites err: %v, want %v", err, want)
	}
}

func TestStreamMigrateDupWorflow(t *testing.T) {
	ctx := context.Background()
	tme := newTestShardMigrater(ctx, t, []string{"0"}, []string{"-80", "80-"})
	defer tme.stopTablets(t)

	// Migrate reads
	err := tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_RDONLY, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}
	err = tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_REPLICA, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}

	tme.expectCheckJournals()

	stopStreams := func() {
		// sm.stopStreams->sm.readSourceStreams->readTabletStreams('Stopped')
		tme.dbSourceClients[0].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)

		// pre-compute sourceRows because they're re-read multiple times.
		var sourceRows [][]string
		for _, sourceTargetShard := range tme.sourceShards {
			var rows []string
			for j, sourceShard := range tme.sourceShards {
				bls := &binlogdatapb.BinlogSource{
					Keyspace: "ks",
					Shard:    sourceShard,
					Filter: &binlogdatapb.Filter{
						Rules: []*binlogdatapb.Rule{{
							Match:  "t1",
							Filter: fmt.Sprintf("select * from t1 where in_keyrange('%s')", sourceTargetShard),
						}},
					},
				}
				rows = append(rows, fmt.Sprintf("%d|test|%v|MariaDB/5-456-888", j+1, bls))
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
		}
	}
	stopStreams()

	_, err = tme.wr.MigrateWrites(ctx, tme.targetKeyspace, "test", 1*time.Second)
	want := "VReplication stream has the same workflow name as the resharding workflow: shard: ks:0, stream: 1"
	if err == nil || err.Error() != want {
		t.Errorf("MigrateWrites err: %v, want %v", err, want)
	}
}

func TestStreamMigrateStreamsMismatch(t *testing.T) {
	ctx := context.Background()
	// Interesting tidbit: you cannot create a shard "0" for an already sharded keyspace.
	tme := newTestShardMigrater(ctx, t, []string{"-80", "80-"}, []string{"-"})
	defer tme.stopTablets(t)

	// Migrate reads
	err := tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_RDONLY, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}
	err = tme.wr.MigrateReads(ctx, tme.targetKeyspace, "test", topodatapb.TabletType_REPLICA, nil, DirectionForward)
	if err != nil {
		t.Fatal(err)
	}

	tme.expectCheckJournals()

	stopStreams := func() {
		// sm.stopStreams->sm.readSourceStreams->readTabletStreams('Stopped')
		tme.dbSourceClients[0].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and state = 'Stopped'", &sqltypes.Result{}, nil)

		// pre-compute sourceRows because they're re-read multiple times.
		var sourceRows [][]string
		for i, sourceTargetShard := range tme.sourceShards {
			var rows []string
			for j, sourceShard := range tme.sourceShards {
				// Skip one stream in one shard.
				if i == 0 && j == 0 {
					continue
				}
				bls := &binlogdatapb.BinlogSource{
					Keyspace: "ks",
					Shard:    sourceShard,
					Filter: &binlogdatapb.Filter{
						Rules: []*binlogdatapb.Rule{{
							Match:  "t1",
							Filter: fmt.Sprintf("select * from t1 where in_keyrange('%s')", sourceTargetShard),
						}},
					},
				}
				rows = append(rows, fmt.Sprintf("%d|t1|%v|MariaDB/5-456-888", j+1, bls))
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
			if i == 0 {
				dbclient.addQuery("select vrepl_id from _vt.copy_state where vrepl_id in (2)", &sqltypes.Result{}, nil)
			} else {
				dbclient.addQuery("select vrepl_id from _vt.copy_state where vrepl_id in (1, 2)", &sqltypes.Result{}, nil)
			}
		}
	}
	stopStreams()

	_, err = tme.wr.MigrateWrites(ctx, tme.targetKeyspace, "test", 1*time.Second)
	want := "streams are mismatched across source shards"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("MigrateWrites err: %v, must contain %v", err, want)
	}
}

func TestTemplatize(t *testing.T) {
	tests := []struct {
		in  []*vrStream
		out string
		err string
	}{{
		// First test contains all fields.
		in: []*vrStream{{
			id:       1,
			workflow: "test",
			bls: &binlogdatapb.BinlogSource{
				Keyspace: "ks",
				Shard:    "80-",
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "select * from t1 where in_keyrange('-80')",
					}},
				},
			},
		}},
		out: `[{"ID":1,"Workflow":"test","Bls":{"keyspace":"ks","shard":"80-","filter":{"rules":[{"match":"t1","filter":"select * from t1 where in_keyrange('{{.}}')"}]}}}]`,
	}, {
		// Empty filter: reference table
		in: []*vrStream{{
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "",
					}},
				},
			},
		}},
		out: "",
	}, {
		// KeyRange filter
		in: []*vrStream{{
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "-80",
					}},
				},
			},
		}},
		out: `[{"ID":0,"Workflow":"","Bls":{"filter":{"rules":[{"match":"t1","filter":"{{.}}"}]}}}]`,
	}, {
		// Excluded table and empty filter
		in: []*vrStream{{
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: vreplication.ExcludeStr,
					}, {
						Match:  "t2",
						Filter: "",
					}},
				},
			},
		}},
		out: "",
	}, {
		// KeyRange filter and excluded table
		in: []*vrStream{{
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "-80",
					}, {
						Match:  "t2",
						Filter: vreplication.ExcludeStr,
					}},
				},
			},
		}},
		out: `[{"ID":0,"Workflow":"","Bls":{"filter":{"rules":[{"match":"t1","filter":"{{.}}"},{"match":"t2","filter":"exclude"}]}}}]`,
	}, {
		// KeyRange filter and ref table
		in: []*vrStream{{
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "-80",
					}, {
						Match:  "t2",
						Filter: "",
					}},
				},
			},
		}},
		err: `cannot migrate streams with a mix of reference and sharded tables: filter:<rules:<match:"t1" filter:"{{.}}" > rules:<match:"t2" > > `,
	}, {
		// Ref table and keyRange filter (different code path)
		in: []*vrStream{{
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "",
					}, {
						Match:  "t2",
						Filter: "-80",
					}},
				},
			},
		}},
		err: `cannot migrate streams with a mix of reference and sharded tables: filter:<rules:<match:"t1" > rules:<match:"t2" filter:"{{.}}" > > `,
	}, {
		// Ref table with select expression
		in: []*vrStream{{
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "select * from t1",
					}},
				},
			},
		}},
		out: "",
	}, {
		// Select expresstion with one keyrange value
		in: []*vrStream{{
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "select * from t1 where in_keyrange('-80')",
					}},
				},
			},
		}},
		out: `[{"ID":0,"Workflow":"","Bls":{"filter":{"rules":[{"match":"t1","filter":"select * from t1 where in_keyrange('{{.}}')"}]}}}]`,
	}, {
		// Select expresstion with three keyrange values
		in: []*vrStream{{
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "select * from t1 where in_keyrange(col, vdx, '-80')",
					}},
				},
			},
		}},
		out: `[{"ID":0,"Workflow":"","Bls":{"filter":{"rules":[{"match":"t1","filter":"select * from t1 where in_keyrange(col, vdx, '{{.}}')"}]}}}]`,
	}, {
		// syntax error
		in: []*vrStream{{
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "bad syntax",
					}},
				},
			},
		}},
		err: "syntax error at position 4 near 'bad'",
	}, {
		// invalid statement
		in: []*vrStream{{
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "update t set a=1",
					}},
				},
			},
		}},
		err: "unexpected query: update t set a=1",
	}, {
		// invalid in_keyrange
		in: []*vrStream{{
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "select * from t1 where in_keyrange(col, vdx, '-80', extra)",
					}},
				},
			},
		}},
		err: "unexpected in_keyrange parameters: in_keyrange(col, vdx, '-80', extra)",
	}, {
		// * in_keyrange
		in: []*vrStream{{
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "select * from t1 where in_keyrange(*)",
					}},
				},
			},
		}},
		err: "unexpected in_keyrange parameters: in_keyrange(*)",
	}, {
		// non-string in_keyrange
		in: []*vrStream{{
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "select * from t1 where in_keyrange(aa)",
					}},
				},
			},
		}},
		err: "unexpected in_keyrange parameters: in_keyrange(aa)",
	}, {
		// '{{' in query
		in: []*vrStream{{
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "select '{{' from t1 where in_keyrange('-80')",
					}},
				},
			},
		}},
		err: "cannot migrate queries that contain '{{' in their string: select '{{' from t1 where in_keyrange('-80')",
	}}
	for _, tt := range tests {
		sm := &streamMigrater{mi: nil}
		out, err := sm.templatize(context.Background(), tt.in)
		var gotErr string
		if err != nil {
			gotErr = err.Error()
		}
		if gotErr != tt.err {
			t.Errorf("templatize(%v) err: %v, want %v", tt.in, err, tt.err)
		}
		got := stringifyVRS(out)
		if !reflect.DeepEqual(tt.out, got) {
			t.Errorf("templatize(%v):\n%v, want\n%v", stringifyVRS(tt.in), got, tt.out)
		}
	}
}

type testVRS struct {
	ID       uint32
	Workflow string
	Bls      *binlogdatapb.BinlogSource
}

func stringifyVRS(in []*vrStream) string {
	if len(in) == 0 {
		return ""
	}
	var converted []*testVRS
	for _, vrs := range in {
		converted = append(converted, &testVRS{
			ID:       vrs.id,
			Workflow: vrs.workflow,
			Bls:      vrs.bls,
		})
	}
	b, _ := json.Marshal(converted)
	return string(b)
}
