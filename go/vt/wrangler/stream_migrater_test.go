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

	tme.expectCreateJournals()
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

func TestStreamMigrateSync(t *testing.T) {
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
