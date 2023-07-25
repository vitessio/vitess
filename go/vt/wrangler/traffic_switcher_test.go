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
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtctl/workflow"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	resultid1    = &sqltypes.Result{Rows: [][]sqltypes.Value{{sqltypes.NewInt64(1)}}}
	resultid2    = &sqltypes.Result{Rows: [][]sqltypes.Value{{sqltypes.NewInt64(2)}}}
	resultid3    = &sqltypes.Result{Rows: [][]sqltypes.Value{{sqltypes.NewInt64(3)}}}
	resultid12   = &sqltypes.Result{Rows: [][]sqltypes.Value{{sqltypes.NewInt64(1)}, {sqltypes.NewInt64(2)}}}
	resultid1234 = &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}, {
			sqltypes.NewInt64(2),
		}, {
			sqltypes.NewInt64(3),
		}, {
			sqltypes.NewInt64(4),
		}},
	}
	resultid34   = &sqltypes.Result{Rows: [][]sqltypes.Value{{sqltypes.NewInt64(3)}, {sqltypes.NewInt64(4)}}}
	resultid3456 = &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(3),
		}, {
			sqltypes.NewInt64(4),
		}, {
			sqltypes.NewInt64(5),
		}, {
			sqltypes.NewInt64(6),
		}},
	}
)

const (
	tsCheckJournals = "select val from _vt.resharding_journal where id=.*"
)

// TestTableMigrate tests table mode migrations.
// This has to be kept in sync with TestShardMigrate.
func TestTableMigrateMainflow(t *testing.T) {
	ctx := context.Background()
	tme := newTestTableMigrater(ctx, t)
	defer tme.close(t)

	checkCellRouting(t, tme.wr, "cell1", map[string][]string{
		"t1":     {"ks1.t1"},
		"ks2.t1": {"ks1.t1"},
		"t2":     {"ks1.t2"},
		"ks2.t2": {"ks1.t2"},
	})

	tme.expectNoPreviousJournals()
	//-------------------------------------------------------------------------------------------------------------------
	// Single cell RDONLY migration.
	_, err := tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, []string{"cell1"}, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkCellRouting(t, tme.wr, "cell1", map[string][]string{
		"t1":            {"ks1.t1"},
		"ks2.t1":        {"ks1.t1"},
		"t2":            {"ks1.t2"},
		"ks2.t2":        {"ks1.t2"},
		"t1@rdonly":     {"ks2.t1"},
		"ks2.t1@rdonly": {"ks2.t1"},
		"ks1.t1@rdonly": {"ks2.t1"},
		"t2@rdonly":     {"ks2.t2"},
		"ks2.t2@rdonly": {"ks2.t2"},
		"ks1.t2@rdonly": {"ks2.t2"},
	})
	checkCellRouting(t, tme.wr, "cell2", map[string][]string{
		"t1":     {"ks1.t1"},
		"ks2.t1": {"ks1.t1"},
		"t2":     {"ks1.t2"},
		"ks2.t2": {"ks1.t2"},
	})
	verifyQueries(t, tme.allDBClients)

	tme.expectNoPreviousJournals()
	//-------------------------------------------------------------------------------------------------------------------
	// Other cell REPLICA migration.
	// The global routing already contains redirections for rdonly.
	// So, adding routes for replica and deploying to cell2 will also cause
	// cell2 to switch rdonly. This is a quirk that can be fixed later if necessary.
	// TODO(sougou): check if it's worth fixing, or clearly document the quirk.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, []string{"cell2"}, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkCellRouting(t, tme.wr, "cell1", map[string][]string{
		"t1":            {"ks1.t1"},
		"ks2.t1":        {"ks1.t1"},
		"t2":            {"ks1.t2"},
		"ks2.t2":        {"ks1.t2"},
		"t1@rdonly":     {"ks2.t1"},
		"ks2.t1@rdonly": {"ks2.t1"},
		"ks1.t1@rdonly": {"ks2.t1"},
		"t2@rdonly":     {"ks2.t2"},
		"ks2.t2@rdonly": {"ks2.t2"},
		"ks1.t2@rdonly": {"ks2.t2"},
	})
	checkCellRouting(t, tme.wr, "cell2", map[string][]string{
		"t1":             {"ks1.t1"},
		"ks2.t1":         {"ks1.t1"},
		"t2":             {"ks1.t2"},
		"ks2.t2":         {"ks1.t2"},
		"t1@rdonly":      {"ks2.t1"},
		"ks2.t1@rdonly":  {"ks2.t1"},
		"ks1.t1@rdonly":  {"ks2.t1"},
		"t2@rdonly":      {"ks2.t2"},
		"ks2.t2@rdonly":  {"ks2.t2"},
		"ks1.t2@rdonly":  {"ks2.t2"},
		"t1@replica":     {"ks2.t1"},
		"ks2.t1@replica": {"ks2.t1"},
		"ks1.t1@replica": {"ks2.t1"},
		"t2@replica":     {"ks2.t2"},
		"ks2.t2@replica": {"ks2.t2"},
		"ks1.t2@replica": {"ks2.t2"},
	})
	verifyQueries(t, tme.allDBClients)
	tme.expectNoPreviousJournals()
	//-------------------------------------------------------------------------------------------------------------------
	// Single cell backward REPLICA migration.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, []string{"cell2"}, workflow.DirectionBackward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkCellRouting(t, tme.wr, "cell1", map[string][]string{
		"t1":            {"ks1.t1"},
		"ks2.t1":        {"ks1.t1"},
		"t2":            {"ks1.t2"},
		"ks2.t2":        {"ks1.t2"},
		"t1@rdonly":     {"ks2.t1"},
		"ks2.t1@rdonly": {"ks2.t1"},
		"ks1.t1@rdonly": {"ks2.t1"},
		"t2@rdonly":     {"ks2.t2"},
		"ks2.t2@rdonly": {"ks2.t2"},
		"ks1.t2@rdonly": {"ks2.t2"},
	})
	checkCellRouting(t, tme.wr, "cell2", map[string][]string{
		"t1":             {"ks1.t1"},
		"ks2.t1":         {"ks1.t1"},
		"t2":             {"ks1.t2"},
		"ks2.t2":         {"ks1.t2"},
		"t1@rdonly":      {"ks1.t1"},
		"ks2.t1@rdonly":  {"ks1.t1"},
		"ks1.t1@rdonly":  {"ks1.t1"},
		"t2@rdonly":      {"ks1.t2"},
		"ks2.t2@rdonly":  {"ks1.t2"},
		"ks1.t2@rdonly":  {"ks1.t2"},
		"t1@replica":     {"ks1.t1"},
		"ks2.t1@replica": {"ks1.t1"},
		"ks1.t1@replica": {"ks1.t1"},
		"t2@replica":     {"ks1.t2"},
		"ks2.t2@replica": {"ks1.t2"},
		"ks1.t2@replica": {"ks1.t2"},
	})
	verifyQueries(t, tme.allDBClients)

	tme.expectNoPreviousJournals()
	//-------------------------------------------------------------------------------------------------------------------
	// Switch all REPLICA.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkRouting(t, tme.wr, map[string][]string{
		"t1":             {"ks1.t1"},
		"ks2.t1":         {"ks1.t1"},
		"t2":             {"ks1.t2"},
		"ks2.t2":         {"ks1.t2"},
		"t1@rdonly":      {"ks2.t1"},
		"ks2.t1@rdonly":  {"ks2.t1"},
		"ks1.t1@rdonly":  {"ks2.t1"},
		"t2@rdonly":      {"ks2.t2"},
		"ks2.t2@rdonly":  {"ks2.t2"},
		"ks1.t2@rdonly":  {"ks2.t2"},
		"t1@replica":     {"ks2.t1"},
		"ks2.t1@replica": {"ks2.t1"},
		"ks1.t1@replica": {"ks2.t1"},
		"t2@replica":     {"ks2.t2"},
		"ks2.t2@replica": {"ks2.t2"},
		"ks1.t2@replica": {"ks2.t2"},
	})
	verifyQueries(t, tme.allDBClients)

	tme.expectNoPreviousJournals()
	//-------------------------------------------------------------------------------------------------------------------
	// All cells RDONLY backward migration.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionBackward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkRouting(t, tme.wr, map[string][]string{
		"t1":             {"ks1.t1"},
		"ks2.t1":         {"ks1.t1"},
		"t2":             {"ks1.t2"},
		"ks2.t2":         {"ks1.t2"},
		"t1@replica":     {"ks2.t1"},
		"ks2.t1@replica": {"ks2.t1"},
		"ks1.t1@replica": {"ks2.t1"},
		"t2@replica":     {"ks2.t2"},
		"ks2.t2@replica": {"ks2.t2"},
		"ks1.t2@replica": {"ks2.t2"},
		"t1@rdonly":      {"ks1.t1"},
		"ks2.t1@rdonly":  {"ks1.t1"},
		"ks1.t1@rdonly":  {"ks1.t1"},
		"t2@rdonly":      {"ks1.t2"},
		"ks2.t2@rdonly":  {"ks1.t2"},
		"ks1.t2@rdonly":  {"ks1.t2"},
	})
	verifyQueries(t, tme.allDBClients)

	tme.expectNoPreviousJournals()
	//-------------------------------------------------------------------------------------------------------------------
	// All cells RDONLY backward migration.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, nil, workflow.DirectionBackward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkRouting(t, tme.wr, map[string][]string{
		"t1":             {"ks1.t1"},
		"ks2.t1":         {"ks1.t1"},
		"t2":             {"ks1.t2"},
		"ks2.t2":         {"ks1.t2"},
		"t1@replica":     {"ks1.t1"},
		"ks2.t1@replica": {"ks1.t1"},
		"ks1.t1@replica": {"ks1.t1"},
		"t2@replica":     {"ks1.t2"},
		"ks2.t2@replica": {"ks1.t2"},
		"ks1.t2@replica": {"ks1.t2"},
		"t1@rdonly":      {"ks1.t1"},
		"ks2.t1@rdonly":  {"ks1.t1"},
		"ks1.t1@rdonly":  {"ks1.t1"},
		"t2@rdonly":      {"ks1.t2"},
		"ks2.t2@rdonly":  {"ks1.t2"},
		"ks1.t2@rdonly":  {"ks1.t2"},
	})
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Can't switch primary with SwitchReads.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_PRIMARY}, nil, workflow.DirectionForward, false)
	want := "tablet type must be REPLICA or RDONLY: PRIMARY"
	if err == nil || err.Error() != want {
		t.Errorf("SwitchReads(primary) err: %v, want %v", err, want)
	}
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Test SwitchWrites cancelation on failure.

	tme.expectNoPreviousJournals()
	// Switch all the reads first.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	tme.expectNoPreviousJournals()
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkRouting(t, tme.wr, map[string][]string{
		"t1":             {"ks1.t1"},
		"ks2.t1":         {"ks1.t1"},
		"t2":             {"ks1.t2"},
		"ks2.t2":         {"ks1.t2"},
		"t1@replica":     {"ks2.t1"},
		"ks2.t1@replica": {"ks2.t1"},
		"ks1.t1@replica": {"ks2.t1"},
		"t2@replica":     {"ks2.t2"},
		"ks2.t2@replica": {"ks2.t2"},
		"ks1.t2@replica": {"ks2.t2"},
		"t1@rdonly":      {"ks2.t1"},
		"ks2.t1@rdonly":  {"ks2.t1"},
		"ks1.t1@rdonly":  {"ks2.t1"},
		"t2@rdonly":      {"ks2.t2"},
		"ks2.t2@rdonly":  {"ks2.t2"},
		"ks1.t2@rdonly":  {"ks2.t2"},
	})

	checkJournals := func() {
		tme.dbSourceClients[0].addQuery("select val from _vt.resharding_journal where id=7672494164556733923", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select val from _vt.resharding_journal where id=7672494164556733923", &sqltypes.Result{}, nil)
	}
	checkJournals()

	deleteReverseReplicaion := func() {
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks1' and workflow = 'test_reverse'", resultid34, nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks1' and workflow = 'test_reverse'", resultid34, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.vreplication where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.vreplication where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.post_copy_action where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.copy_state where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.post_copy_action where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
	}
	cancelMigration := func() {
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", resultid12, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", resultid12, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", runningResult(1), nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 2", runningResult(2), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 1", runningResult(1), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 2", runningResult(2), nil)

		deleteReverseReplicaion()
	}
	cancelMigration()

	switchWrites(tme)
	_, _, err = tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", 0*time.Second, false, false, true, false)
	want = "DeadlineExceeded"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("SwitchWrites(0 timeout) err: %v, must contain %v", err, want)
	}
	verifyQueries(t, tme.allDBClients)
	checkRouting(t, tme.wr, map[string][]string{
		"t1":             {"ks1.t1"},
		"ks2.t1":         {"ks1.t1"},
		"t2":             {"ks1.t2"},
		"ks2.t2":         {"ks1.t2"},
		"t1@replica":     {"ks2.t1"},
		"ks2.t1@replica": {"ks2.t1"},
		"ks1.t1@replica": {"ks2.t1"},
		"t2@replica":     {"ks2.t2"},
		"ks2.t2@replica": {"ks2.t2"},
		"ks1.t2@replica": {"ks2.t2"},
		"t1@rdonly":      {"ks2.t1"},
		"ks2.t1@rdonly":  {"ks2.t1"},
		"ks1.t1@rdonly":  {"ks2.t1"},
		"t2@rdonly":      {"ks2.t2"},
		"ks2.t2@rdonly":  {"ks2.t2"},
		"ks1.t2@rdonly":  {"ks2.t2"},
	})
	checkDenyList(t, tme.ts, "ks1:-40", nil)
	checkDenyList(t, tme.ts, "ks1:40-", nil)
	checkDenyList(t, tme.ts, "ks2:-80", nil)
	checkDenyList(t, tme.ts, "ks2:80-", nil)

	//-------------------------------------------------------------------------------------------------------------------
	// Test successful SwitchWrites.

	checkJournals()

	waitForCatchup := func() {
		// mi.waitForCatchup-> mi.wr.tmc.VReplicationWaitForPos
		state := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"pos|state|message",
			"varchar|varchar|varchar"),
			"MariaDB/5-456-892|Running",
		)
		tme.dbTargetClients[0].addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
		tme.dbTargetClients[0].addQuery("select pos, state, message from _vt.vreplication where id=2", state, nil)
		tme.dbTargetClients[1].addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
		tme.dbTargetClients[1].addQuery("select pos, state, message from _vt.vreplication where id=2", state, nil)

		// mi.waitForCatchup-> mi.wr.tmc.VReplicationExec('Stopped')
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where id = 1", resultid1, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (1)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where id = 2", resultid2, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where id = 1", resultid1, nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (1)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where id = 2", resultid2, nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	}
	waitForCatchup()

	createReverseVReplication := func() {
		deleteReverseReplicaion()

		tme.dbSourceClients[0].addQueryRE("insert into _vt.vreplication.*test_reverse.*ks2.*-80.*t1.*in_keyrange.*c1.*hash.*-40.*t2.*-40.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
		tme.dbSourceClients[0].addQueryRE("insert into _vt.vreplication.*test_reverse.*ks2.*80-.*t1.*in_keyrange.*c1.*hash.*-40.*t2.*-40.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 2}, nil)
		tme.dbSourceClients[1].addQueryRE("insert into _vt.vreplication.*test_reverse.*ks2.*-80.*t1.*in_keyrange.*c1.*hash.*40-.*t2.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
		tme.dbSourceClients[1].addQueryRE("insert into _vt.vreplication.*test_reverse.*ks2.*80-.*t1.*in_keyrange.*c1.*hash.*40-.*t2.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 2}, nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	}
	createReverseVReplication()

	createJournals := func() {
		journal1 := "insert into _vt.resharding_journal.*7672494164556733923,.*tables.*t1.*t2.*local_position.*MariaDB/5-456-892.*shard_gtids.*-80.*MariaDB/5-456-893.*participants.*40.*40"
		tme.dbSourceClients[0].addQueryRE(journal1, &sqltypes.Result{}, nil)
		journal2 := "insert into _vt.resharding_journal.*7672494164556733923,.*tables.*t1.*t2.*local_position.*MariaDB/5-456-892.*shard_gtids.*80.*MariaDB/5-456-893.*80.*participants.*40.*40"
		tme.dbSourceClients[1].addQueryRE(journal2, &sqltypes.Result{}, nil)
	}
	createJournals()

	startReverseVReplication := func() {
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks1'", resultid34, nil)
		tme.dbSourceClients[0].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 3", runningResult(3), nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 4", runningResult(4), nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks1'", resultid34, nil)
		tme.dbSourceClients[1].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 3", runningResult(3), nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 4", runningResult(4), nil)
	}
	startReverseVReplication()

	deleteTargetVReplication := func() {
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", resultid12, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", resultid12, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set message = 'FROZEN' where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set message = 'FROZEN' where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	}
	deleteTargetVReplication()

	journalID, _, err := tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", 1*time.Second, false, false, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if journalID != 7672494164556733923 {
		t.Errorf("journal id: %d, want 7672494164556733923", journalID)
	}

	checkRouting(t, tme.wr, map[string][]string{
		"t1":             {"ks2.t1"},
		"ks1.t1":         {"ks2.t1"},
		"t2":             {"ks2.t2"},
		"ks1.t2":         {"ks2.t2"},
		"t1@replica":     {"ks2.t1"},
		"ks2.t1@replica": {"ks2.t1"},
		"ks1.t1@replica": {"ks2.t1"},
		"t2@replica":     {"ks2.t2"},
		"ks2.t2@replica": {"ks2.t2"},
		"ks1.t2@replica": {"ks2.t2"},
		"t1@rdonly":      {"ks2.t1"},
		"ks2.t1@rdonly":  {"ks2.t1"},
		"ks1.t1@rdonly":  {"ks2.t1"},
		"t2@rdonly":      {"ks2.t2"},
		"ks2.t2@rdonly":  {"ks2.t2"},
		"ks1.t2@rdonly":  {"ks2.t2"},
	})
	checkDenyList(t, tme.ts, "ks1:-40", []string{"t1", "t2"})
	checkDenyList(t, tme.ts, "ks1:40-", []string{"t1", "t2"})
	checkDenyList(t, tme.ts, "ks2:-80", nil)
	checkDenyList(t, tme.ts, "ks2:80-", nil)

	verifyQueries(t, tme.allDBClients)
}

// TestShardMigrate tests table mode migrations.
// This has to be kept in sync with TestTableMigrate.
func TestShardMigrateMainflow(t *testing.T) {
	ctx := context.Background()
	tme := newTestShardMigrater(ctx, t, []string{"-40", "40-"}, []string{"-80", "80-"})
	defer tme.close(t)

	// Initial check
	checkServedTypes(t, tme.ts, "ks:-40", 3)
	checkServedTypes(t, tme.ts, "ks:40-", 3)
	checkServedTypes(t, tme.ts, "ks:-80", 0)
	checkServedTypes(t, tme.ts, "ks:80-", 0)

	tme.expectNoPreviousJournals()
	//-------------------------------------------------------------------------------------------------------------------
	// Single cell RDONLY migration.
	_, err := tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, []string{"cell1"}, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkCellServedTypes(t, tme.ts, "ks:-40", "cell1", 2)
	checkCellServedTypes(t, tme.ts, "ks:40-", "cell1", 2)
	checkCellServedTypes(t, tme.ts, "ks:-80", "cell1", 1)
	checkCellServedTypes(t, tme.ts, "ks:80-", "cell1", 1)
	checkCellServedTypes(t, tme.ts, "ks:-40", "cell2", 3)
	checkCellServedTypes(t, tme.ts, "ks:40-", "cell2", 3)
	checkCellServedTypes(t, tme.ts, "ks:-80", "cell2", 0)
	checkCellServedTypes(t, tme.ts, "ks:80-", "cell2", 0)
	verifyQueries(t, tme.allDBClients)

	tme.expectNoPreviousJournals()
	//-------------------------------------------------------------------------------------------------------------------
	// Other cell REPLICA migration.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, []string{"cell2"}, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkCellServedTypes(t, tme.ts, "ks:-40", "cell1", 2)
	checkCellServedTypes(t, tme.ts, "ks:40-", "cell1", 2)
	checkCellServedTypes(t, tme.ts, "ks:-80", "cell1", 1)
	checkCellServedTypes(t, tme.ts, "ks:80-", "cell1", 1)
	checkCellServedTypes(t, tme.ts, "ks:-40", "cell2", 1)
	checkCellServedTypes(t, tme.ts, "ks:40-", "cell2", 1)
	checkCellServedTypes(t, tme.ts, "ks:-80", "cell2", 2)
	checkCellServedTypes(t, tme.ts, "ks:80-", "cell2", 2)
	verifyQueries(t, tme.allDBClients)

	tme.expectNoPreviousJournals()
	//-------------------------------------------------------------------------------------------------------------------
	// Single cell backward REPLICA migration.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, []string{"cell2"}, workflow.DirectionBackward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkCellServedTypes(t, tme.ts, "ks:-40", "cell1", 2)
	checkCellServedTypes(t, tme.ts, "ks:40-", "cell1", 2)
	checkCellServedTypes(t, tme.ts, "ks:-80", "cell1", 1)
	checkCellServedTypes(t, tme.ts, "ks:80-", "cell1", 1)
	checkCellServedTypes(t, tme.ts, "ks:-40", "cell2", 3)
	checkCellServedTypes(t, tme.ts, "ks:40-", "cell2", 3)
	checkCellServedTypes(t, tme.ts, "ks:-80", "cell2", 0)
	checkCellServedTypes(t, tme.ts, "ks:80-", "cell2", 0)
	verifyQueries(t, tme.allDBClients)

	tme.expectNoPreviousJournals()
	//-------------------------------------------------------------------------------------------------------------------
	// Switch all RDONLY.
	// This is an extra step that does not exist in the tables test.
	// The per-cell migration mechanism is different for tables. So, this
	// extra step is needed to bring things in sync.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkServedTypes(t, tme.ts, "ks:-40", 2)
	checkServedTypes(t, tme.ts, "ks:40-", 2)
	checkServedTypes(t, tme.ts, "ks:-80", 1)
	checkServedTypes(t, tme.ts, "ks:80-", 1)
	verifyQueries(t, tme.allDBClients)

	tme.expectNoPreviousJournals()
	//-------------------------------------------------------------------------------------------------------------------
	// Switch all REPLICA.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkServedTypes(t, tme.ts, "ks:-40", 1)
	checkServedTypes(t, tme.ts, "ks:40-", 1)
	checkServedTypes(t, tme.ts, "ks:-80", 2)
	checkServedTypes(t, tme.ts, "ks:80-", 2)
	verifyQueries(t, tme.allDBClients)

	tme.expectNoPreviousJournals()
	//-------------------------------------------------------------------------------------------------------------------
	// All cells RDONLY backward migration.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionBackward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkServedTypes(t, tme.ts, "ks:-40", 2)
	checkServedTypes(t, tme.ts, "ks:40-", 2)
	checkServedTypes(t, tme.ts, "ks:-80", 1)
	checkServedTypes(t, tme.ts, "ks:80-", 1)
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Can't switch primary with SwitchReads.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_PRIMARY}, nil, workflow.DirectionForward, false)
	want := "tablet type must be REPLICA or RDONLY: PRIMARY"
	if err == nil || err.Error() != want {
		t.Errorf("SwitchReads(primary) err: %v, want %v", err, want)
	}
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Test SwitchWrites cancelation on failure.

	tme.expectNoPreviousJournals()
	// Switch all the reads first.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkServedTypes(t, tme.ts, "ks:-40", 1)
	checkServedTypes(t, tme.ts, "ks:40-", 1)
	checkServedTypes(t, tme.ts, "ks:-80", 2)
	checkServedTypes(t, tme.ts, "ks:80-", 2)
	checkIfPrimaryServing(t, tme.ts, "ks:-40", true)
	checkIfPrimaryServing(t, tme.ts, "ks:40-", true)
	checkIfPrimaryServing(t, tme.ts, "ks:-80", false)
	checkIfPrimaryServing(t, tme.ts, "ks:80-", false)

	checkJournals := func() {
		tme.dbSourceClients[0].addQuery("select val from _vt.resharding_journal where id=6432976123657117097", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select val from _vt.resharding_journal where id=6432976123657117097", &sqltypes.Result{}, nil)
	}
	checkJournals()

	stopStreams := func() {
		tme.dbSourceClients[0].addQuery("select id, workflow, source, pos, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name='vt_ks' and workflow != 'test_reverse' and state = 'Stopped' and message != 'FROZEN'", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select id, workflow, source, pos, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name='vt_ks' and workflow != 'test_reverse' and state = 'Stopped' and message != 'FROZEN'", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("select id, workflow, source, pos, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name='vt_ks' and workflow != 'test_reverse'", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select id, workflow, source, pos, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name='vt_ks' and workflow != 'test_reverse'", &sqltypes.Result{}, nil)
	}
	stopStreams()

	deleteReverseReplicaion := func() {
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test_reverse'", resultid3, nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test_reverse'", resultid34, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.vreplication where id in (3)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.vreplication where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (3)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.post_copy_action where vrepl_id in (3)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.copy_state where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.post_copy_action where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
	}
	cancelMigration := func() {
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow != 'test_reverse'", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow != 'test_reverse'", &sqltypes.Result{}, nil)

		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test'", resultid12, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test'", resultid2, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", runningResult(1), nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 2", runningResult(2), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 2", runningResult(2), nil)

		deleteReverseReplicaion()
	}
	cancelMigration()

	_, _, err = tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", 0*time.Second, false, false, true, false)
	want = "DeadlineExceeded"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("SwitchWrites(0 timeout) err: %v, must contain %v", err, want)
	}

	verifyQueries(t, tme.allDBClients)
	checkServedTypes(t, tme.ts, "ks:-40", 1)
	checkServedTypes(t, tme.ts, "ks:40-", 1)
	checkServedTypes(t, tme.ts, "ks:-80", 2)
	checkServedTypes(t, tme.ts, "ks:80-", 2)
	checkIfPrimaryServing(t, tme.ts, "ks:-40", true)
	checkIfPrimaryServing(t, tme.ts, "ks:40-", true)
	checkIfPrimaryServing(t, tme.ts, "ks:-80", false)
	checkIfPrimaryServing(t, tme.ts, "ks:80-", false)

	//-------------------------------------------------------------------------------------------------------------------
	// Test successful SwitchWrites.

	checkJournals()
	stopStreams()

	waitForCatchup := func() {
		// mi.waitForCatchup-> mi.wr.tmc.VReplicationWaitForPos
		state := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"pos|state|message",
			"varchar|varchar|varchar"),
			"MariaDB/5-456-892|Running",
		)
		tme.dbTargetClients[0].addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
		tme.dbTargetClients[1].addQuery("select pos, state, message from _vt.vreplication where id=2", state, nil)
		tme.dbTargetClients[0].addQuery("select pos, state, message from _vt.vreplication where id=2", state, nil)

		// mi.waitForCatchup-> mi.wr.tmc.VReplicationExec('stopped for cutover')
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where id = 1", resultid1, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (1)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where id = 2", resultid2, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where id = 2", resultid2, nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	}
	waitForCatchup()

	createReverseVReplication := func() {
		deleteReverseReplicaion()

		tme.dbSourceClients[0].addQueryRE("insert into _vt.vreplication.*-80.*-40.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
		tme.dbSourceClients[1].addQueryRE("insert into _vt.vreplication.*-80.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
		tme.dbSourceClients[1].addQueryRE("insert into _vt.vreplication.*80-.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 2}, nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	}
	createReverseVReplication()

	createJournals := func() {
		journal1 := "insert into _vt.resharding_journal.*6432976123657117097.*migration_type:SHARDS.*local_position.*MariaDB/5-456-892.*shard_gtids.*-80.*MariaDB/5-456-893.*participants.*40.*40"
		tme.dbSourceClients[0].addQueryRE(journal1, &sqltypes.Result{}, nil)
		journal2 := "insert into _vt.resharding_journal.*6432976123657117097.*migration_type:SHARDS.*local_position.*MariaDB/5-456-892.*shard_gtids.*80.*MariaDB/5-456-893.*shard_gtids.*80.*MariaDB/5-456-893.*participants.*40.*40"
		tme.dbSourceClients[1].addQueryRE(journal2, &sqltypes.Result{}, nil)
	}
	createJournals()

	startReverseVReplication := func() {
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks'", resultid34, nil)
		tme.dbSourceClients[0].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 3", runningResult(3), nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 4", runningResult(4), nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks'", resultid34, nil)
		tme.dbSourceClients[1].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 3", runningResult(3), nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 4", runningResult(4), nil)
	}
	startReverseVReplication()

	freezeTargetVReplication := func() {
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test'", resultid12, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set message = 'FROZEN' where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test'", resultid2, nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set message = 'FROZEN' where id in (2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	}
	freezeTargetVReplication()

	journalID, _, err := tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", 1*time.Second, false, false, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if journalID != 6432976123657117097 {
		t.Errorf("journal id: %d, want 6432976123657117097", journalID)
	}

	verifyQueries(t, tme.allDBClients)

	checkServedTypes(t, tme.ts, "ks:-40", 0)
	checkServedTypes(t, tme.ts, "ks:40-", 0)
	checkServedTypes(t, tme.ts, "ks:-80", 3)
	checkServedTypes(t, tme.ts, "ks:80-", 3)

	checkIfPrimaryServing(t, tme.ts, "ks:-40", false)
	checkIfPrimaryServing(t, tme.ts, "ks:40-", false)
	checkIfPrimaryServing(t, tme.ts, "ks:-80", true)
	checkIfPrimaryServing(t, tme.ts, "ks:80-", true)

	verifyQueries(t, tme.allDBClients)
}

func TestTableMigrateOneToManyKeepNoArtifacts(t *testing.T) {
	testTableMigrateOneToMany(t, false, false)
}

func TestTableMigrateOneToManyKeepDataArtifacts(t *testing.T) {
	testTableMigrateOneToMany(t, true, false)
}

func TestTableMigrateOneToManyKeepRoutingArtifacts(t *testing.T) {
	testTableMigrateOneToMany(t, false, true)
}

func TestTableMigrateOneToManyKeepAllArtifacts(t *testing.T) {
	testTableMigrateOneToMany(t, true, true)
}

func testTableMigrateOneToMany(t *testing.T, keepData, keepRoutingRules bool) {
	ctx := context.Background()
	tme := newTestTableMigraterCustom(ctx, t, []string{"0"}, []string{"-80", "80-"}, "select * %s")
	defer tme.close(t)

	tme.expectNoPreviousJournals()
	_, err := tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	tme.expectNoPreviousJournals()
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}

	waitForCatchup := func() {
		// mi.waitForCatchup-> mi.wr.tmc.VReplicationWaitForPos
		state := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"pos|state|message",
			"varchar|varchar|varchar"),
			"MariaDB/5-456-892|Running",
		)
		tme.dbTargetClients[0].addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
		tme.dbTargetClients[1].addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)

		// mi.waitForCatchup-> mi.wr.tmc.VReplicationExec('Stopped')
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where id = 1", resultid1, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (1)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where id = 1", resultid1, nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (1)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
	}
	waitForCatchup()

	deleteReverseReplication := func() {
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks1' and workflow = 'test_reverse'", resultid34, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.vreplication where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.post_copy_action where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
	}

	createReverseVReplication := func() {
		deleteReverseReplication()

		tme.dbSourceClients[0].addQueryRE(`insert into _vt.vreplication.*test_reverse.*ks2.*-80.*t1.*from `+"`"+"t1`"+`\\".*t2.*from `+"`"+"t2`"+`\\"`, &sqltypes.Result{InsertID: 1}, nil)
		tme.dbSourceClients[0].addQueryRE(`insert into _vt.vreplication.*test_reverse.*ks2.*80-.*t1.*from `+"`"+"t1`"+`\\".*t2.*from `+"`"+"t2`"+`\\"`, &sqltypes.Result{InsertID: 2}, nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	}
	createReverseVReplication()

	createJournals := func() {
		journal1 := "insert into _vt.resharding_journal.*tables.*t1.*t2.*local_position.*MariaDB/5-456-892.*shard_gtids.*80.*MariaDB/5-456-893.*80.*MariaDB/5-456-893.*participants.*0"
		tme.dbSourceClients[0].addQueryRE(journal1, &sqltypes.Result{}, nil)
	}
	createJournals()

	freezeTargetVReplication := func() {
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", resultid1, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", resultid1, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set message = 'FROZEN' where id in (1)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set message = 'FROZEN' where id in (1)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
	}
	freezeTargetVReplication()

	dropSourcesInvalid := func() {
		tme.dbTargetClients[0].addQuery("select 1 from _vt.vreplication where db_name='vt_ks2' and workflow='test' and message!='FROZEN'", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select 1 from _vt.vreplication where db_name='vt_ks2' and workflow='test' and message!='FROZEN'", &sqltypes.Result{}, nil)
	}
	dropSourcesInvalid()
	_, err = tme.wr.DropSources(ctx, tme.targetKeyspace, "test", workflow.DropTable, keepData, keepRoutingRules, false, false)
	require.Error(t, err, "Workflow has not completed, cannot DropSources")

	tme.dbSourceClients[0].addQueryRE(tsCheckJournals, &sqltypes.Result{}, nil)

	switchWrites(tme)
	_, _, err = tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", 1*time.Second, false, false, false, false)
	if err != nil {
		t.Fatal(err)
	}

	dropSourcesDryRun := func() {
		tme.dbTargetClients[0].addQuery("select 1 from _vt.vreplication where db_name='vt_ks2' and workflow='test' and message!='FROZEN'", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select 1 from _vt.vreplication where db_name='vt_ks2' and workflow='test' and message!='FROZEN'", &sqltypes.Result{}, nil)
	}
	dropSourcesDryRun()
	wantdryRunDropSources := []string{
		"Lock keyspace ks1",
		"Lock keyspace ks2",
	}
	if !keepData {
		wantdryRunDropSources = append(wantdryRunDropSources, "Dropping these tables from the database and removing them from the vschema for keyspace ks1:",
			"	Keyspace ks1 Shard 0 DbName vt_ks1 Tablet 10 Table t1",
			"	Keyspace ks1 Shard 0 DbName vt_ks1 Tablet 10 Table t2",
			"Denied tables [t1,t2] will be removed from:",
			"	Keyspace ks1 Shard 0 Tablet 10")
	}
	wantdryRunDropSources = append(wantdryRunDropSources, "Delete reverse vreplication streams on source:",
		"	Keyspace ks1 Shard 0 Workflow test_reverse DbName vt_ks1 Tablet 10",
		"Delete vreplication streams on target:",
		"	Keyspace ks2 Shard -80 Workflow test DbName vt_ks2 Tablet 20",
		"	Keyspace ks2 Shard 80- Workflow test DbName vt_ks2 Tablet 30")
	if !keepRoutingRules {
		wantdryRunDropSources = append(wantdryRunDropSources, "Routing rules for participating tables will be deleted")
	}
	wantdryRunDropSources = append(wantdryRunDropSources, "Unlock keyspace ks2", "Unlock keyspace ks1")
	results, err := tme.wr.DropSources(ctx, tme.targetKeyspace, "test", workflow.DropTable, keepData, keepRoutingRules, false, true)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(wantdryRunDropSources, *results))
	checkDenyList(t, tme.ts, fmt.Sprintf("%s:%s", "ks1", "0"), []string{"t1", "t2"})

	dropSourcesDryRunRename := func() {
		tme.dbTargetClients[0].addQuery("select 1 from _vt.vreplication where db_name='vt_ks2' and workflow='test' and message!='FROZEN'", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select 1 from _vt.vreplication where db_name='vt_ks2' and workflow='test' and message!='FROZEN'", &sqltypes.Result{}, nil)
	}
	dropSourcesDryRunRename()
	wantdryRunRenameSources := []string{
		"Lock keyspace ks1",
		"Lock keyspace ks2",
	}
	if !keepData {
		wantdryRunRenameSources = append(wantdryRunRenameSources, "Renaming these tables from the database and removing them from the vschema for keyspace ks1:", "	"+
			"Keyspace ks1 Shard 0 DbName vt_ks1 Tablet 10 Table t1",
			"	Keyspace ks1 Shard 0 DbName vt_ks1 Tablet 10 Table t2",
			"Denied tables [t1,t2] will be removed from:",
			"	Keyspace ks1 Shard 0 Tablet 10")
	}
	wantdryRunRenameSources = append(wantdryRunRenameSources, "Delete reverse vreplication streams on source:",
		"	Keyspace ks1 Shard 0 Workflow test_reverse DbName vt_ks1 Tablet 10",
		"Delete vreplication streams on target:",
		"	Keyspace ks2 Shard -80 Workflow test DbName vt_ks2 Tablet 20",
		"	Keyspace ks2 Shard 80- Workflow test DbName vt_ks2 Tablet 30")
	if !keepRoutingRules {
		wantdryRunRenameSources = append(wantdryRunRenameSources, "Routing rules for participating tables will be deleted")
	}
	wantdryRunRenameSources = append(wantdryRunRenameSources, "Unlock keyspace ks2", "Unlock keyspace ks1")
	results, err = tme.wr.DropSources(ctx, tme.targetKeyspace, "test", workflow.RenameTable, keepData, keepRoutingRules, false, true)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(wantdryRunRenameSources, *results))
	checkDenyList(t, tme.ts, fmt.Sprintf("%s:%s", "ks1", "0"), []string{"t1", "t2"})

	dropSources := func() {
		tme.dbTargetClients[0].addQuery("select 1 from _vt.vreplication where db_name='vt_ks2' and workflow='test' and message!='FROZEN'", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select 1 from _vt.vreplication where db_name='vt_ks2' and workflow='test' and message!='FROZEN'", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks1' and workflow = 'test_reverse'", &sqltypes.Result{}, nil)
		tme.tmeDB.AddQuery(fmt.Sprintf("rename table `vt_ks1`.`t1` TO `vt_ks1`.`%s`", getRenameFileName("t1")), &sqltypes.Result{})
		tme.tmeDB.AddQuery(fmt.Sprintf("rename table `vt_ks1`.`t2` TO `vt_ks1`.`%s`", getRenameFileName("t2")), &sqltypes.Result{})
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", &sqltypes.Result{}, nil) //
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", &sqltypes.Result{}, nil)
	}
	dropSources()

	wantRouting := map[string][]string{
		"t1":             {"ks2.t1"},
		"ks1.t1":         {"ks2.t1"},
		"t2":             {"ks2.t2"},
		"ks1.t2":         {"ks2.t2"},
		"t1@replica":     {"ks2.t1"},
		"ks2.t1@replica": {"ks2.t1"},
		"ks1.t1@replica": {"ks2.t1"},
		"t2@replica":     {"ks2.t2"},
		"ks2.t2@replica": {"ks2.t2"},
		"ks1.t2@replica": {"ks2.t2"},
		"t1@rdonly":      {"ks2.t1"},
		"ks2.t1@rdonly":  {"ks2.t1"},
		"ks1.t1@rdonly":  {"ks2.t1"},
		"t2@rdonly":      {"ks2.t2"},
		"ks2.t2@rdonly":  {"ks2.t2"},
		"ks1.t2@rdonly":  {"ks2.t2"},
	}
	checkRouting(t, tme.wr, wantRouting)
	_, err = tme.wr.DropSources(ctx, tme.targetKeyspace, "test", workflow.RenameTable, keepData, keepRoutingRules, false, false)
	require.NoError(t, err)
	var wantDenyList []string
	if keepData {
		wantDenyList = []string{"t1", "t2"}
	}
	checkDenyList(t, tme.ts, fmt.Sprintf("%s:%s", "ks1", "0"), wantDenyList)
	if !keepRoutingRules {
		wantRouting = map[string][]string{}
	}
	checkRouting(t, tme.wr, wantRouting)

	verifyQueries(t, tme.allDBClients)
}

func TestTableMigrateOneToManyDryRun(t *testing.T) {
	var err error
	ctx := context.Background()
	tme := newTestTableMigraterCustom(ctx, t, []string{"0"}, []string{"-80", "80-"}, "select * %s")
	defer tme.close(t)

	wantdryRunReads := []string{
		"Lock keyspace ks1",
		"Switch reads for tables [t1,t2] to keyspace ks2 for tablet types [RDONLY]",
		"Routing rules for tables [t1,t2] will be updated",
		"Unlock keyspace ks1",
	}
	wantdryRunWrites := []string{
		"Lock keyspace ks1",
		"Lock keyspace ks2",
		"Stop writes on keyspace ks1, tables [t1,t2]:",
		"\tKeyspace ks1, Shard 0 at Position MariaDB/5-456-892",
		"Wait for VReplication on stopped streams to catchup for up to 1s",
		"Create reverse replication workflow test_reverse",
		"Create journal entries on source databases",
		"Enable writes on keyspace ks2 tables [t1,t2]",
		"Switch routing from keyspace ks1 to keyspace ks2",
		"Routing rules for tables [t1,t2] will be updated",
		"Switch writes completed, freeze and delete vreplication streams on:",
		"	tablet 20",
		"	tablet 30",
		"Mark vreplication streams frozen on:",
		"	Keyspace ks2, Shard -80, Tablet 20, Workflow test, DbName vt_ks2",
		"	Keyspace ks2, Shard 80-, Tablet 30, Workflow test, DbName vt_ks2",
		"Unlock keyspace ks2",
		"Unlock keyspace ks1",
	}
	tme.expectNoPreviousJournals()
	dryRunResults, err := tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, true)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(wantdryRunReads, *dryRunResults))

	tme.expectNoPreviousJournals()
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, false)
	require.NoError(t, err)
	tme.expectNoPreviousJournals()
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, nil, workflow.DirectionForward, false)
	require.NoError(t, err)

	verifyQueries(t, tme.allDBClients)

	// checkJournals
	tme.dbSourceClients[0].addQueryRE(tsCheckJournals, &sqltypes.Result{}, nil)

	waitForCatchup := func() {
		// mi.waitForCatchup-> mi.wr.tmc.VReplicationWaitForPos
		state := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"pos|state|message",
			"varchar|varchar|varchar"),
			"MariaDB/5-456-892|Running",
		)
		tme.dbTargetClients[0].addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
		tme.dbTargetClients[1].addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)

		// mi.waitForCatchup-> mi.wr.tmc.VReplicationExec('Stopped')
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where id = 1", resultid1, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (1)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where id = 1", resultid1, nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (1)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
	}
	waitForCatchup()

	deleteReverseReplicaion := func() {
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks1' and workflow = 'test_reverse'", resultid34, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.vreplication where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.post_copy_action where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
	}

	createReverseVReplication := func() {
		deleteReverseReplicaion()

		tme.dbSourceClients[0].addQueryRE(`insert into _vt.vreplication.*test_reverse.*ks2.*-80.*t1.*from t1\\".*t2.*from t2\\"`, &sqltypes.Result{InsertID: 1}, nil)
		tme.dbSourceClients[0].addQueryRE(`insert into _vt.vreplication.*test_reverse.*ks2.*80-.*t1.*from t1\\".*t2.*from t2\\"`, &sqltypes.Result{InsertID: 2}, nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	}
	createReverseVReplication()

	createJournals := func() {
		journal1 := "insert into _vt.resharding_journal.*tables.*t1.*t2.*local_position.*MariaDB/5-456-892.*shard_gtids.*80.*MariaDB/5-456-893.*80.*MariaDB/5-456-893.*participants.*0"
		tme.dbSourceClients[0].addQueryRE(journal1, &sqltypes.Result{}, nil)
	}
	createJournals()

	deleteTargetVReplication := func() {
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", resultid1, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", resultid1, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set message = 'FROZEN' where id in (1)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set message = 'FROZEN' where id in (1)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)

		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", resultid1, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", resultid1, nil)
		tme.dbTargetClients[0].addQuery("delete from _vt.vreplication where id in (1)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (1)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("delete from _vt.post_copy_action where vrepl_id in (1)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("delete from _vt.vreplication where id in (1)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("delete from _vt.copy_state where vrepl_id in (1)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("delete from _vt.post_copy_action where vrepl_id in (1)", &sqltypes.Result{}, nil)
	}
	deleteTargetVReplication()

	switchWrites(tme)
	_, results, err := tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", 1*time.Second, false, false, false, true)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(wantdryRunWrites, *results))
}

// TestMigrateFailJournal tests that cancel doesn't get called after point of no return.
// No need to test this for shard migrate because code paths are the same.
func TestMigrateFailJournal(t *testing.T) {
	ctx := context.Background()
	tme := newTestTableMigrater(ctx, t)
	defer tme.close(t)

	tme.expectNoPreviousJournals()
	_, err := tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	tme.expectNoPreviousJournals()
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, nil, workflow.DirectionForward, false)
	require.NoError(t, err)

	// mi.checkJournals
	tme.dbSourceClients[0].addQuery("select val from _vt.resharding_journal where id=7672494164556733923", &sqltypes.Result{}, nil)
	tme.dbSourceClients[1].addQuery("select val from _vt.resharding_journal where id=7672494164556733923", &sqltypes.Result{}, nil)

	// mi.waitForCatchup-> mi.wr.tmc.VReplicationWaitForPos
	state := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"pos|state|message",
		"varchar|varchar|varchar"),
		"MariaDB/5-456-892|Running",
	)
	tme.dbTargetClients[0].addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
	tme.dbTargetClients[0].addQuery("select pos, state, message from _vt.vreplication where id=2", state, nil)
	tme.dbTargetClients[1].addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
	tme.dbTargetClients[1].addQuery("select pos, state, message from _vt.vreplication where id=2", state, nil)

	// mi.waitForCatchup-> mi.wr.tmc.VReplicationExec('stopped for cutover')
	tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where id = 1", resultid1, nil)
	tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (1)", &sqltypes.Result{}, nil)
	tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where id = 2", resultid2, nil)
	tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (2)", &sqltypes.Result{}, nil)
	tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where id = 1", resultid1, nil)
	tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (1)", &sqltypes.Result{}, nil)
	tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where id = 2", resultid2, nil)
	tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (2)", &sqltypes.Result{}, nil)
	tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
	tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
	tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)

	// mi.cancelMigration: these must not get called.
	cancel1 := "update _vt.vreplication set state = 'Running', stop_pos = null where id in (1)"
	cancel2 := "update _vt.vreplication set state = 'Running', stop_pos = null where id in (2)"
	tme.dbTargetClients[0].addQuery(cancel1, &sqltypes.Result{}, nil)
	tme.dbTargetClients[0].addQuery(cancel2, &sqltypes.Result{}, nil)
	tme.dbTargetClients[1].addQuery(cancel1, &sqltypes.Result{}, nil)
	tme.dbTargetClients[1].addQuery(cancel2, &sqltypes.Result{}, nil)

	deleteReverseReplicaion := func() {
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks1' and workflow = 'test_reverse'", resultid34, nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks1' and workflow = 'test_reverse'", resultid34, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.vreplication where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.vreplication where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.post_copy_action where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.copy_state where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.post_copy_action where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
	}

	createReverseVReplication := func() {
		deleteReverseReplicaion()

		tme.dbSourceClients[0].addQueryRE("insert into _vt.vreplication.*test_reverse.*ks2.*-80.*t1.*in_keyrange.*c1.*hash.*-40.*t2.*-40.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
		tme.dbSourceClients[0].addQueryRE("insert into _vt.vreplication.*test_reverse.*ks2.*80-.*t1.*in_keyrange.*c1.*hash.*-40.*t2.*-40.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 2}, nil)
		tme.dbSourceClients[1].addQueryRE("insert into _vt.vreplication.*test_reverse.*ks2.*-80.*t1.*in_keyrange.*c1.*hash.*40-.*t2.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
		tme.dbSourceClients[1].addQueryRE("insert into _vt.vreplication.*test_reverse.*ks2.*80-.*t1.*in_keyrange.*c1.*hash.*40-.*t2.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 2}, nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	}
	createReverseVReplication()

	// Make the journal call fail.
	tme.dbSourceClients[0].addQueryRE("insert into _vt.resharding_journal", nil, errors.New("journaling intentionally failed"))
	tme.dbSourceClients[1].addQueryRE("insert into _vt.resharding_journal", nil, errors.New("journaling intentionally failed"))

	switchWrites(tme)
	_, _, err = tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", 1*time.Second, false, false, true, false)
	want := "journaling intentionally failed"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("SwitchWrites(0 timeout) err: %v, must contain %v", err, want)
	}

	// Verify that cancel didn't happen.
	if tme.dbTargetClients[0].queries[cancel1].exhausted() {
		t.Errorf("tme.dbTargetClients[0].queries[cancel1].exhausted: %v, want false", tme.dbTargetClients[0].queries[cancel1])
	}
	if tme.dbTargetClients[1].queries[cancel1].exhausted() {
		t.Errorf("tme.dbTargetClients[0].queries[cancel1].exhausted: %v, want false", tme.dbTargetClients[0].queries[cancel1])
	}
	if tme.dbTargetClients[0].queries[cancel2].exhausted() {
		t.Errorf("tme.dbTargetClients[0].queries[cancel1].exhausted: %v, want false", tme.dbTargetClients[0].queries[cancel1])
	}
}

func TestTableMigrateJournalExists(t *testing.T) {
	ctx := context.Background()
	tme := newTestTableMigrater(ctx, t)
	defer tme.close(t)

	tme.expectNoPreviousJournals()
	_, err := tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	tme.expectNoPreviousJournals()
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	// mi.checkJournals: Show one journal as created.
	tme.dbSourceClients[0].addQuery("select val from _vt.resharding_journal where id=7672494164556733923", sqltypes.MakeTestResult(sqltypes.MakeTestFields("val", "varbinary"), ""), nil)
	tme.dbSourceClients[1].addQuery("select val from _vt.resharding_journal where id=7672494164556733923", &sqltypes.Result{}, nil)

	// mi.createJournals: Create the missing journal.
	journal2 := "insert into _vt.resharding_journal.*7672494164556733923,.*tables.*t1.*t2.*local_position.*MariaDB/5-456-892.*shard_gtids.*80.*MariaDB/5-456-893.*80.*participants.*40.*40"
	tme.dbSourceClients[1].addQueryRE(journal2, &sqltypes.Result{}, nil)

	// mi.startReverseVReplication
	tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks1'", resultid34, nil)
	tme.dbSourceClients[0].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (3, 4)", &sqltypes.Result{}, nil)
	tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 3", runningResult(3), nil)
	tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 4", runningResult(4), nil)
	tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks1'", resultid34, nil)
	tme.dbSourceClients[1].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (3, 4)", &sqltypes.Result{}, nil)
	tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 3", runningResult(3), nil)
	tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 4", runningResult(4), nil)

	// mi.deleteTargetVReplication
	tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", resultid12, nil)
	tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", resultid12, nil)
	tme.dbTargetClients[0].addQuery("update _vt.vreplication set message = 'FROZEN' where id in (1, 2)", &sqltypes.Result{}, nil)
	tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
	tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	tme.dbTargetClients[1].addQuery("update _vt.vreplication set message = 'FROZEN' where id in (1, 2)", &sqltypes.Result{}, nil)
	tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
	tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)

	switchWrites(tme)
	_, _, err = tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", 1*time.Second, false, false, true, false)
	if err != nil {
		t.Fatal(err)
	}

	// Routes will be redone.
	checkRouting(t, tme.wr, map[string][]string{
		"t1":             {"ks2.t1"},
		"ks1.t1":         {"ks2.t1"},
		"t2":             {"ks2.t2"},
		"ks1.t2":         {"ks2.t2"},
		"t1@replica":     {"ks2.t1"},
		"ks2.t1@replica": {"ks2.t1"},
		"ks1.t1@replica": {"ks2.t1"},
		"t2@replica":     {"ks2.t2"},
		"ks2.t2@replica": {"ks2.t2"},
		"ks1.t2@replica": {"ks2.t2"},
		"t1@rdonly":      {"ks2.t1"},
		"ks2.t1@rdonly":  {"ks2.t1"},
		"ks1.t1@rdonly":  {"ks2.t1"},
		"t2@rdonly":      {"ks2.t2"},
		"ks2.t2@rdonly":  {"ks2.t2"},
		"ks1.t2@rdonly":  {"ks2.t2"},
	})
	// We're showing that there are no denied tables. But in real life,
	// tables on ks1 should be denied from the previous failed attempt.
	checkDenyList(t, tme.ts, "ks1:-40", nil)
	checkDenyList(t, tme.ts, "ks1:40-", nil)
	checkDenyList(t, tme.ts, "ks2:-80", nil)
	checkDenyList(t, tme.ts, "ks2:80-", nil)

	verifyQueries(t, tme.allDBClients)
}

func TestShardMigrateJournalExists(t *testing.T) {
	ctx := context.Background()
	tme := newTestShardMigrater(ctx, t, []string{"-40", "40-"}, []string{"-80", "80-"})
	defer tme.stopTablets(t)

	tme.expectNoPreviousJournals()
	_, err := tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	tme.expectNoPreviousJournals()
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}

	// mi.checkJournals
	tme.dbSourceClients[0].addQuery("select val from _vt.resharding_journal where id=6432976123657117097", sqltypes.MakeTestResult(sqltypes.MakeTestFields("val", "varbinary"), ""), nil)
	tme.dbSourceClients[1].addQuery("select val from _vt.resharding_journal where id=6432976123657117097", &sqltypes.Result{}, nil)

	// mi.creaetJournals: Create the missing journal.
	journal2 := "insert into _vt.resharding_journal.*6432976123657117097.*migration_type:SHARDS.*local_position.*MariaDB/5-456-892.*shard_gtids.*80.*MariaDB/5-456-893.*shard_gtids.*80.*MariaDB/5-456-893.*participants.*40.*40"
	tme.dbSourceClients[1].addQueryRE(journal2, &sqltypes.Result{}, nil)

	// mi.startReverseVReplication
	tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks'", resultid34, nil)
	tme.dbSourceClients[0].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (3, 4)", &sqltypes.Result{}, nil)
	tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 3", runningResult(3), nil)
	tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 4", runningResult(4), nil)
	tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks'", resultid34, nil)
	tme.dbSourceClients[1].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (3, 4)", &sqltypes.Result{}, nil)
	tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 3", runningResult(3), nil)
	tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 4", runningResult(4), nil)

	// mi.deleteTargetVReplication
	tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test'", resultid12, nil)
	tme.dbTargetClients[0].addQuery("update _vt.vreplication set message = 'FROZEN' where id in (1, 2)", &sqltypes.Result{}, nil)
	tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
	tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test'", resultid2, nil)
	tme.dbTargetClients[1].addQuery("update _vt.vreplication set message = 'FROZEN' where id in (2)", &sqltypes.Result{}, nil)
	tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)

	switchWrites(tme)
	_, _, err = tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", 1*time.Second, false, false, true, false)
	if err != nil {
		t.Fatal(err)
	}

	checkServedTypes(t, tme.ts, "ks:-40", 0)
	checkServedTypes(t, tme.ts, "ks:40-", 0)
	checkServedTypes(t, tme.ts, "ks:-80", 3)
	checkServedTypes(t, tme.ts, "ks:80-", 3)

	checkIfPrimaryServing(t, tme.ts, "ks:-40", false)
	checkIfPrimaryServing(t, tme.ts, "ks:40-", false)
	checkIfPrimaryServing(t, tme.ts, "ks:-80", true)
	checkIfPrimaryServing(t, tme.ts, "ks:80-", true)

	verifyQueries(t, tme.allDBClients)
}

func TestTableMigrateCancel(t *testing.T) {
	ctx := context.Background()
	tme := newTestTableMigrater(ctx, t)
	defer tme.close(t)

	tme.expectNoPreviousJournals()
	_, err := tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	tme.expectNoPreviousJournals()
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}

	checkJournals := func() {
		tme.dbSourceClients[0].addQuery("select val from _vt.resharding_journal where id=7672494164556733923", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select val from _vt.resharding_journal where id=7672494164556733923", &sqltypes.Result{}, nil)
	}
	checkJournals()

	deleteReverseReplicaion := func() {
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks1' and workflow = 'test_reverse'", resultid34, nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks1' and workflow = 'test_reverse'", resultid34, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.vreplication where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.vreplication where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.post_copy_action where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.copy_state where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.post_copy_action where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
	}
	cancelMigration := func() {
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", resultid12, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", resultid12, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", runningResult(1), nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 2", runningResult(2), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 1", runningResult(1), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 2", runningResult(2), nil)

		deleteReverseReplicaion()
	}
	cancelMigration()

	switchWrites(tme)
	_, _, err = tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", 1*time.Second, true, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	verifyQueries(t, tme.allDBClients)
}

func TestTableMigrateCancelDryRun(t *testing.T) {
	ctx := context.Background()
	tme := newTestTableMigrater(ctx, t)
	defer tme.close(t)

	want := []string{
		"Lock keyspace ks1",
		"Lock keyspace ks2",
		"Cancel stream migrations as requested",
		"Unlock keyspace ks2",
		"Unlock keyspace ks1",
	}

	tme.expectNoPreviousJournals()
	_, err := tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	tme.expectNoPreviousJournals()
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}

	checkJournals := func() {
		tme.dbSourceClients[0].addQuery("select val from _vt.resharding_journal where id=7672494164556733923", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select val from _vt.resharding_journal where id=7672494164556733923", &sqltypes.Result{}, nil)
	}
	checkJournals()

	deleteReverseReplicaion := func() {
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks1' and workflow = 'test_reverse'", resultid34, nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks1' and workflow = 'test_reverse'", resultid34, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.vreplication where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.vreplication where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.post_copy_action where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.copy_state where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.post_copy_action where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
	}
	cancelMigration := func() {
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", resultid12, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", resultid12, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", runningResult(1), nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 2", runningResult(2), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 1", runningResult(1), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 2", runningResult(2), nil)

		deleteReverseReplicaion()
	}
	cancelMigration()

	switchWrites(tme)
	_, dryRunResults, err := tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", 1*time.Second, true, false, false, true)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(want, *dryRunResults))
}

func TestTableMigrateNoReverse(t *testing.T) {
	ctx := context.Background()
	tme := newTestTableMigrater(ctx, t)
	defer tme.close(t)

	tme.expectNoPreviousJournals()
	_, err := tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	tme.expectNoPreviousJournals()
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}

	checkJournals := func() {
		tme.dbSourceClients[0].addQuery("select val from _vt.resharding_journal where id=7672494164556733923", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select val from _vt.resharding_journal where id=7672494164556733923", &sqltypes.Result{}, nil)
	}
	checkJournals()

	waitForCatchup := func() {
		// mi.waitForCatchup-> mi.wr.tmc.VReplicationWaitForPos
		state := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"pos|state|message",
			"varchar|varchar|varchar"),
			"MariaDB/5-456-892|Running",
		)
		tme.dbTargetClients[0].addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
		tme.dbTargetClients[0].addQuery("select pos, state, message from _vt.vreplication where id=2", state, nil)
		tme.dbTargetClients[1].addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
		tme.dbTargetClients[1].addQuery("select pos, state, message from _vt.vreplication where id=2", state, nil)

		// mi.waitForCatchup-> mi.wr.tmc.VReplicationExec('Stopped')
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where id = 1", resultid1, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (1)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where id = 2", resultid2, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where id = 1", resultid1, nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (1)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where id = 2", resultid2, nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	}
	waitForCatchup()

	deleteReverseReplicaion := func() {
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks1' and workflow = 'test_reverse'", resultid34, nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks1' and workflow = 'test_reverse'", resultid34, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.vreplication where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.vreplication where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.post_copy_action where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.copy_state where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.post_copy_action where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
	}

	createReverseVReplication := func() {
		deleteReverseReplicaion()

		tme.dbSourceClients[0].addQueryRE("insert into _vt.vreplication.*test_reverse.*ks2.*-80.*t1.*in_keyrange.*c1.*hash.*-40.*t2.*-40.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
		tme.dbSourceClients[0].addQueryRE("insert into _vt.vreplication.*test_reverse.*ks2.*80-.*t1.*in_keyrange.*c1.*hash.*-40.*t2.*-40.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 2}, nil)
		tme.dbSourceClients[1].addQueryRE("insert into _vt.vreplication.*test_reverse.*ks2.*-80.*t1.*in_keyrange.*c1.*hash.*40-.*t2.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
		tme.dbSourceClients[1].addQueryRE("insert into _vt.vreplication.*test_reverse.*ks2.*80-.*t1.*in_keyrange.*c1.*hash.*40-.*t2.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 2}, nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	}
	createReverseVReplication()

	createJournals := func() {
		journal1 := "insert into _vt.resharding_journal.*7672494164556733923,.*tables.*t1.*t2.*local_position.*MariaDB/5-456-892.*shard_gtids.*-80.*MariaDB/5-456-893.*participants.*40.*40"
		tme.dbSourceClients[0].addQueryRE(journal1, &sqltypes.Result{}, nil)
		journal2 := "insert into _vt.resharding_journal.*7672494164556733923,.*tables.*t1.*t2.*local_position.*MariaDB/5-456-892.*shard_gtids.*80.*MariaDB/5-456-893.*80.*participants.*40.*40"
		tme.dbSourceClients[1].addQueryRE(journal2, &sqltypes.Result{}, nil)
	}
	createJournals()

	deleteTargetVReplication := func() {
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", resultid12, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", resultid12, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set message = 'FROZEN' where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set message = 'FROZEN' where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	}
	deleteTargetVReplication()

	switchWrites(tme)
	_, _, err = tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", 1*time.Second, false, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	verifyQueries(t, tme.allDBClients)
}

func TestMigrateFrozen(t *testing.T) {
	ctx := context.Background()
	tme := newTestTableMigrater(ctx, t)
	defer tme.close(t)

	tme.expectNoPreviousJournals()
	_, err := tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}

	tme.expectNoPreviousJournals()
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}

	bls1 := &binlogdatapb.BinlogSource{
		Keyspace: "ks1",
		Shard:    "-40",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "",
			}},
		},
	}
	tme.dbTargetClients[0].addQuery(streamInfoKs2, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"id|source|message|cell|tablet_types",
		"int64|varchar|varchar|varchar|varchar"),
		fmt.Sprintf("1|%v|FROZEN||", bls1),
	), nil)
	tme.dbTargetClients[1].addQuery(streamInfoKs2, &sqltypes.Result{}, nil)

	switchWrites(tme)
	_, _, err = tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", 0*time.Second, false, false, true, false)
	if err != nil {
		t.Fatal(err)
	}
	verifyQueries(t, tme.allDBClients)
}

func TestMigrateNoStreamsFound(t *testing.T) {
	ctx := context.Background()
	tme := newTestTableMigrater(ctx, t)
	defer tme.close(t)

	tme.dbTargetClients[0].addQuery(streamInfoKs2, &sqltypes.Result{}, nil)
	tme.dbTargetClients[1].addQuery(streamInfoKs2, &sqltypes.Result{}, nil)

	tme.expectNoPreviousJournals()
	_, err := tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, false)
	want := "workflow test not found in keyspace ks2"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("SwitchReads: %v, must contain %v", err, want)
	}
}

func TestMigrateDistinctSources(t *testing.T) {
	ctx := context.Background()
	tme := newTestTableMigrater(ctx, t)
	defer tme.close(t)

	bls := &binlogdatapb.BinlogSource{
		Keyspace: "ks2",
		Shard:    "-80",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select * from t1 where in_keyrange('-80')",
			}, {
				Match:  "t2",
				Filter: "select * from t2 where in_keyrange('-80')",
			}},
		},
	}
	tme.dbTargetClients[0].addQuery(streamInfoKs2, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"id|source|message|cell|tablet_types",
		"int64|varchar|varchar|varchar|varchar"),
		fmt.Sprintf("1|%v|||", bls),
	), nil)

	tme.expectNoPreviousJournals()
	_, err := tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, false)
	want := "source keyspaces are mismatched across streams"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("SwitchReads: %v, must contain %v", err, want)
	}
}

func TestMigrateMismatchedTables(t *testing.T) {
	ctx := context.Background()
	tme := newTestTableMigrater(ctx, t)
	defer tme.close(t)

	bls := &binlogdatapb.BinlogSource{
		Keyspace: "ks1",
		Shard:    "-40",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select * from t1 where in_keyrange('-80')",
			}},
		},
	}
	tme.dbTargetClients[0].addQuery(streamInfoKs2, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"id|source|message|cell|tablet_types",
		"int64|varchar|varchar|varchar|varchar"),
		fmt.Sprintf("1|%v|||", bls)),
		nil,
	)

	tme.expectNoPreviousJournals()
	_, err := tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, false)
	want := "table lists are mismatched across streams"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("SwitchReads: %v, must contain %v", err, want)
	}
}

func TestTableMigrateAllShardsNotPresent(t *testing.T) {
	ctx := context.Background()
	tme := newTestTableMigrater(ctx, t)
	defer tme.close(t)

	tme.dbTargetClients[0].addQuery(streamInfoKs2, &sqltypes.Result{}, nil)

	tme.expectNoPreviousJournals()
	_, err := tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, false)
	want := "mismatched shards for keyspace"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("SwitchReads: %v, must contain %v", err, want)
	}
}

func TestMigrateNoTableWildcards(t *testing.T) {
	ctx := context.Background()
	tme := newTestTableMigrater(ctx, t)
	defer tme.close(t)

	// validate that no previous journals exist
	tme.dbSourceClients[0].addQueryRE(tsCheckJournals, &sqltypes.Result{}, nil)
	tme.dbSourceClients[1].addQueryRE(tsCheckJournals, &sqltypes.Result{}, nil)

	bls1 := &binlogdatapb.BinlogSource{
		Keyspace: "ks1",
		Shard:    "-40",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "/.*",
				Filter: "",
			}},
		},
	}
	bls2 := &binlogdatapb.BinlogSource{
		Keyspace: "ks1",
		Shard:    "40-",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "/.*",
				Filter: "",
			}},
		},
	}
	tme.dbTargetClients[0].addQuery(streamInfoKs2, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"id|source|message|cell|tablet_types",
		"int64|varchar|varchar|varchar|varchar"),
		fmt.Sprintf("1|%v|||", bls1),
		fmt.Sprintf("2|%v|||", bls2),
	), nil)
	bls3 := &binlogdatapb.BinlogSource{
		Keyspace: "ks1",
		Shard:    "40-",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "/.*",
				Filter: "",
			}},
		},
	}
	tme.dbTargetClients[1].addQuery(streamInfoKs2, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"id|source|message|cell|tablet_types",
		"int64|varchar|varchar|varchar|varchar"),
		fmt.Sprintf("1|%v|||", bls3),
	), nil)
	tme.expectNoPreviousJournals()
	_, err := tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, false)
	want := "cannot migrate streams with wild card table names: /.*"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("SwitchReads: %v, must contain %v", err, want)
	}
}

func TestReverseVReplicationUpdateQuery(t *testing.T) {
	ts := &trafficSwitcher{
		reverseWorkflow: "wf",
	}
	dbname := "db"
	type tCase struct {
		optCells       string
		optTabletTypes string
		targetCell     string
		sourceCell     string
		want           string
	}
	updateQuery := "update _vt.vreplication set cell = '%s', tablet_types = '%s' where workflow = 'wf' and db_name = 'db'"
	tCases := []tCase{
		{
			targetCell: "cell1", sourceCell: "cell1", optCells: "cell1", optTabletTypes: "",
			want: fmt.Sprintf(updateQuery, "cell1", ""),
		},
		{
			targetCell: "cell1", sourceCell: "cell2", optCells: "cell1", optTabletTypes: "",
			want: fmt.Sprintf(updateQuery, "cell2", ""),
		},
		{
			targetCell: "cell1", sourceCell: "cell2", optCells: "cell2", optTabletTypes: "",
			want: fmt.Sprintf(updateQuery, "cell2", ""),
		},
		{
			targetCell: "cell1", sourceCell: "cell1", optCells: "cell1,cell2", optTabletTypes: "replica,primary",
			want: fmt.Sprintf(updateQuery, "cell1,cell2", "replica,primary"),
		},
		{
			targetCell: "cell1", sourceCell: "cell1", optCells: "", optTabletTypes: "replica,primary",
			want: fmt.Sprintf(updateQuery, "", "replica,primary"),
		},
	}
	for _, tc := range tCases {
		t.Run("", func(t *testing.T) {
			ts.optCells = tc.optCells
			ts.optTabletTypes = tc.optTabletTypes
			got := ts.getReverseVReplicationUpdateQuery(tc.targetCell, tc.sourceCell, dbname)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestShardMigrateNoAvailableTabletsForReverseReplication(t *testing.T) {
	ctx := context.Background()
	tme := newTestShardMigrater(ctx, t, []string{"-40", "40-"}, []string{"-80", "80-"})
	defer tme.stopTablets(t)

	// Initial check
	checkServedTypes(t, tme.ts, "ks:-40", 3)
	checkServedTypes(t, tme.ts, "ks:40-", 3)
	checkServedTypes(t, tme.ts, "ks:-80", 0)
	checkServedTypes(t, tme.ts, "ks:80-", 0)

	tme.expectNoPreviousJournals()
	//-------------------------------------------------------------------------------------------------------------------
	// Single cell RDONLY migration.
	_, err := tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, []string{"cell1"}, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkCellServedTypes(t, tme.ts, "ks:-40", "cell1", 2)
	checkCellServedTypes(t, tme.ts, "ks:40-", "cell1", 2)
	checkCellServedTypes(t, tme.ts, "ks:-80", "cell1", 1)
	checkCellServedTypes(t, tme.ts, "ks:80-", "cell1", 1)
	checkCellServedTypes(t, tme.ts, "ks:-40", "cell2", 3)
	checkCellServedTypes(t, tme.ts, "ks:40-", "cell2", 3)
	checkCellServedTypes(t, tme.ts, "ks:-80", "cell2", 0)
	checkCellServedTypes(t, tme.ts, "ks:80-", "cell2", 0)
	verifyQueries(t, tme.allDBClients)

	tme.expectNoPreviousJournals()
	//-------------------------------------------------------------------------------------------------------------------
	// Other cell REPLICA migration.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, []string{"cell2"}, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkCellServedTypes(t, tme.ts, "ks:-40", "cell1", 2)
	checkCellServedTypes(t, tme.ts, "ks:40-", "cell1", 2)
	checkCellServedTypes(t, tme.ts, "ks:-80", "cell1", 1)
	checkCellServedTypes(t, tme.ts, "ks:80-", "cell1", 1)
	checkCellServedTypes(t, tme.ts, "ks:-40", "cell2", 1)
	checkCellServedTypes(t, tme.ts, "ks:40-", "cell2", 1)
	checkCellServedTypes(t, tme.ts, "ks:-80", "cell2", 2)
	checkCellServedTypes(t, tme.ts, "ks:80-", "cell2", 2)
	verifyQueries(t, tme.allDBClients)

	tme.expectNoPreviousJournals()
	//-------------------------------------------------------------------------------------------------------------------
	// Single cell backward REPLICA migration.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, []string{"cell2"}, workflow.DirectionBackward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkCellServedTypes(t, tme.ts, "ks:-40", "cell1", 2)
	checkCellServedTypes(t, tme.ts, "ks:40-", "cell1", 2)
	checkCellServedTypes(t, tme.ts, "ks:-80", "cell1", 1)
	checkCellServedTypes(t, tme.ts, "ks:80-", "cell1", 1)
	checkCellServedTypes(t, tme.ts, "ks:-40", "cell2", 3)
	checkCellServedTypes(t, tme.ts, "ks:40-", "cell2", 3)
	checkCellServedTypes(t, tme.ts, "ks:-80", "cell2", 0)
	checkCellServedTypes(t, tme.ts, "ks:80-", "cell2", 0)
	verifyQueries(t, tme.allDBClients)

	tme.expectNoPreviousJournals()
	//-------------------------------------------------------------------------------------------------------------------
	// Switch all RDONLY.
	// This is an extra step that does not exist in the tables test.
	// The per-cell migration mechanism is different for tables. So, this
	// extra step is needed to bring things in sync.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkServedTypes(t, tme.ts, "ks:-40", 2)
	checkServedTypes(t, tme.ts, "ks:40-", 2)
	checkServedTypes(t, tme.ts, "ks:-80", 1)
	checkServedTypes(t, tme.ts, "ks:80-", 1)
	verifyQueries(t, tme.allDBClients)

	tme.expectNoPreviousJournals()
	//-------------------------------------------------------------------------------------------------------------------
	// Switch all REPLICA.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkServedTypes(t, tme.ts, "ks:-40", 1)
	checkServedTypes(t, tme.ts, "ks:40-", 1)
	checkServedTypes(t, tme.ts, "ks:-80", 2)
	checkServedTypes(t, tme.ts, "ks:80-", 2)
	verifyQueries(t, tme.allDBClients)

	tme.expectNoPreviousJournals()
	//-------------------------------------------------------------------------------------------------------------------
	// All cells RDONLY backward migration.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionBackward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkServedTypes(t, tme.ts, "ks:-40", 2)
	checkServedTypes(t, tme.ts, "ks:40-", 2)
	checkServedTypes(t, tme.ts, "ks:-80", 1)
	checkServedTypes(t, tme.ts, "ks:80-", 1)
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Can't switch primary with SwitchReads.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_PRIMARY}, nil, workflow.DirectionForward, false)
	want := "tablet type must be REPLICA or RDONLY: PRIMARY"
	if err == nil || err.Error() != want {
		t.Errorf("SwitchReads(primary) err: %v, want %v", err, want)
	}
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Test SwitchWrites cancelation on failure.

	tme.expectNoPreviousJournals()
	// Switch all the reads first.
	_, err = tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_RDONLY}, nil, workflow.DirectionForward, false)
	if err != nil {
		t.Fatal(err)
	}
	checkServedTypes(t, tme.ts, "ks:-40", 1)
	checkServedTypes(t, tme.ts, "ks:40-", 1)
	checkServedTypes(t, tme.ts, "ks:-80", 2)
	checkServedTypes(t, tme.ts, "ks:80-", 2)
	checkIfPrimaryServing(t, tme.ts, "ks:-40", true)
	checkIfPrimaryServing(t, tme.ts, "ks:40-", true)
	checkIfPrimaryServing(t, tme.ts, "ks:-80", false)
	checkIfPrimaryServing(t, tme.ts, "ks:80-", false)

	checkJournals := func() {
		tme.dbSourceClients[0].addQuery("select val from _vt.resharding_journal where id=6432976123657117097", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select val from _vt.resharding_journal where id=6432976123657117097", &sqltypes.Result{}, nil)
	}
	checkJournals()

	stopStreams := func() {
		tme.dbSourceClients[0].addQuery("select id, workflow, source, pos, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name='vt_ks' and workflow != 'test_reverse' and state = 'Stopped' and message != 'FROZEN'", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select id, workflow, source, pos, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name='vt_ks' and workflow != 'test_reverse' and state = 'Stopped' and message != 'FROZEN'", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("select id, workflow, source, pos, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name='vt_ks' and workflow != 'test_reverse'", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select id, workflow, source, pos, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name='vt_ks' and workflow != 'test_reverse'", &sqltypes.Result{}, nil)
	}
	stopStreams()

	deleteReverseReplicaion := func() {
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test_reverse'", resultid3, nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test_reverse'", resultid34, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.vreplication where id in (3)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.vreplication where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (3)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("delete from _vt.post_copy_action where vrepl_id in (3)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.copy_state where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("delete from _vt.post_copy_action where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
	}
	cancelMigration := func() {
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow != 'test_reverse'", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow != 'test_reverse'", &sqltypes.Result{}, nil)

		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test'", resultid12, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test'", resultid2, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", runningResult(1), nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 2", runningResult(2), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 2", runningResult(2), nil)

		deleteReverseReplicaion()
	}
	cancelMigration()

	switchWrites(tme)
	_, _, err = tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", 0*time.Second, false, false, true, false)
	want = "DeadlineExceeded"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("SwitchWrites(0 timeout) err: %v, must contain %v", err, want)
	}

	verifyQueries(t, tme.allDBClients)
	checkServedTypes(t, tme.ts, "ks:-40", 1)
	checkServedTypes(t, tme.ts, "ks:40-", 1)
	checkServedTypes(t, tme.ts, "ks:-80", 2)
	checkServedTypes(t, tme.ts, "ks:80-", 2)
	checkIfPrimaryServing(t, tme.ts, "ks:-40", true)
	checkIfPrimaryServing(t, tme.ts, "ks:40-", true)
	checkIfPrimaryServing(t, tme.ts, "ks:-80", false)
	checkIfPrimaryServing(t, tme.ts, "ks:80-", false)

	//-------------------------------------------------------------------------------------------------------------------
	// Test successful SwitchWrites.

	checkJournals()
	stopStreams()

	waitForCatchup := func() {
		// mi.waitForCatchup-> mi.wr.tmc.VReplicationWaitForPos
		state := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"pos|state|message",
			"varchar|varchar|varchar"),
			"MariaDB/5-456-892|Running",
		)
		tme.dbTargetClients[0].addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
		tme.dbTargetClients[1].addQuery("select pos, state, message from _vt.vreplication where id=2", state, nil)
		tme.dbTargetClients[0].addQuery("select pos, state, message from _vt.vreplication where id=2", state, nil)

		// mi.waitForCatchup-> mi.wr.tmc.VReplicationExec('stopped for cutover')
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where id = 1", resultid1, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (1)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where id = 2", resultid2, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where id = 2", resultid2, nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	}
	waitForCatchup()

	createReverseVReplication := func() {
		deleteReverseReplicaion()

		tme.dbSourceClients[0].addQueryRE("insert into _vt.vreplication.*-80.*-40.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
		tme.dbSourceClients[1].addQueryRE("insert into _vt.vreplication.*-80.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
		tme.dbSourceClients[1].addQueryRE("insert into _vt.vreplication.*80-.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 2}, nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	}
	createReverseVReplication()

	createJournals := func() {
		journal1 := "insert into _vt.resharding_journal.*6432976123657117097.*migration_type:SHARDS.*local_position.*MariaDB/5-456-892.*shard_gtids.*-80.*MariaDB/5-456-893.*participants.*40.*40"
		tme.dbSourceClients[0].addQueryRE(journal1, &sqltypes.Result{}, nil)
		journal2 := "insert into _vt.resharding_journal.*6432976123657117097.*migration_type:SHARDS.*local_position.*MariaDB/5-456-892.*shard_gtids.*80.*MariaDB/5-456-893.*shard_gtids.*80.*MariaDB/5-456-893.*participants.*40.*40"
		tme.dbSourceClients[1].addQueryRE(journal2, &sqltypes.Result{}, nil)
	}
	createJournals()

	startReverseVReplication := func() {
		tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks'", resultid34, nil)
		tme.dbSourceClients[0].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 3", runningResult(3), nil)
		tme.dbSourceClients[0].addQuery("select * from _vt.vreplication where id = 4", runningResult(4), nil)
		tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks'", resultid34, nil)
		tme.dbSourceClients[1].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (3, 4)", &sqltypes.Result{}, nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 3", runningResult(3), nil)
		tme.dbSourceClients[1].addQuery("select * from _vt.vreplication where id = 4", runningResult(4), nil)
	}
	startReverseVReplication()

	freezeTargetVReplication := func() {
		tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test'", resultid12, nil)
		tme.dbTargetClients[0].addQuery("update _vt.vreplication set message = 'FROZEN' where id in (1, 2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
		tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test'", resultid2, nil)
		tme.dbTargetClients[1].addQuery("update _vt.vreplication set message = 'FROZEN' where id in (2)", &sqltypes.Result{}, nil)
		tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	}
	freezeTargetVReplication()

	// Temporarily set tablet types to RDONLY to test that SwitchWrites fails if no tablets of rdonly are available
	invariants := make(map[string]*sqltypes.Result)
	for i := range tme.targetShards {
		invariants[fmt.Sprintf("%s-%d", streamInfoKs, i)] = tme.dbTargetClients[i].getInvariant(streamInfoKs)
		tme.dbTargetClients[i].addInvariant(streamInfoKs, tme.dbTargetClients[i].getInvariant(streamInfoKs+"-rdonly"))
	}
	_, _, err = tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", 1*time.Second, false, false, true, false)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "no tablet found"))
	require.True(t, strings.Contains(err.Error(), "-80"))
	require.True(t, strings.Contains(err.Error(), "80-"))
	require.False(t, strings.Contains(err.Error(), "40"))
	for i := range tme.targetShards {
		tme.dbTargetClients[i].addInvariant(streamInfoKs, invariants[fmt.Sprintf("%s-%d", streamInfoKs, i)])
	}

	journalID, _, err := tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", 1*time.Second, false, false, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if journalID != 6432976123657117097 {
		t.Errorf("journal id: %d, want 6432976123657117097", journalID)
	}

	verifyQueries(t, tme.allDBClients)

	checkServedTypes(t, tme.ts, "ks:-40", 0)
	checkServedTypes(t, tme.ts, "ks:40-", 0)
	checkServedTypes(t, tme.ts, "ks:-80", 3)
	checkServedTypes(t, tme.ts, "ks:80-", 3)

	checkIfPrimaryServing(t, tme.ts, "ks:-40", false)
	checkIfPrimaryServing(t, tme.ts, "ks:40-", false)
	checkIfPrimaryServing(t, tme.ts, "ks:-80", true)
	checkIfPrimaryServing(t, tme.ts, "ks:80-", true)

	verifyQueries(t, tme.allDBClients)
}

func TestIsPartialMoveTables(t *testing.T) {
	ts := &trafficSwitcher{}
	type testCase struct {
		name                       string
		sourceShards, targetShards []string
		want                       bool
	}
	testCases := []testCase{
		{
			name:         "-80",
			sourceShards: []string{"-80"},
			targetShards: []string{"-80"},
			want:         true,
		},
		{
			name:         "80-",
			sourceShards: []string{"80-"},
			targetShards: []string{"80-"},
			want:         true,
		},
		{
			name:         "-80,80-",
			sourceShards: []string{"-80", "80-"},
			targetShards: []string{"-80", "80-"},
			want:         false,
		},
		{
			name:         "mismatch",
			sourceShards: []string{"-c0", "c0-"},
			targetShards: []string{"-80", "80-"},
			want:         false,
		},
		{
			name:         "different number of shards",
			sourceShards: []string{"-a0", "a0-c0", "c0-"},
			targetShards: []string{"-80", "80-"},
			want:         false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ts.isPartialMoveTables(tc.sourceShards, tc.targetShards)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})

	}
}

func checkRouting(t *testing.T, wr *Wrangler, want map[string][]string) {
	t.Helper()
	ctx := context.Background()
	got, err := topotools.GetRoutingRules(ctx, wr.ts)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("rules:\n%v, want\n%v", got, want)
	}
	cells, err := wr.ts.GetCellInfoNames(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, cell := range cells {
		checkCellRouting(t, wr, cell, want)
	}
}

func checkCellRouting(t *testing.T, wr *Wrangler, cell string, want map[string][]string) {
	t.Helper()
	ctx := context.Background()
	svs, err := wr.ts.GetSrvVSchema(ctx, cell)
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string][]string)
	for _, rr := range svs.RoutingRules.Rules {
		got[rr.FromTable] = append(got[rr.FromTable], rr.ToTables...)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ERROR: routing rules don't match for cell %s:got\n%v, want\n%v", cell, got, want)
	}
}

func checkDenyList(t *testing.T, ts *topo.Server, keyspaceShard string, want []string) {
	t.Helper()
	ctx := context.Background()
	splits := strings.Split(keyspaceShard, ":")
	si, err := ts.GetShard(ctx, splits[0], splits[1])
	if err != nil {
		t.Fatal(err)
	}
	tc := si.GetTabletControl(topodatapb.TabletType_PRIMARY)
	var got []string
	if tc != nil {
		got = tc.DeniedTables
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Denied tables for %v: %v, want %v", keyspaceShard, got, want)
	}
}

func checkServedTypes(t *testing.T, ts *topo.Server, keyspaceShard string, want int) {
	t.Helper()
	ctx := context.Background()
	splits := strings.Split(keyspaceShard, ":")
	si, err := ts.GetShard(ctx, splits[0], splits[1])
	if err != nil {
		t.Fatal(err)
	}

	servedTypes, err := ts.GetShardServingTypes(ctx, si)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, want, len(servedTypes), fmt.Sprintf("shard %v has wrong served types: got: %v, want: %v",
		keyspaceShard, len(servedTypes), want))
}

func checkCellServedTypes(t *testing.T, ts *topo.Server, keyspaceShard, cell string, want int) {
	t.Helper()
	ctx := context.Background()
	splits := strings.Split(keyspaceShard, ":")
	srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, splits[0])
	if err != nil {
		t.Fatal(err)
	}
	count := 0
outer:
	for _, partition := range srvKeyspace.GetPartitions() {
		for _, ref := range partition.ShardReferences {
			if ref.Name == splits[1] {
				count++
				continue outer
			}
		}
	}
	require.Equal(t, want, count, fmt.Sprintf("serving types for keyspaceShard %s, cell %s: %d, want %d",
		keyspaceShard, cell, count, want))
}

func checkIfPrimaryServing(t *testing.T, ts *topo.Server, keyspaceShard string, want bool) {
	t.Helper()
	ctx := context.Background()
	splits := strings.Split(keyspaceShard, ":")
	si, err := ts.GetShard(ctx, splits[0], splits[1])
	if err != nil {
		t.Fatal(err)
	}
	if want != si.IsPrimaryServing {
		t.Errorf("IsPrimaryServing(%v): %v, want %v", keyspaceShard, si.IsPrimaryServing, want)
	}
}

func getResult(id int, state string, keyspace string, shard string) *sqltypes.Result {
	return sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"id|state|cell|tablet_types|source",
		"int64|varchar|varchar|varchar|varchar"),
		fmt.Sprintf("%d|%s|cell1|PRIMARY|keyspace:\"%s\" shard:\"%s\"", id, state, keyspace, shard),
	)
}

func stoppedResult(id int) *sqltypes.Result {
	return getResult(id, binlogdatapb.VReplicationWorkflowState_Stopped.String(), tpChoice.keyspace, tpChoice.shard)
}

func runningResult(id int) *sqltypes.Result {
	return getResult(id, binlogdatapb.VReplicationWorkflowState_Running.String(), tpChoice.keyspace, tpChoice.shard)
}

func switchWrites(tmeT any) {
	if tme, ok := tmeT.(*testMigraterEnv); ok {
		tme.tmeDB.AddQuery("lock tables `t1` read,`t2` read", &sqltypes.Result{})
	} else if tme, ok := tmeT.(*testShardMigraterEnv); ok {
		tme.tmeDB.AddQuery("lock tables `t1` read,`t2` read", &sqltypes.Result{})
	}
}
