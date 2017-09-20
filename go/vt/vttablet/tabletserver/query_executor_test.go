/*
Copyright 2017 Google Inc.

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

package tabletserver

import (
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/mysql/fakesqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/callinfo/fakecallinfo"
	"github.com/youtube/vitess/go/vt/tableacl"
	"github.com/youtube/vitess/go/vt/tableacl/simpleacl"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/connpool"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/messager"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/rules"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	tableaclpb "github.com/youtube/vitess/go/vt/proto/tableacl"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestQueryExecutorPlanDDL(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "alter table test_table add zipcode int"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanDDL, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanPassDmlRBR(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "update test_table set pk = foo()"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	ctx := context.Background()
	// RBR mode
	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()
	txid := newTransaction(tsv, nil)
	qre := newTestQueryExecutor(ctx, tsv, query, txid)
	tsv.qe.binlogFormat = connpool.BinlogFormatRow
	checkPlanID(t, planbuilder.PlanPassDML, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	wantqueries := []string{query}
	gotqueries := fetchRecordedQueries(qre)
	if !reflect.DeepEqual(gotqueries, wantqueries) {
		t.Errorf("queries: %v, want %v", gotqueries, wantqueries)
	}

	// Statement mode
	tsv.qe.binlogFormat = connpool.BinlogFormatStatement
	_, err = qre.Execute()
	if code := vterrors.Code(err); code != vtrpcpb.Code_UNIMPLEMENTED {
		t.Errorf("qre.Execute: %v, want %v", code, vtrpcpb.Code_INVALID_ARGUMENT)
	}
	testCommitHelper(t, tsv, qre)
}

func TestQueryExecutorPlanPassDmlAutoCommitRBR(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "update test_table set pk = foo()"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	ctx := context.Background()
	// RBR mode
	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	tsv.qe.binlogFormat = connpool.BinlogFormatRow
	checkPlanID(t, planbuilder.PlanPassDML, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}

	// Statement mode
	tsv.qe.binlogFormat = connpool.BinlogFormatStatement
	_, err = qre.Execute()
	if code := vterrors.Code(err); code != vtrpcpb.Code_UNIMPLEMENTED {
		t.Errorf("qre.Execute: %v, want %v", code, vtrpcpb.Code_INVALID_ARGUMENT)
	}
}

func TestQueryExecutorPlanPassDmlReplaceInto(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "replace into test_table values (1)"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	ctx := context.Background()
	// RBR mode
	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()
	txid := newTransaction(tsv, nil)
	qre := newTestQueryExecutor(ctx, tsv, query, txid)
	tsv.qe.binlogFormat = connpool.BinlogFormatRow
	checkPlanID(t, planbuilder.PlanPassDML, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	wantqueries := []string{query}
	gotqueries := fetchRecordedQueries(qre)
	if !reflect.DeepEqual(gotqueries, wantqueries) {
		t.Errorf("queries: %v, want %v", gotqueries, wantqueries)
	}

	// Statement mode
	tsv.qe.binlogFormat = connpool.BinlogFormatStatement
	_, err = qre.Execute()
	if code := vterrors.Code(err); code != vtrpcpb.Code_UNIMPLEMENTED {
		t.Errorf("qre.Execute: %v, want %v", code, vtrpcpb.Code_INVALID_ARGUMENT)
	}
	testCommitHelper(t, tsv, qre)
}

func TestQueryExecutorPlanInsertPk(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	db.AddQuery("insert into test_table(pk) values (1) /* _stream test_table (pk ) (1 ); */", &sqltypes.Result{})
	want := &sqltypes.Result{}
	query := "insert into test_table(pk) values(1)"
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanInsertPK, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanInsertMessage(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	db.AddQueryPattern("insert into msg\\(time_scheduled, id, message, time_next, time_created, epoch\\) values \\(1, 2, 3, 1,.*", &sqltypes.Result{})
	db.AddQuery(
		"select time_next, epoch, time_created, id, time_scheduled, message from msg where (time_scheduled = 1 and id = 2)",
		&sqltypes.Result{
			Fields: []*querypb.Field{
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
			},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewVarBinary("1"),
				sqltypes.NewVarBinary("0"),
				sqltypes.NewVarBinary("10"),
				sqltypes.NewVarBinary("1"),
				sqltypes.NewVarBinary("10"),
				sqltypes.NewVarBinary("2"),
			}},
		},
	)
	want := &sqltypes.Result{}
	query := "insert into msg(time_scheduled, id, message) values(1, 2, 3)"
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanInsertMessage, qre.plan.PlanID)
	ch1 := make(chan *sqltypes.Result)
	count := 0
	tsv.messager.Subscribe(context.Background(), "msg", func(qr *sqltypes.Result) error {
		if count > 1 {
			return io.EOF
		}
		count++
		ch1 <- qr
		return nil
	})
	<-ch1
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	mr := <-ch1
	wantqr := &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NewInt64(10),
			sqltypes.NewInt64(2),
		}},
	}
	if !reflect.DeepEqual(mr, wantqr) {
		t.Errorf("rows:\n%+v, want\n%+v", mr, wantqr)
	}

	txid := newTransaction(tsv, nil)
	qre = newTestQueryExecutor(ctx, tsv, query, txid)
	defer testCommitHelper(t, tsv, qre)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorInsertMessageACL(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int63())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group02",
			TableNamesOrPrefixes: []string{"msg"},
			Readers:              []string{"u2"},
		}},
	}
	if err := tableacl.InitFromProto(config); err != nil {
		t.Fatalf("unable to load tableacl config, error: %v", err)
	}

	db := setUpQueryExecutorTest(t)
	defer db.Close()

	query := "insert into msg(time_scheduled, id, message) values(1, 2, 3)"

	callerID := &querypb.VTGateCallerID{
		Username: "u2",
	}
	ctx := callerid.NewContext(context.Background(), nil, callerID)

	tsv := newTestTabletServer(ctx, enableStrictTableACL, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()

	_, err := qre.Execute()
	want := `table acl error: "u2" [] cannot run INSERT_MESSAGE on table "msg"`
	if err == nil || err.Error() != want {
		t.Errorf("qre.Execute(insert into msg) error: %v, want %s", err, want)
	}
}

func TestQueryExecutorPlanInsertSubQueryAutoCommmit(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "insert into test_table(pk) select pk from test_table where pk = 2"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	selectQuery := "select pk from test_table where pk = 2 limit 10001"
	db.AddQuery(selectQuery, &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "pk",
			Type: sqltypes.Int32,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewInt32(2)},
		},
	})

	insertQuery := "insert into test_table(pk) values (2) /* _stream test_table (pk ) (2 ); */"

	db.AddQuery(insertQuery, &sqltypes.Result{})
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanInsertSubquery, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanInsertSubQuery(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "insert into test_table(pk) select pk from test_table where pk = 2"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	selectQuery := "select pk from test_table where pk = 2 limit 10001"
	db.AddQuery(selectQuery, &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "pk",
			Type: sqltypes.Int32,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewInt32(2)},
		},
	})

	insertQuery := "insert into test_table(pk) values (2) /* _stream test_table (pk ) (2 ); */"

	db.AddQuery(insertQuery, &sqltypes.Result{})
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	txid := newTransaction(tsv, nil)
	qre := newTestQueryExecutor(ctx, tsv, query, txid)

	defer tsv.StopService()
	defer testCommitHelper(t, tsv, qre)
	checkPlanID(t, planbuilder.PlanInsertSubquery, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	wantqueries := []string{"insert into test_table(pk) values (2) /* _stream test_table (pk ) (2 ); */"}
	gotqueries := fetchRecordedQueries(qre)
	if !reflect.DeepEqual(gotqueries, wantqueries) {
		t.Errorf("queries: %v, want %v", gotqueries, wantqueries)
	}
}

func TestQueryExecutorPlanInsertSubQueryRBR(t *testing.T) {
	// RBR test is almost identical to the non-RBR test, except that
	// the _stream comments are suppressed for RBR.
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "insert into test_table(pk) select pk from test_table where pk = 2"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	selectQuery := "select pk from test_table where pk = 2 limit 10001"
	db.AddQuery(selectQuery, &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "pk",
			Type: sqltypes.Int32,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewInt32(2)},
		},
	})

	insertQuery := "insert into test_table(pk) values (2)"

	db.AddQuery(insertQuery, &sqltypes.Result{})
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	txid := newTransaction(tsv, nil)
	qre := newTestQueryExecutor(ctx, tsv, query, txid)
	tsv.qe.binlogFormat = connpool.BinlogFormatRow

	defer tsv.StopService()
	defer testCommitHelper(t, tsv, qre)
	checkPlanID(t, planbuilder.PlanInsertSubquery, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	wantqueries := []string{"insert into test_table(pk) values (2)"}
	gotqueries := fetchRecordedQueries(qre)
	if !reflect.DeepEqual(gotqueries, wantqueries) {
		t.Errorf("queries: %v, want %v", gotqueries, wantqueries)
	}
}

func TestQueryExecutorPlanUpsertPk(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	db.AddQuery("insert into test_table(pk) values (1) /* _stream test_table (pk ) (1 ); */", &sqltypes.Result{})
	want := &sqltypes.Result{}
	query := "insert into test_table(pk) values(1) on duplicate key update val=1"
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	txid := newTransaction(tsv, nil)
	qre := newTestQueryExecutor(ctx, tsv, query, txid)
	defer tsv.StopService()
	defer testCommitHelper(t, tsv, qre)
	checkPlanID(t, planbuilder.PlanUpsertPK, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	wantqueries := []string{"insert into test_table(pk) values (1) /* _stream test_table (pk ) (1 ); */"}
	gotqueries := fetchRecordedQueries(qre)
	if !reflect.DeepEqual(gotqueries, wantqueries) {
		t.Errorf("queries: %v, want %v", gotqueries, wantqueries)
	}

	db.AddRejectedQuery("insert into test_table(pk) values (1) /* _stream test_table (pk ) (1 ); */", errRejected)
	txid = newTransaction(tsv, nil)
	qre = newTestQueryExecutor(ctx, tsv, query, txid)
	defer testCommitHelper(t, tsv, qre)
	_, err = qre.Execute()
	wantErr := "rejected"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("qre.Execute() = %v, want %v", err, wantErr)
	}
	if gotqueries = fetchRecordedQueries(qre); gotqueries != nil {
		t.Errorf("queries: %v, want nil", gotqueries)
	}

	db.AddRejectedQuery(
		"insert into test_table(pk) values (1) /* _stream test_table (pk ) (1 ); */",
		mysql.NewSQLError(mysql.ERDupEntry, mysql.SSDupKey, "err"),
	)
	db.AddQuery("update test_table(pk) set val = 1 where pk in (1) /* _stream test_table (pk ) (1 ); */", &sqltypes.Result{})
	txid = newTransaction(tsv, nil)
	qre = newTestQueryExecutor(ctx, tsv, query, txid)
	defer testCommitHelper(t, tsv, qre)
	_, err = qre.Execute()
	wantErr = "err (errno 1062) (sqlstate 23000)"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("qre.Execute() = %v, want %v", err, wantErr)
	}
	wantqueries = []string{}
	if gotqueries = fetchRecordedQueries(qre); gotqueries != nil {
		t.Errorf("queries: %v, want nil", gotqueries)
	}

	db.AddRejectedQuery(
		"insert into test_table(pk) values (1) /* _stream test_table (pk ) (1 ); */",
		mysql.NewSQLError(mysql.ERDupEntry, mysql.SSDupKey, "ERROR 1062 (23000): Duplicate entry '2' for key 'PRIMARY'"),
	)
	db.AddQuery(
		"update test_table set val = 1 where pk in (1) /* _stream test_table (pk ) (1 ); */",
		&sqltypes.Result{RowsAffected: 1},
	)
	txid = newTransaction(tsv, nil)
	qre = newTestQueryExecutor(ctx, tsv, query, txid)
	defer testCommitHelper(t, tsv, qre)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	want = &sqltypes.Result{
		RowsAffected: 2,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got: %v, want: %v", got, want)
	}
	wantqueries = []string{"update test_table set val = 1 where pk in (1) /* _stream test_table (pk ) (1 ); */"}
	gotqueries = fetchRecordedQueries(qre)
	if !reflect.DeepEqual(gotqueries, wantqueries) {
		t.Errorf("queries: %v, want %v", gotqueries, wantqueries)
	}

	// Test pk change.
	db.AddRejectedQuery(
		"insert into test_table(pk) values (1) /* _stream test_table (pk ) (1 ); */",
		mysql.NewSQLError(mysql.ERDupEntry, mysql.SSDupKey, "ERROR 1062 (23000): Duplicate entry '2' for key 'PRIMARY'"),
	)
	db.AddQuery(
		"update test_table set pk = 2 where pk in (1) /* _stream test_table (pk ) (1 ) (2 ); */",
		&sqltypes.Result{RowsAffected: 1},
	)
	txid = newTransaction(tsv, nil)
	qre = newTestQueryExecutor(ctx, tsv, "insert into test_table(pk) values (1) on duplicate key update pk=2", txid)
	defer testCommitHelper(t, tsv, qre)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	want = &sqltypes.Result{
		RowsAffected: 2,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got: %v, want: %v", got, want)
	}
	wantqueries = []string{"update test_table set pk = 2 where pk in (1) /* _stream test_table (pk ) (1 ) (2 ); */"}
	gotqueries = fetchRecordedQueries(qre)
	if !reflect.DeepEqual(gotqueries, wantqueries) {
		t.Errorf("queries: %v, want %v", gotqueries, wantqueries)
	}
}

func TestQueryExecutorPlanUpsertPkSingleUnique(t *testing.T) {
	db := setUpQueryExecutorTestWithOneUniqueKey(t)
	defer db.Close()
	query := "insert into test_table(pk) values (1) on duplicate key update val = 1 /* _stream test_table (pk ) (1 ); */"
	db.AddQuery(query, &sqltypes.Result{})
	db.AddRejectedQuery("insert into test_table(pk) values (1) /* _stream test_table (pk ) (1 ); */", errRejected)
	want := &sqltypes.Result{}
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	txid := newTransaction(tsv, nil)
	qre := newTestQueryExecutor(ctx, tsv, query[0:strings.Index(query, " /*")], txid)
	defer tsv.StopService()
	defer testCommitHelper(t, tsv, qre)
	checkPlanID(t, planbuilder.PlanInsertPK, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	wantqueries := []string{query}
	gotqueries := fetchRecordedQueries(qre)
	if !reflect.DeepEqual(gotqueries, wantqueries) {
		t.Errorf("queries: %v, want %v", gotqueries, wantqueries)
	}

	// PK changed by upsert.
	query = "insert into test_table(pk) values (1), (2), (3) on duplicate key update pk = 5 /* _stream test_table (pk ) (1 ) (2 ) (3 ) (5 ) (5 ) (5 ); */"
	db.AddQuery(query, &sqltypes.Result{})
	want = &sqltypes.Result{}
	ctx = context.Background()
	tsv = newTestTabletServer(ctx, noFlags, db)
	txid = newTransaction(tsv, nil)
	qre = newTestQueryExecutor(ctx, tsv, query[0:strings.Index(query, " /*")], txid)
	defer testCommitHelper(t, tsv, qre)
	checkPlanID(t, planbuilder.PlanInsertPK, qre.plan.PlanID)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	wantqueries = []string{query}
	gotqueries = fetchRecordedQueries(qre)
	if !reflect.DeepEqual(gotqueries, wantqueries) {
		t.Errorf("queries: %v, want %v", gotqueries, wantqueries)
	}

	// PK changed by using values.
	query = "insert into test_table(pk, name) values (1, 4), (2, 5), (3, 6) on duplicate key update pk = values(name) /* _stream test_table (pk ) (1 ) (2 ) (3 ) (4 ) (5 ) (6 ); */"
	db.AddQuery(query, &sqltypes.Result{})
	want = &sqltypes.Result{}
	ctx = context.Background()
	tsv = newTestTabletServer(ctx, noFlags, db)
	txid = newTransaction(tsv, nil)
	qre = newTestQueryExecutor(ctx, tsv, query[0:strings.Index(query, " /*")], txid)
	defer testCommitHelper(t, tsv, qre)
	checkPlanID(t, planbuilder.PlanInsertPK, qre.plan.PlanID)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	wantqueries = []string{query}
	gotqueries = fetchRecordedQueries(qre)
	if !reflect.DeepEqual(gotqueries, wantqueries) {
		t.Errorf("queries: %v, want %v", gotqueries, wantqueries)
	}

	query = "insert into test_table(pk) values (1), (2), (3) on duplicate key update val = 5 /* _stream test_table (pk ) (1 ) (2 ) (3 ); */"
	db.AddQuery(query, &sqltypes.Result{})
	want = &sqltypes.Result{}
	ctx = context.Background()
	tsv = newTestTabletServer(ctx, noFlags, db)
	txid = newTransaction(tsv, nil)
	qre = newTestQueryExecutor(ctx, tsv, query[0:strings.Index(query, " /*")], txid)
	defer testCommitHelper(t, tsv, qre)
	checkPlanID(t, planbuilder.PlanInsertPK, qre.plan.PlanID)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	wantqueries = []string{query}
	gotqueries = fetchRecordedQueries(qre)
	if !reflect.DeepEqual(gotqueries, wantqueries) {
		t.Errorf("queries: %v, want %v", gotqueries, wantqueries)
	}
}

func TestQueryExecutorPlanUpsertPkRBR(t *testing.T) {
	// For UPSERT, the query just becomes a pass-through in RBR mode.
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "insert into test_table(pk) values (1) on duplicate key update val = 1"
	db.AddQuery(query, &sqltypes.Result{})
	want := &sqltypes.Result{}
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	txid := newTransaction(tsv, nil)
	qre := newTestQueryExecutor(ctx, tsv, query, txid)
	tsv.qe.binlogFormat = connpool.BinlogFormatRow
	defer tsv.StopService()
	defer testCommitHelper(t, tsv, qre)
	checkPlanID(t, planbuilder.PlanUpsertPK, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	wantqueries := []string{query}
	gotqueries := fetchRecordedQueries(qre)
	if !reflect.DeepEqual(gotqueries, wantqueries) {
		t.Errorf("queries: %v, want %v", gotqueries, wantqueries)
	}
}

func TestQueryExecutorPlanUpsertPkAutoCommit(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	db.AddQuery("insert into test_table(pk) values (1) /* _stream test_table (pk ) (1 ); */", &sqltypes.Result{})
	want := &sqltypes.Result{}
	query := "insert into test_table(pk) values(1) on duplicate key update val=1"
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanUpsertPK, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}

	db.AddRejectedQuery("insert into test_table(pk) values (1) /* _stream test_table (pk ) (1 ); */", errRejected)
	_, err = qre.Execute()
	wantErr := "rejected"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("qre.Execute() = %v, want %v", err, wantErr)
	}

	db.AddRejectedQuery(
		"insert into test_table(pk) values (1) /* _stream test_table (pk ) (1 ); */",
		mysql.NewSQLError(mysql.ERDupEntry, mysql.SSDupKey, "err"),
	)
	db.AddQuery("update test_table set val = 1 where pk in (1) /* _stream test_table (pk ) (1 ); */", &sqltypes.Result{})
	_, err = qre.Execute()
	wantErr = "err (errno 1062) (sqlstate 23000)"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("qre.Execute() = %v, want %v", err, wantErr)
	}

	db.AddRejectedQuery(
		"insert into test_table(pk) values (1) /* _stream test_table (pk ) (1 ); */",
		mysql.NewSQLError(mysql.ERDupEntry, mysql.SSDupKey, "ERROR 1062 (23000): Duplicate entry '2' for key 'PRIMARY'"),
	)
	db.AddQuery(
		"update test_table set val = 1 where pk in (1) /* _stream test_table (pk ) (1 ); */",
		&sqltypes.Result{RowsAffected: 1},
	)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	want = &sqltypes.Result{
		RowsAffected: 2,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanDmlPk(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	txid := newTransaction(tsv, nil)
	qre := newTestQueryExecutor(ctx, tsv, query, txid)
	defer tsv.StopService()
	defer testCommitHelper(t, tsv, qre)
	checkPlanID(t, planbuilder.PlanDMLPK, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	wantqueries := []string{"update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"}
	gotqueries := fetchRecordedQueries(qre)
	if !reflect.DeepEqual(gotqueries, wantqueries) {
		t.Errorf("queries: %v, want %v", gotqueries, wantqueries)
	}
}

func TestQueryExecutorPlanDmlPkTransactionIsolation(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	db.AddQuery("set transaction isolation level SERIALIZABLE", &sqltypes.Result{})
	txid := newTransaction(tsv, &querypb.ExecuteOptions{
		TransactionIsolation: querypb.ExecuteOptions_SERIALIZABLE,
	})
	qre := newTestQueryExecutor(ctx, tsv, query, txid)
	defer tsv.StopService()
	defer testCommitHelper(t, tsv, qre)
	checkPlanID(t, planbuilder.PlanDMLPK, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	wantqueries := []string{"update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"}
	gotqueries := fetchRecordedQueries(qre)
	if !reflect.DeepEqual(gotqueries, wantqueries) {
		t.Errorf("queries: %v, want %v", gotqueries, wantqueries)
	}
}

func TestQueryExecutorPlanDmlPkRBR(t *testing.T) {
	// RBR test is almost identical to the non-RBR test, except that
	// the _stream comments are suppressed for RBR.
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "update test_table set name = 2 where pk in (1)"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	txid := newTransaction(tsv, nil)
	qre := newTestQueryExecutor(ctx, tsv, query, txid)
	tsv.qe.binlogFormat = connpool.BinlogFormatRow
	defer tsv.StopService()
	defer testCommitHelper(t, tsv, qre)
	checkPlanID(t, planbuilder.PlanDMLPK, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	wantqueries := []string{query}
	gotqueries := fetchRecordedQueries(qre)
	if !reflect.DeepEqual(gotqueries, wantqueries) {
		t.Errorf("queries: %v, want %v", gotqueries, wantqueries)
	}
}

func TestQueryExecutorPlanDmlMessage(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "update msg set time_acked = 2, time_next = null where id in (1)"
	want := &sqltypes.Result{}
	db.AddQuery("select time_scheduled, id from msg where id in (1) limit 10001 for update", &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.Int64},
			{Type: sqltypes.Int64},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("12"),
			sqltypes.NewVarBinary("1"),
		}},
	})
	db.AddQuery("update msg set time_acked = 2, time_next = null where (time_scheduled = 12 and id = 1) /* _stream msg (time_scheduled id ) (12 1 ); */", want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	txid := newTransaction(tsv, nil)
	qre := newTestQueryExecutor(ctx, tsv, query, txid)
	defer tsv.StopService()
	defer testCommitHelper(t, tsv, qre)
	checkPlanID(t, planbuilder.PlanDMLSubquery, qre.plan.PlanID)
	_, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	conn, err := qre.tsv.te.txPool.Get(txid, "for test")
	if err != nil {
		t.Fatal(err)
	}
	wantChanged := map[string][]string{"msg": {"1"}}
	if !reflect.DeepEqual(conn.ChangedMessages, wantChanged) {
		t.Errorf("conn.ChangedMessages: %+v, want: %+v", conn.ChangedMessages, wantChanged)
	}
	conn.Recycle()
}

func TestQueryExecutorPlanDmlAutoCommit(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanDMLPK, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanDmlAutoCommitTransactionIsolation(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)

	qre.options = &querypb.ExecuteOptions{
		TransactionIsolation: querypb.ExecuteOptions_READ_UNCOMMITTED,
	}
	db.AddQuery("set transaction isolation level READ UNCOMMITTED", &sqltypes.Result{})

	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanDMLPK, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanDmlSubQuery(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "update test_table set addr = 3 where name = 1"
	expandedQuery := "select pk from test_table where name = 1 limit 10001 for update"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	db.AddQuery(expandedQuery, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.Int32},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewInt32(2)},
		},
	})
	updateQuery := "update test_table set addr = 3 where pk in (2) /* _stream test_table (pk ) (2 ); */"
	db.AddQuery(updateQuery, want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	txid := newTransaction(tsv, nil)
	qre := newTestQueryExecutor(ctx, tsv, query, txid)
	defer tsv.StopService()
	defer testCommitHelper(t, tsv, qre)
	checkPlanID(t, planbuilder.PlanDMLSubquery, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	wantqueries := []string{updateQuery}
	gotqueries := fetchRecordedQueries(qre)
	if !reflect.DeepEqual(gotqueries, wantqueries) {
		t.Errorf("queries: %v, want %v", gotqueries, wantqueries)
	}
}

func TestQueryExecutorPlanDmlSubQueryRBR(t *testing.T) {
	// RBR test is almost identical to the non-RBR test, except that
	// the _stream comments are suppressed for RBR.
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "update test_table set addr = 3 where name = 1"
	expandedQuery := "select pk from test_table where name = 1 limit 10001 for update"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	db.AddQuery(expandedQuery, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.Int32},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewInt32(2)},
		},
	})
	updateQuery := "update test_table set addr = 3 where pk in (2)"
	db.AddQuery(updateQuery, want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	txid := newTransaction(tsv, nil)
	qre := newTestQueryExecutor(ctx, tsv, query, txid)
	tsv.qe.binlogFormat = connpool.BinlogFormatRow
	defer tsv.StopService()
	defer testCommitHelper(t, tsv, qre)
	checkPlanID(t, planbuilder.PlanDMLSubquery, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	wantqueries := []string{updateQuery}
	gotqueries := fetchRecordedQueries(qre)
	if !reflect.DeepEqual(gotqueries, wantqueries) {
		t.Errorf("queries: %v, want %v", gotqueries, wantqueries)
	}
}

func TestQueryExecutorPlanDmlSubQueryAutoCommit(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "update test_table set addr = 3 where name = 1"
	expandedQuery := "select pk from test_table where name = 1 limit 10001 for update"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	db.AddQuery(expandedQuery, want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanDMLSubquery, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanOtherWithinATransaction(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "show test_table"
	want := &sqltypes.Result{
		Fields: getTestTableFields(),
	}
	db.AddQuery(query, want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	txid := newTransaction(tsv, nil)
	qre := newTestQueryExecutor(ctx, tsv, query, txid)
	defer tsv.StopService()
	defer testCommitHelper(t, tsv, qre)
	checkPlanID(t, planbuilder.PlanOtherRead, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	if gotqueries := fetchRecordedQueries(qre); gotqueries != nil {
		t.Errorf("queries: %v, want nil", gotqueries)
	}
}

func TestQueryExecutorPlanPassSelectWithInATransaction(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	fields := []*querypb.Field{
		{Name: "addr", Type: sqltypes.Int32},
	}
	query := "select addr from test_table where pk = 1 limit 1000"
	want := &sqltypes.Result{
		Fields:       fields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewInt32(123)},
		},
	}
	db.AddQuery(query, want)
	db.AddQuery("select addr from test_table where 1 != 1", &sqltypes.Result{
		Fields: fields,
	})
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	txid := newTransaction(tsv, nil)
	qre := newTestQueryExecutor(ctx, tsv, query, txid)
	defer tsv.StopService()
	defer testCommitHelper(t, tsv, qre)
	checkPlanID(t, planbuilder.PlanPassSelect, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	if gotqueries := fetchRecordedQueries(qre); gotqueries != nil {
		t.Errorf("queries: %v, want nil", gotqueries)
	}
}

func TestQueryExecutorPlanPassSelectWithLockOutsideATransaction(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "select * from test_table for update"
	want := &sqltypes.Result{
		Fields: getTestTableFields(),
		Rows:   [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	db.AddQuery("select * from test_table where 1 != 1", &sqltypes.Result{
		Fields: getTestTableFields(),
	})
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanSelectLock, qre.plan.PlanID)
	_, err := qre.Execute()
	if code := vterrors.Code(err); code != vtrpcpb.Code_FAILED_PRECONDITION {
		t.Fatalf("qre.Execute: %v, want %v", code, vtrpcpb.Code_FAILED_PRECONDITION)
	}
}

func TestQueryExecutorPlanPassSelect(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "select * from test_table limit 1000"
	want := &sqltypes.Result{
		Fields: getTestTableFields(),
	}
	db.AddQuery(query, want)
	db.AddQuery("select * from test_table where 1 != 1", &sqltypes.Result{
		Fields: getTestTableFields(),
	})
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanPassSelect, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanPassSelectSqlSelectLimit(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "select * from test_table"
	expandedQuery := "select * from test_table limit 20"
	want := &sqltypes.Result{
		Fields: getTestTableFields(),
	}
	db.AddQuery(query, want)
	db.AddQuery(expandedQuery, want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	qre.options = &querypb.ExecuteOptions{
		SqlSelectLimit: 20,
	}
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanPassSelect, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanSet(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	setQuery := "set unknown_key = 1"
	db.AddQuery(setQuery, &sqltypes.Result{})
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()
	qre := newTestQueryExecutor(ctx, tsv, setQuery, 0)
	checkPlanID(t, planbuilder.PlanSet, qre.plan.PlanID)
	// Query will be delegated to MySQL and both Fields and Rows should be
	// empty arrays in this case.
	want := &sqltypes.Result{}
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}

	// Test inside transaction.
	txid := newTransaction(tsv, nil)
	qre = newTestQueryExecutor(ctx, tsv, setQuery, txid)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
	wantqueries := []string{"set unknown_key = 1"}
	gotqueries := fetchRecordedQueries(qre)
	if !reflect.DeepEqual(gotqueries, wantqueries) {
		t.Errorf("queries: %v, want %v", gotqueries, wantqueries)
	}
	testCommitHelper(t, tsv, qre)
	tsv.StopService()
}

func TestQueryExecutorPlanOther(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "show test_table"
	want := &sqltypes.Result{
		Fields: getTestTableFields(),
	}
	db.AddQuery(query, want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanOtherRead, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanNextval(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	selQuery := "select next_id, cache from seq where id = 0 for update"
	db.AddQuery(selQuery, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.Int64},
			{Type: sqltypes.Int64},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NewInt64(3),
		}},
	})
	updateQuery := "update seq set next_id = 4 where id = 0"
	db.AddQuery(updateQuery, &sqltypes.Result{})
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()
	qre := newTestQueryExecutor(ctx, tsv, "select next value from seq", 0)
	checkPlanID(t, planbuilder.PlanNextval, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	want := &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "nextval",
			Type: sqltypes.Int64,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() =\n%#v, want:\n%#v", got, want)
	}

	// At this point, NextVal==2, LastVal==4.
	// So, a single value gen should not cause a db access.
	db.DeleteQuery(selQuery)
	qre = newTestQueryExecutor(ctx, tsv, "select next 1 values from seq", 0)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	want = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "nextval",
			Type: sqltypes.Int64,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(2),
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() =\n%#v, want:\n%#v", got, want)
	}

	// NextVal==3, LastVal==4
	// Let's try the next 2 values.
	db.AddQuery(selQuery, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.Int64},
			{Type: sqltypes.Int64},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(4),
			sqltypes.NewInt64(3),
		}},
	})
	updateQuery = "update seq set next_id = 7 where id = 0"
	db.AddQuery(updateQuery, &sqltypes.Result{})
	qre = newTestQueryExecutor(ctx, tsv, "select next 2 values from seq", 0)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	want = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "nextval",
			Type: sqltypes.Int64,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(3),
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() =\n%#v, want:\n%#v", got, want)
	}

	// NextVal==5, LastVal==7
	// Let's try jumping a full cache range.
	db.AddQuery(selQuery, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.Int64},
			{Type: sqltypes.Int64},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(7),
			sqltypes.NewInt64(3),
		}},
	})
	updateQuery = "update seq set next_id = 13 where id = 0"
	db.AddQuery(updateQuery, &sqltypes.Result{})
	qre = newTestQueryExecutor(ctx, tsv, "select next 6 values from seq", 0)
	got, err = qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	want = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "nextval",
			Type: sqltypes.Int64,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(5),
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() =\n%#v, want:\n%#v", got, want)
	}
}

func TestQueryExecutorMessageStream(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()
	logStats := tabletenv.NewLogStats(ctx, "TestQueryExecutor")

	plan, err := tsv.qe.GetMessageStreamPlan("msg")
	if err != nil {
		t.Fatal(err)
	}

	// We can't call newTestQueryExecutor because there's no
	// SQL syntax for message streaming yet.
	qre := &QueryExecutor{
		ctx:      ctx,
		query:    "stream from msg",
		plan:     plan,
		logStats: logStats,
		tsv:      tsv,
	}
	checkPlanID(t, planbuilder.PlanMessageStream, qre.plan.PlanID)

	ch := make(chan *sqltypes.Result, 1)
	done := make(chan struct{})
	skippedField := false
	go func() {
		if err := qre.MessageStream(func(qr *sqltypes.Result) error {
			// Skip first result (field info).
			if !skippedField {
				skippedField = true
				return nil
			}
			ch <- qr
			return io.EOF
		}); err != nil {
			t.Error(err)
		}
		close(done)
	}()

	newMessages := map[string][]*messager.MessageRow{
		"msg": {
			&messager.MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("1"), sqltypes.NULL}},
		},
	}
	// We hack-push a new message into the cache, which will cause
	// the message manager to send it.
	// We may have to iterate a few times before the stream kicks in.
	for {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
		unlock := tsv.messager.LockDB(newMessages, nil)
		tsv.messager.UpdateCaches(newMessages, nil)
		unlock()
		want := &sqltypes.Result{
			Rows: [][]sqltypes.Value{{
				sqltypes.NewVarBinary("1"),
				sqltypes.NULL,
			}},
		}
		select {
		case got := <-ch:
			if !want.Equal(got) {
				t.Errorf("Stream:\n%v, want\n%v", got, want)
			}
		default:
			continue
		}
		break
	}
	// This should not hang.
	<-done
}

func TestQueryExecutorMessageStreamACL(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int63())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group02",
			TableNamesOrPrefixes: []string{"msg"},
			Readers:              []string{"u2"},
			Writers:              []string{"u1"},
		}},
	}
	if err := tableacl.InitFromProto(config); err != nil {
		t.Fatalf("unable to load tableacl config, error: %v", err)
	}

	db := setUpQueryExecutorTest(t)
	defer db.Close()

	tsv := newTestTabletServer(context.Background(), enableStrictTableACL, db)
	defer tsv.StopService()

	plan, err := tsv.qe.GetMessageStreamPlan("msg")
	if err != nil {
		t.Fatal(err)
	}

	callerID := &querypb.VTGateCallerID{
		Username: "u1",
	}
	ctx := callerid.NewContext(context.Background(), nil, callerID)
	qre := &QueryExecutor{
		ctx:      ctx,
		query:    "stream from msg",
		plan:     plan,
		logStats: tabletenv.NewLogStats(ctx, "TestQueryExecutor"),
		tsv:      tsv,
	}

	// Should not fail because u1 has permission.
	err = qre.MessageStream(func(qr *sqltypes.Result) error {
		return io.EOF
	})
	if err != nil {
		t.Fatal(err)
	}

	callerID = &querypb.VTGateCallerID{
		Username: "u2",
	}
	qre.ctx = callerid.NewContext(context.Background(), nil, callerID)
	// Should fail because u2 does not have permission.
	err = qre.MessageStream(func(qr *sqltypes.Result) error {
		return io.EOF
	})

	want := `table acl error: "u2" [] cannot run MESSAGE_STREAM on table "msg"`
	if err == nil || err.Error() != want {
		t.Errorf("qre.MessageStream(msg) error: %v, want %s", err, want)
	}
	if code := vterrors.Code(err); code != vtrpcpb.Code_PERMISSION_DENIED {
		t.Fatalf("qre.Execute: %v, want %v", code, vtrpcpb.Code_PERMISSION_DENIED)
	}
}

func TestQueryExecutorTableAcl(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int63())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "select * from test_table limit 1000"
	want := &sqltypes.Result{
		Fields: getTestTableFields(),
	}
	db.AddQuery(query, want)
	db.AddQuery("select * from test_table where 1 != 1", &sqltypes.Result{
		Fields: getTestTableFields(),
	})

	username := "u2"
	callerID := &querypb.VTGateCallerID{
		Username: username,
	}
	ctx := callerid.NewContext(context.Background(), nil, callerID)
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group01",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{username},
		}},
	}
	if err := tableacl.InitFromProto(config); err != nil {
		t.Fatalf("unable to load tableacl config, error: %v", err)
	}

	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanPassSelect, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
}

func TestQueryExecutorTableAclNoPermission(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int63())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "select * from test_table limit 1000"
	want := &sqltypes.Result{
		Fields: getTestTableFields(),
	}
	db.AddQuery(query, want)
	db.AddQuery("select * from test_table where 1 != 1", &sqltypes.Result{
		Fields: getTestTableFields(),
	})

	username := "u2"
	callerID := &querypb.VTGateCallerID{
		Username: username,
	}
	ctx := callerid.NewContext(context.Background(), nil, callerID)
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group02",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{"superuser"},
		}},
	}

	if err := tableacl.InitFromProto(config); err != nil {
		t.Fatalf("unable to load tableacl config, error: %v", err)
	}
	// without enabling Config.StrictTableAcl
	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	checkPlanID(t, planbuilder.PlanPassSelect, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
	tsv.StopService()

	// enable Config.StrictTableAcl
	tsv = newTestTabletServer(ctx, enableStrictTableACL, db)
	qre = newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanPassSelect, qre.plan.PlanID)
	// query should fail because current user do not have read permissions
	_, err = qre.Execute()
	if err == nil {
		t.Fatal("got: nil, want: error")
	}
	if code := vterrors.Code(err); code != vtrpcpb.Code_PERMISSION_DENIED {
		t.Fatalf("qre.Execute: %v, want %v", code, vtrpcpb.Code_PERMISSION_DENIED)
	}
}

func TestQueryExecutorTableAclExemptACL(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int63())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "select * from test_table limit 1000"
	want := &sqltypes.Result{
		Fields:       getTestTableFields(),
		RowsAffected: 0,
		Rows:         [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	db.AddQuery("select * from test_table where 1 != 1", &sqltypes.Result{
		Fields: getTestTableFields(),
	})

	username := "u2"
	callerID := &querypb.VTGateCallerID{
		Username: username,
	}
	ctx := callerid.NewContext(context.Background(), nil, callerID)

	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group02",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{"u1"},
		}},
	}

	if err := tableacl.InitFromProto(config); err != nil {
		t.Fatalf("unable to load tableacl config, error: %v", err)
	}

	// enable Config.StrictTableAcl
	tsv := newTestTabletServer(ctx, enableStrictTableACL, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanPassSelect, qre.plan.PlanID)
	// query should fail because current user do not have read permissions
	_, err := qre.Execute()
	if code := vterrors.Code(err); code != vtrpcpb.Code_PERMISSION_DENIED {
		t.Fatalf("qre.Execute: %v, want %v", code, vtrpcpb.Code_PERMISSION_DENIED)
	}
	wanterr := "table acl error"
	if !strings.Contains(err.Error(), wanterr) {
		t.Fatalf("qre.Execute: %v, want %s", err, wanterr)
	}

	// table acl should be ignored since this is an exempt user.
	username = "exempt-acl"
	f, _ := tableacl.GetCurrentAclFactory()
	if tsv.qe.exemptACL, err = f.New([]string{username}); err != nil {
		t.Fatalf("Cannot load exempt ACL for Table ACL: %v", err)
	}
	callerID = &querypb.VTGateCallerID{
		Username: username,
	}
	ctx = callerid.NewContext(context.Background(), nil, callerID)

	qre = newTestQueryExecutor(ctx, tsv, query, 0)
	_, err = qre.Execute()
	if err != nil {
		t.Fatal("qre.Execute: nil, want: error")
	}
}

func TestQueryExecutorTableAclDryRun(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int63())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "select * from test_table limit 1000"
	want := &sqltypes.Result{
		Fields:       getTestTableFields(),
		RowsAffected: 0,
		Rows:         [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	db.AddQuery("select * from test_table where 1 != 1", &sqltypes.Result{
		Fields: getTestTableFields(),
	})

	username := "u2"
	callerID := &querypb.VTGateCallerID{
		Username: username,
	}
	ctx := callerid.NewContext(context.Background(), nil, callerID)

	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group02",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{"u1"},
		}},
	}

	if err := tableacl.InitFromProto(config); err != nil {
		t.Fatalf("unable to load tableacl config, error: %v", err)
	}

	tableACLStatsKey := strings.Join([]string{
		"test_table",
		"group02",
		planbuilder.PlanPassSelect.String(),
		username,
	}, ".")
	// enable Config.StrictTableAcl
	tsv := newTestTabletServer(ctx, enableStrictTableACL, db)
	tsv.qe.enableTableACLDryRun = true
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanPassSelect, qre.plan.PlanID)
	beforeCount := tabletenv.TableaclPseudoDenied.Counters.Counts()[tableACLStatsKey]
	// query should fail because current user do not have read permissions
	_, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want: nil", err)
	}
	afterCount := tabletenv.TableaclPseudoDenied.Counters.Counts()[tableACLStatsKey]
	if afterCount-beforeCount != 1 {
		t.Fatalf("table acl pseudo denied count should increase by one. got: %d, want: %d", afterCount, beforeCount+1)
	}
}

func TestQueryExecutorBlacklistQRFail(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "select * from test_table where name = 1 limit 1000"
	expandedQuery := "select pk from test_table use index (`index`) where name = 1 limit 1000"
	expected := &sqltypes.Result{
		Fields: getTestTableFields(),
	}
	db.AddQuery(query, expected)
	db.AddQuery(expandedQuery, expected)

	db.AddQuery("select * from test_table where 1 != 1", &sqltypes.Result{
		Fields: getTestTableFields(),
	})

	bannedAddr := "127.0.0.1"
	bannedUser := "u2"

	alterRule := rules.NewQueryRule("disable update", "disable update", rules.QRFail)
	alterRule.SetIPCond(bannedAddr)
	alterRule.SetUserCond(bannedUser)
	alterRule.SetQueryCond("select.*")
	alterRule.AddPlanCond(planbuilder.PlanPassSelect)
	alterRule.AddTableCond("test_table")

	rulesName := "blacklistedRulesQRFail"
	rules := rules.New()
	rules.Add(alterRule)

	callInfo := &fakecallinfo.FakeCallInfo{
		Remote: bannedAddr,
		User:   bannedUser,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)
	tsv := newTestTabletServer(ctx, noFlags, db)
	tsv.qe.queryRuleSources.UnRegisterSource(rulesName)
	tsv.qe.queryRuleSources.RegisterSource(rulesName)
	defer tsv.qe.queryRuleSources.UnRegisterSource(rulesName)

	if err := tsv.qe.queryRuleSources.SetRules(rulesName, rules); err != nil {
		t.Fatalf("failed to set rule, error: %v", err)
	}

	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()

	checkPlanID(t, planbuilder.PlanPassSelect, qre.plan.PlanID)
	// execute should fail because query has been blacklisted
	_, err := qre.Execute()
	if code := vterrors.Code(err); code != vtrpcpb.Code_INVALID_ARGUMENT {
		t.Fatalf("qre.Execute: %v, want %v", code, vtrpcpb.Code_INVALID_ARGUMENT)
	}
}

func TestQueryExecutorBlacklistQRRetry(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	query := "select * from test_table where name = 1 limit 1000"
	expandedQuery := "select pk from test_table use index (`index`) where name = 1 limit 1000"
	expected := &sqltypes.Result{
		Fields: getTestTableFields(),
	}
	db.AddQuery(query, expected)
	db.AddQuery(expandedQuery, expected)

	db.AddQuery("select * from test_table where 1 != 1", &sqltypes.Result{
		Fields: getTestTableFields(),
	})

	bannedAddr := "127.0.0.1"
	bannedUser := "x"

	alterRule := rules.NewQueryRule("disable update", "disable update", rules.QRFailRetry)
	alterRule.SetIPCond(bannedAddr)
	alterRule.SetUserCond(bannedUser)
	alterRule.SetQueryCond("select.*")
	alterRule.AddPlanCond(planbuilder.PlanPassSelect)
	alterRule.AddTableCond("test_table")

	rulesName := "blacklistedRulesQRRetry"
	rules := rules.New()
	rules.Add(alterRule)

	callInfo := &fakecallinfo.FakeCallInfo{
		Remote: bannedAddr,
		User:   bannedUser,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)
	tsv := newTestTabletServer(ctx, noFlags, db)
	tsv.qe.queryRuleSources.UnRegisterSource(rulesName)
	tsv.qe.queryRuleSources.RegisterSource(rulesName)
	defer tsv.qe.queryRuleSources.UnRegisterSource(rulesName)

	if err := tsv.qe.queryRuleSources.SetRules(rulesName, rules); err != nil {
		t.Fatalf("failed to set rule, error: %v", err)
	}

	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()

	checkPlanID(t, planbuilder.PlanPassSelect, qre.plan.PlanID)
	_, err := qre.Execute()
	if code := vterrors.Code(err); code != vtrpcpb.Code_FAILED_PRECONDITION {
		t.Fatalf("tsv.qe.queryRuleSources.SetRules: %v, want %v", code, vtrpcpb.Code_FAILED_PRECONDITION)
	}
}

type executorFlags int64

const (
	noFlags              executorFlags = 0
	enableStrictTableACL               = 1 << iota
	smallTxPool
	noTwopc
	shortTwopcAge
)

// newTestQueryExecutor uses a package level variable testTabletServer defined in tabletserver_test.go
func newTestTabletServer(ctx context.Context, flags executorFlags, db *fakesqldb.DB) *TabletServer {
	randID := rand.Int63()
	config := tabletenv.DefaultQsConfig
	config.PoolNamePrefix = fmt.Sprintf("Pool-%d-", randID)
	config.PoolSize = 100
	if flags&smallTxPool > 0 {
		config.TransactionCap = 3
	} else {
		config.TransactionCap = 100
	}
	config.EnableAutoCommit = true
	if flags&enableStrictTableACL > 0 {
		config.StrictTableACL = true
	} else {
		config.StrictTableACL = false
	}
	if flags&noTwopc > 0 {
		config.TwoPCEnable = false
	} else {
		config.TwoPCEnable = true
	}
	config.TwoPCCoordinatorAddress = "fake"
	if flags&shortTwopcAge > 0 {
		config.TwoPCAbandonAge = 0.5
	} else {
		config.TwoPCAbandonAge = 10
	}
	tsv := NewTabletServerWithNilTopoServer(config)
	testUtils := newTestUtils()
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	return tsv
}

func newTransaction(tsv *TabletServer, options *querypb.ExecuteOptions) int64 {
	transactionID, err := tsv.Begin(context.Background(), &tsv.target, options)
	if err != nil {
		panic(fmt.Errorf("failed to start a transaction: %v", err))
	}
	return transactionID
}

func newTestQueryExecutor(ctx context.Context, tsv *TabletServer, sql string, txID int64) *QueryExecutor {
	logStats := tabletenv.NewLogStats(ctx, "TestQueryExecutor")
	plan, err := tsv.qe.GetPlan(ctx, logStats, sql)
	if err != nil {
		panic(err)
	}
	return &QueryExecutor{
		ctx:           ctx,
		query:         sql,
		bindVars:      make(map[string]*querypb.BindVariable),
		transactionID: txID,
		plan:          plan,
		logStats:      logStats,
		tsv:           tsv,
	}
}

func testCommitHelper(t *testing.T, tsv *TabletServer, queryExecutor *QueryExecutor) {
	if err := tsv.Commit(queryExecutor.ctx, &tsv.target, queryExecutor.transactionID); err != nil {
		t.Fatalf("failed to commit transaction: %d, err: %v", queryExecutor.transactionID, err)
	}
}

func setUpQueryExecutorTest(t *testing.T) *fakesqldb.DB {
	db := fakesqldb.New(t)
	initQueryExecutorTestDB(db, true)
	return db
}

func setUpQueryExecutorTestWithOneUniqueKey(t *testing.T) *fakesqldb.DB {
	db := fakesqldb.New(t)
	initQueryExecutorTestDB(db, false)
	return db
}

func initQueryExecutorTestDB(db *fakesqldb.DB, testTableHasMultipleUniqueKeys bool) {
	for query, result := range getQueryExecutorSupportedQueries(testTableHasMultipleUniqueKeys) {
		db.AddQuery(query, result)
	}
}

func fetchRecordedQueries(qre *QueryExecutor) []string {
	conn, err := qre.tsv.te.txPool.Get(qre.transactionID, "for query")
	if err != nil {
		panic(err)
	}
	defer conn.Recycle()
	return conn.Queries
}

func getTestTableFields() []*querypb.Field {
	return []*querypb.Field{
		{Name: "pk", Type: sqltypes.Int32},
		{Name: "name", Type: sqltypes.Int32},
		{Name: "addr", Type: sqltypes.Int32},
	}
}

func checkPlanID(
	t *testing.T,
	expectedPlanID planbuilder.PlanType,
	actualPlanID planbuilder.PlanType) {
	if expectedPlanID != actualPlanID {
		t.Fatalf("expect to get PlanID: %s, but got %s",
			expectedPlanID.String(), actualPlanID.String())
	}
}

func getQueryExecutorSupportedQueries(testTableHasMultipleUniqueKeys bool) map[string]*sqltypes.Result {
	return map[string]*sqltypes.Result{
		// queries for twopc
		sqlTurnoffBinlog:                                  {},
		fmt.Sprintf(sqlCreateSidecarDB, "`_vt`"):          {},
		fmt.Sprintf(sqlDropLegacy1, "`_vt`"):              {},
		fmt.Sprintf(sqlDropLegacy2, "`_vt`"):              {},
		fmt.Sprintf(sqlDropLegacy3, "`_vt`"):              {},
		fmt.Sprintf(sqlDropLegacy4, "`_vt`"):              {},
		fmt.Sprintf(sqlCreateTableRedoState, "`_vt`"):     {},
		fmt.Sprintf(sqlCreateTableRedoStatement, "`_vt`"): {},
		fmt.Sprintf(sqlCreateTableDTState, "`_vt`"):       {},
		fmt.Sprintf(sqlCreateTableDTParticipant, "`_vt`"): {},
		// queries for schema info
		"select unix_timestamp()": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.NewInt32(1427325875)},
			},
		},
		"select @@global.sql_mode": {
			Fields: []*querypb.Field{{
				Type: sqltypes.VarChar,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarBinary("STRICT_TRANS_TABLES")},
			},
		},
		"select @@autocommit": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarBinary("1")},
			},
		},
		"show variables like 'binlog_format'": {
			Fields: []*querypb.Field{{
				Type: sqltypes.VarChar,
			}, {
				Type: sqltypes.VarChar,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewVarBinary("binlog_format"),
				sqltypes.NewVarBinary("STATEMENT"),
			}},
		},
		mysql.BaseShowTables: {
			Fields:       mysql.BaseShowTablesFields,
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				mysql.BaseShowTablesRow("test_table", false, ""),
				mysql.BaseShowTablesRow("seq", false, "vitess_sequence"),
				mysql.BaseShowTablesRow("msg", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
			},
		},
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
		"describe test_table": {
			Fields:       mysql.DescribeTableFields,
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				mysql.DescribeTableRow("pk", "int(11)", false, "PRI", "0"),
				mysql.DescribeTableRow("name", "int(11)", false, "", "0"),
				mysql.DescribeTableRow("addr", "int(11)", false, "", "0"),
			},
		},
		// for SplitQuery because it needs a primary key column
		"show index from test_table": {
			Fields:       mysql.ShowIndexFromTableFields,
			RowsAffected: 2,
			Rows: [][]sqltypes.Value{
				mysql.ShowIndexFromTableRow("test_table", true, "PRIMARY", 1, "pk", false),
				mysql.ShowIndexFromTableRow("test_table", testTableHasMultipleUniqueKeys, "index", 1, "name", true),
			},
		},
		"begin":  {},
		"commit": {},
		mysql.BaseShowTablesForTable("test_table"): {
			Fields:       mysql.BaseShowTablesFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.BaseShowTablesRow("test_table", false, ""),
			},
		},
		"rollback": {},
		"select * from seq where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "id",
				Type: sqltypes.Int32,
			}, {
				Name: "next_id",
				Type: sqltypes.Int64,
			}, {
				Name: "cache",
				Type: sqltypes.Int64,
			}, {
				Name: "increment",
				Type: sqltypes.Int64,
			}},
		},
		"describe seq": {
			Fields:       mysql.DescribeTableFields,
			RowsAffected: 4,
			Rows: [][]sqltypes.Value{
				mysql.DescribeTableRow("id", "int(11)", false, "PRI", "0"),
				mysql.DescribeTableRow("next_id", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("cache", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("increment", "bigint(20)", false, "", "0"),
			},
		},
		"show index from seq": {
			Fields:       mysql.ShowIndexFromTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.ShowIndexFromTableRow("seq", true, "PRIMARY", 1, "id", false),
			},
		},
		mysql.BaseShowTablesForTable("seq"): {
			Fields:       mysql.BaseShowTablesFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.BaseShowTablesRow("seq", false, "vitess_sequence"),
			},
		},
		"select * from msg where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "time_scheduled",
				Type: sqltypes.Int32,
			}, {
				Name: "id",
				Type: sqltypes.Int64,
			}, {
				Name: "time_next",
				Type: sqltypes.Int64,
			}, {
				Name: "epoch",
				Type: sqltypes.Int64,
			}, {
				Name: "time_created",
				Type: sqltypes.Int64,
			}, {
				Name: "time_acked",
				Type: sqltypes.Int64,
			}, {
				Name: "message",
				Type: sqltypes.Int64,
			}},
		},
		"describe msg": {
			Fields:       mysql.DescribeTableFields,
			RowsAffected: 7,
			Rows: [][]sqltypes.Value{
				mysql.DescribeTableRow("time_scheduled", "int(11)", false, "PRI", "0"),
				mysql.DescribeTableRow("id", "bigint(20)", false, "PRI", "0"),
				mysql.DescribeTableRow("time_next", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("epoch", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("time_created", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("time_acked", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("message", "bigint(20)", false, "", "0"),
			},
		},
		"show index from msg": {
			Fields:       mysql.ShowIndexFromTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.ShowIndexFromTableRow("msg", true, "PRIMARY", 1, "time_scheduled", false),
				mysql.ShowIndexFromTableRow("msg", true, "PRIMARY", 2, "id", false),
			},
		},
		mysql.BaseShowTablesForTable("msg"): {
			Fields:       mysql.BaseShowTablesFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.BaseShowTablesRow("test_table", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
			},
		},
		fmt.Sprintf(sqlReadAllRedo, "`_vt`", "`_vt`"): {},
	}
}
