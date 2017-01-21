// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/tableacl"
	"github.com/youtube/vitess/go/vt/tableacl/simpleacl"
	"github.com/youtube/vitess/go/vt/tabletserver/planbuilder"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	tableaclpb "github.com/youtube/vitess/go/vt/proto/tableacl"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestQueryExecutorPlanDDL(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "alter table test_table add zipcode int"
	want := &sqltypes.Result{
		Rows: [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, enableStrict, db)
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

func TestQueryExecutorPlanPassDmlStrictMode(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "update test_table set pk = foo()"
	want := &sqltypes.Result{
		Rows: [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	ctx := context.Background()
	// non strict mode
	tsv := newTestTabletServer(ctx, noFlags, db)
	txid := newTransaction(tsv)
	qre := newTestQueryExecutor(ctx, tsv, query, txid)
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
	testCommitHelper(t, tsv, qre)
	tsv.StopService()

	// strict mode
	tsv = newTestTabletServer(ctx, enableStrict, db)
	qre = newTestQueryExecutor(ctx, tsv, query, newTransaction(tsv))
	defer tsv.StopService()
	defer testCommitHelper(t, tsv, qre)
	checkPlanID(t, planbuilder.PlanPassDML, qre.plan.PlanID)
	got, err = qre.Execute()
	if err == nil {
		t.Fatal("qre.Execute() = nil, want error")
	}
	tabletError, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("got: %v, want: a TabletError", tabletError)
	}
	if tabletError.ErrorCode != vtrpcpb.ErrorCode_BAD_INPUT {
		t.Fatalf("got: %s, want: BAD_INPUT", tabletError.ErrorCode)
	}
}

func TestQueryExecutorPlanPassDmlStrictModeAutoCommit(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "update test_table set pk = foo()"
	want := &sqltypes.Result{
		Rows: [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	// non strict mode
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	checkPlanID(t, planbuilder.PlanPassDML, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	tsv.StopService()

	// strict mode
	// update should fail because strict mode is not enabled
	tsv = newTestTabletServer(ctx, enableStrict, db)
	qre = newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanPassDML, qre.plan.PlanID)
	_, err = qre.Execute()
	if err == nil {
		t.Fatal("got: nil, want: error")
	}
	tabletError, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("got: %v, want: *TabletError", tabletError)
	}
	if tabletError.ErrorCode != vtrpcpb.ErrorCode_BAD_INPUT {
		t.Fatalf("got: %s, want: BAD_INPUT", tabletError.ErrorCode)
	}
}

func TestQueryExecutorPlanInsertPk(t *testing.T) {
	db := setUpQueryExecutorTest()
	db.AddQuery("insert into test_table values (1) /* _stream test_table (pk ) (1 ); */", &sqltypes.Result{})
	want := &sqltypes.Result{
		Rows: make([][]sqltypes.Value, 0),
	}
	query := "insert into test_table values(1)"
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, enableStrict, db)
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
	db := setUpQueryExecutorTest()
	db.AddQueryPattern("insert into msg\\(time_scheduled, id, message, time_next, time_created, epoch\\) values \\(1, 2, 3, 1,.*", &sqltypes.Result{})
	db.AddQuery(
		"select time_next, epoch, id, message from msg where (time_scheduled = 1 and id = 2)",
		&sqltypes.Result{
			Rows: [][]sqltypes.Value{{
				sqltypes.MakeString([]byte("1")),
				sqltypes.MakeString([]byte("0")),
				sqltypes.MakeString([]byte("1")),
				sqltypes.MakeString([]byte("01")),
			}},
		},
	)
	want := &sqltypes.Result{
		Rows: make([][]sqltypes.Value, 0),
	}
	query := "insert into msg(time_scheduled, id, message) values(1, 2, 3)"
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, enableStrict, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanInsertMessage, qre.plan.PlanID)
	r1 := newTestReceiver(1)
	tsv.messager.schemaChanged(map[string]*TableInfo{
		"msg": {
			Table: &schema.Table{
				Type: schema.Message,
			},
		},
	})
	tsv.messager.Subscribe("msg", r1.rcv)
	<-r1.ch
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	mr := <-r1.ch
	wantqr := &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("01")),
		}},
	}
	if !reflect.DeepEqual(mr, wantqr) {
		t.Errorf("rows:\n%+v, want\n%+v", got, wantqr)
	}

	txid := newTransaction(tsv)
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

func TestQueryExecutorPlanInsertSubQueryAutoCommmit(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "insert into test_table(pk) select pk from test_table where pk = 1 limit 1000"
	want := &sqltypes.Result{
		Rows: [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	selectQuery := "select pk from test_table where pk = 1 limit 1000"
	db.AddQuery(selectQuery, &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.MakeTrusted(sqltypes.Int32, []byte("2"))},
		},
	})

	insertQuery := "insert into test_table(pk) values (2) /* _stream test_table (pk ) (2 ); */"

	db.AddQuery(insertQuery, &sqltypes.Result{})
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, enableStrict, db)
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
	db := setUpQueryExecutorTest()
	query := "insert into test_table(pk) select pk from test_table where pk = 1 limit 1000"
	want := &sqltypes.Result{
		Rows: [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	selectQuery := "select pk from test_table where pk = 1 limit 1000"
	db.AddQuery(selectQuery, &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.MakeTrusted(sqltypes.Int32, []byte("2"))},
		},
	})

	insertQuery := "insert into test_table(pk) values (2) /* _stream test_table (pk ) (2 ); */"

	db.AddQuery(insertQuery, &sqltypes.Result{})
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, enableStrict, db)
	txid := newTransaction(tsv)
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

func TestQueryExecutorPlanUpsertPk(t *testing.T) {
	db := setUpQueryExecutorTest()
	db.AddQuery("insert into test_table values (1) /* _stream test_table (pk ) (1 ); */", &sqltypes.Result{})
	want := &sqltypes.Result{
		Rows: make([][]sqltypes.Value, 0),
	}
	query := "insert into test_table values(1) on duplicate key update val=1"
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, enableStrict, db)
	txid := newTransaction(tsv)
	qre := newTestQueryExecutor(ctx, tsv, query, txid)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanUpsertPK, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want: %v", got, want)
	}
	wantqueries := []string{"insert into test_table values (1) /* _stream test_table (pk ) (1 ); */"}
	gotqueries := fetchRecordedQueries(qre)
	if !reflect.DeepEqual(gotqueries, wantqueries) {
		t.Errorf("queries: %v, want %v", gotqueries, wantqueries)
	}
	testCommitHelper(t, tsv, qre)

	db.AddRejectedQuery("insert into test_table values (1) /* _stream test_table (pk ) (1 ); */", errRejected)
	txid = newTransaction(tsv)
	qre = newTestQueryExecutor(ctx, tsv, query, txid)
	_, err = qre.Execute()
	wantErr := "error: rejected"
	if err == nil || err.Error() != wantErr {
		t.Errorf("qre.Execute() = %v, want %v", err, wantErr)
	}
	if gotqueries = fetchRecordedQueries(qre); gotqueries != nil {
		t.Errorf("queries: %v, want nil", gotqueries)
	}
	testCommitHelper(t, tsv, qre)

	db.AddRejectedQuery(
		"insert into test_table values (1) /* _stream test_table (pk ) (1 ); */",
		sqldb.NewSQLError(mysql.ErrDupEntry, "23000", "err"),
	)
	db.AddQuery("update test_table set val = 1 where pk in (1) /* _stream test_table (pk ) (1 ); */", &sqltypes.Result{})
	txid = newTransaction(tsv)
	qre = newTestQueryExecutor(ctx, tsv, query, txid)
	_, err = qre.Execute()
	wantErr = "error: err (errno 1062) (sqlstate 23000)"
	if err == nil || err.Error() != wantErr {
		t.Errorf("qre.Execute() = %v, want %v", err, wantErr)
	}
	wantqueries = []string{}
	if gotqueries = fetchRecordedQueries(qre); gotqueries != nil {
		t.Errorf("queries: %v, want nil", gotqueries)
	}
	testCommitHelper(t, tsv, qre)

	db.AddRejectedQuery(
		"insert into test_table values (1) /* _stream test_table (pk ) (1 ); */",
		sqldb.NewSQLError(mysql.ErrDupEntry, "23000", "ERROR 1062 (23000): Duplicate entry '2' for key 'PRIMARY'"),
	)
	db.AddQuery(
		"update test_table set val = 1 where pk in (1) /* _stream test_table (pk ) (1 ); */",
		&sqltypes.Result{RowsAffected: 1},
	)
	txid = newTransaction(tsv)
	qre = newTestQueryExecutor(ctx, tsv, query, txid)
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
	testCommitHelper(t, tsv, qre)
}

func TestQueryExecutorPlanUpsertPkAutoCommit(t *testing.T) {
	db := setUpQueryExecutorTest()
	db.AddQuery("insert into test_table values (1) /* _stream test_table (pk ) (1 ); */", &sqltypes.Result{})
	want := &sqltypes.Result{
		Rows: make([][]sqltypes.Value, 0),
	}
	query := "insert into test_table values(1) on duplicate key update val=1"
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, enableStrict, db)
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

	db.AddRejectedQuery("insert into test_table values (1) /* _stream test_table (pk ) (1 ); */", errRejected)
	_, err = qre.Execute()
	wantErr := "error: rejected"
	if err == nil || err.Error() != wantErr {
		t.Fatalf("qre.Execute() = %v, want %v", err, wantErr)
	}

	db.AddRejectedQuery(
		"insert into test_table values (1) /* _stream test_table (pk ) (1 ); */",
		sqldb.NewSQLError(mysql.ErrDupEntry, "23000", "err"),
	)
	db.AddQuery("update test_table set val = 1 where pk in (1) /* _stream test_table (pk ) (1 ); */", &sqltypes.Result{})
	_, err = qre.Execute()
	wantErr = "error: err (errno 1062) (sqlstate 23000)"
	if err == nil || err.Error() != wantErr {
		t.Fatalf("qre.Execute() = %v, want %v", err, wantErr)
	}

	db.AddRejectedQuery(
		"insert into test_table values (1) /* _stream test_table (pk ) (1 ); */",
		sqldb.NewSQLError(mysql.ErrDupEntry, "23000", "ERROR 1062 (23000): Duplicate entry '2' for key 'PRIMARY'"),
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
	db := setUpQueryExecutorTest()
	query := "update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, enableStrict, db)
	txid := newTransaction(tsv)
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

func TestQueryExecutorPlanDmlMessage(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "update msg set time_acked = 2, time_next = null where id in (1)"
	want := &sqltypes.Result{}
	db.AddQuery("select time_scheduled, id from msg where id in (1) limit 10001 for update", &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("12")),
			sqltypes.MakeString([]byte("1")),
		}},
	})
	db.AddQuery("update msg set time_acked = 2, time_next = null where (time_scheduled = '12' and id = '1') /* _stream msg (time_scheduled id ) ('MTI=' 'MQ==' ); */", want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, enableStrict, db)
	txid := newTransaction(tsv)
	qre := newTestQueryExecutor(ctx, tsv, query, txid)
	defer tsv.StopService()
	defer testCommitHelper(t, tsv, qre)
	checkPlanID(t, planbuilder.PlanDMLSubquery, qre.plan.PlanID)
	_, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	conn, err := qre.te.txPool.Get(txid, "for test")
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
	db := setUpQueryExecutorTest()
	query := "update test_table set name = 2 where pk in (1) /* _stream test_table (pk ) (1 ); */"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, enableStrict, db)
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

func TestQueryExecutorPlanDmlSubQuery(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "update test_table set addr = 3 where name = 1 limit 1000"
	expandedQuery := "select pk from test_table where name = 1 limit 1000 for update"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	db.AddQuery(expandedQuery, &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.MakeTrusted(sqltypes.Int32, []byte("2"))},
		},
	})
	updateQuery := "update test_table set addr = 3 where pk in (2) /* _stream test_table (pk ) (2 ); */"
	db.AddQuery(updateQuery, want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, enableStrict, db)
	txid := newTransaction(tsv)
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

func TestQueryExecutorPlanDmlSubQueryAutoCommit(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "update test_table set addr = 3 where name = 1 limit 1000"
	expandedQuery := "select pk from test_table where name = 1 limit 1000 for update"
	want := &sqltypes.Result{}
	db.AddQuery(query, want)
	db.AddQuery(expandedQuery, want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, enableStrict, db)
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
	db := setUpQueryExecutorTest()
	query := "show test_table"
	want := &sqltypes.Result{
		Fields:       getTestTableFields(),
		RowsAffected: 0,
		Rows:         [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, enableStrict, db)
	txid := newTransaction(tsv)
	qre := newTestQueryExecutor(ctx, tsv, query, txid)
	defer tsv.StopService()
	defer testCommitHelper(t, tsv, qre)
	checkPlanID(t, planbuilder.PlanOther, qre.plan.PlanID)
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
	db := setUpQueryExecutorTest()
	fields := []*querypb.Field{
		{Name: "addr", Type: sqltypes.Int32},
	}
	query := "select addr from test_table where pk = 1 limit 1000"
	want := &sqltypes.Result{
		Fields:       fields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.MakeString([]byte("123"))},
		},
	}
	db.AddQuery(query, want)
	db.AddQuery("select addr from test_table where 1 != 1", &sqltypes.Result{
		Fields: fields,
	})
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, enableStrict, db)
	txid := newTransaction(tsv)
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
	db := setUpQueryExecutorTest()
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
	tsv := newTestTabletServer(ctx, enableStrict, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanSelectLock, qre.plan.PlanID)
	_, err := qre.Execute()
	if err == nil {
		t.Fatal("got: nil, want: error")
	}
	got, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("got: %v, want: *TabletError", err)
	}
	if got.ErrorCode != vtrpcpb.ErrorCode_BAD_INPUT {
		t.Fatalf("got: %s, want: BAD_INPUT", got.ErrorCode)
	}
}

func TestQueryExecutorPlanPassSelect(t *testing.T) {
	db := setUpQueryExecutorTest()
	query := "select * from test_table limit 1000"
	want := &sqltypes.Result{
		Fields: getTestTableFields(),
		Rows:   [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	db.AddQuery("select * from test_table where 1 != 1", &sqltypes.Result{
		Fields: getTestTableFields(),
	})
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, enableStrict, db)
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

func TestQueryExecutorPlanSet(t *testing.T) {
	db := setUpQueryExecutorTest()
	setQuery := "set unknown_key = 1"
	db.AddQuery(setQuery, &sqltypes.Result{})
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, enableStrict, db)
	defer tsv.StopService()
	qre := newTestQueryExecutor(ctx, tsv, setQuery, 0)
	checkPlanID(t, planbuilder.PlanSet, qre.plan.PlanID)
	// Query will be delegated to MySQL and both Fields and Rows should be
	// empty arrays in this case.
	want := &sqltypes.Result{
		Rows: make([][]sqltypes.Value, 0),
	}
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}

	// Test inside transaction.
	txid := newTransaction(tsv)
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
	db := setUpQueryExecutorTest()
	query := "show test_table"
	want := &sqltypes.Result{
		Fields:       getTestTableFields(),
		RowsAffected: 0,
		Rows:         [][]sqltypes.Value{},
	}
	db.AddQuery(query, want)
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, enableStrict, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanOther, qre.plan.PlanID)
	got, err := qre.Execute()
	if err != nil {
		t.Fatalf("got: %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() = %v, want: %v", got, want)
	}
}

func TestQueryExecutorPlanNextval(t *testing.T) {
	db := setUpQueryExecutorTest()
	selQuery := "select next_id, cache from seq where id = 0 for update"
	db.AddQuery(selQuery, &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeTrusted(sqltypes.Int64, []byte("1")),
			sqltypes.MakeTrusted(sqltypes.Int64, []byte("3")),
		}},
	})
	updateQuery := "update seq set next_id = 4 where id = 0"
	db.AddQuery(updateQuery, &sqltypes.Result{})
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, enableStrict, db)
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
			sqltypes.MakeTrusted(sqltypes.Int64, []byte("1")),
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
			sqltypes.MakeTrusted(sqltypes.Int64, []byte("2")),
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() =\n%#v, want:\n%#v", got, want)
	}

	// NextVal==3, LastVal==4
	// Let's try the next 2 values.
	db.AddQuery(selQuery, &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeTrusted(sqltypes.Int64, []byte("4")),
			sqltypes.MakeTrusted(sqltypes.Int64, []byte("3")),
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
			sqltypes.MakeTrusted(sqltypes.Int64, []byte("3")),
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() =\n%#v, want:\n%#v", got, want)
	}

	// NextVal==5, LastVal==7
	// Let's try jumping a full cache range.
	db.AddQuery(selQuery, &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeTrusted(sqltypes.Int64, []byte("7")),
			sqltypes.MakeTrusted(sqltypes.Int64, []byte("3")),
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
			sqltypes.MakeTrusted(sqltypes.Int64, []byte("5")),
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qre.Execute() =\n%#v, want:\n%#v", got, want)
	}
}

func TestQueryExecutorTableAcl(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int63())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	db := setUpQueryExecutorTest()
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
			Name:                 "group01",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{username},
		}},
	}
	if err := tableacl.InitFromProto(config); err != nil {
		t.Fatalf("unable to load tableacl config, error: %v", err)
	}

	tsv := newTestTabletServer(ctx, enableStrict, db)
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
	db := setUpQueryExecutorTest()
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
			Readers:              []string{"superuser"},
		}},
	}

	if err := tableacl.InitFromProto(config); err != nil {
		t.Fatalf("unable to load tableacl config, error: %v", err)
	}
	// without enabling Config.StrictTableAcl
	tsv := newTestTabletServer(ctx, enableStrict, db)
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
	tsv = newTestTabletServer(ctx, enableStrict|enableStrictTableACL, db)
	qre = newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanPassSelect, qre.plan.PlanID)
	// query should fail because current user do not have read permissions
	_, err = qre.Execute()
	if err == nil {
		t.Fatal("got: nil, want: error")
	}
	tabletError, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("got: %v, want: *TabletError", err)
	}
	if tabletError.ErrorCode != vtrpcpb.ErrorCode_PERMISSION_DENIED {
		t.Fatalf("got: %s, want: PERMISSION_DENIED", tabletError.ErrorCode)
	}
}

func TestQueryExecutorTableAclExemptACL(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-test-%d", rand.Int63())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	db := setUpQueryExecutorTest()
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
	tsv := newTestTabletServer(ctx, enableStrict|enableStrictTableACL, db)
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanPassSelect, qre.plan.PlanID)
	// query should fail because current user do not have read permissions
	_, err := qre.Execute()
	if err == nil {
		t.Fatal("got: nil, want: error")
	}
	tabletError, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("got: %v, want: *TabletError", err)
	}
	if tabletError.ErrorCode != vtrpcpb.ErrorCode_PERMISSION_DENIED {
		t.Fatalf("got: %s, want: PERMISSION_DENIED", tabletError.ErrorCode)
	}
	if !strings.Contains(tabletError.Error(), "table acl error") {
		t.Fatalf("got %s, want tablet errorL table acl error", tabletError.Error())
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
	db := setUpQueryExecutorTest()
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
	tsv := newTestTabletServer(ctx, enableStrict|enableStrictTableACL, db)
	tsv.qe.enableTableAclDryRun = true
	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()
	checkPlanID(t, planbuilder.PlanPassSelect, qre.plan.PlanID)
	beforeCount := tsv.qe.tableaclPseudoDenied.Counters.Counts()[tableACLStatsKey]
	// query should fail because current user do not have read permissions
	_, err := qre.Execute()
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want: nil", err)
	}
	afterCount := tsv.qe.tableaclPseudoDenied.Counters.Counts()[tableACLStatsKey]
	if afterCount-beforeCount != 1 {
		t.Fatalf("table acl pseudo denied count should increase by one. got: %d, want: %d", afterCount, beforeCount+1)
	}
}

func TestQueryExecutorBlacklistQRFail(t *testing.T) {
	db := setUpQueryExecutorTest()
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

	alterRule := NewQueryRule("disable update", "disable update", QRFail)
	alterRule.SetIPCond(bannedAddr)
	alterRule.SetUserCond(bannedUser)
	alterRule.SetQueryCond("select.*")
	alterRule.AddPlanCond(planbuilder.PlanPassSelect)
	alterRule.AddTableCond("test_table")

	rulesName := "blacklistedRulesQRFail"
	rules := NewQueryRules()
	rules.Add(alterRule)

	callInfo := &fakeCallInfo{
		remoteAddr: bannedAddr,
		username:   bannedUser,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)
	tsv := newTestTabletServer(ctx, enableStrict, db)
	tsv.qe.schemaInfo.queryRuleSources.UnRegisterQueryRuleSource(rulesName)
	tsv.qe.schemaInfo.queryRuleSources.RegisterQueryRuleSource(rulesName)
	defer tsv.qe.schemaInfo.queryRuleSources.UnRegisterQueryRuleSource(rulesName)

	if err := tsv.qe.schemaInfo.queryRuleSources.SetRules(rulesName, rules); err != nil {
		t.Fatalf("failed to set rule, error: %v", err)
	}

	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()

	checkPlanID(t, planbuilder.PlanPassSelect, qre.plan.PlanID)
	// execute should fail because query has been blacklisted
	_, err := qre.Execute()
	if err == nil {
		t.Fatal("got: nil, want: error")
	}
	got, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("got: %v, want: *TabletError", err)
	}
	if got.ErrorCode != vtrpcpb.ErrorCode_BAD_INPUT {
		t.Fatalf("got: %s, want: BAD_INPUT", got.ErrorCode)
	}
}

func TestQueryExecutorBlacklistQRRetry(t *testing.T) {
	db := setUpQueryExecutorTest()
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

	alterRule := NewQueryRule("disable update", "disable update", QRFailRetry)
	alterRule.SetIPCond(bannedAddr)
	alterRule.SetUserCond(bannedUser)
	alterRule.SetQueryCond("select.*")
	alterRule.AddPlanCond(planbuilder.PlanPassSelect)
	alterRule.AddTableCond("test_table")

	rulesName := "blacklistedRulesQRRetry"
	rules := NewQueryRules()
	rules.Add(alterRule)

	callInfo := &fakeCallInfo{
		remoteAddr: bannedAddr,
		username:   bannedUser,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)
	tsv := newTestTabletServer(ctx, enableStrict, db)
	tsv.qe.schemaInfo.queryRuleSources.UnRegisterQueryRuleSource(rulesName)
	tsv.qe.schemaInfo.queryRuleSources.RegisterQueryRuleSource(rulesName)
	defer tsv.qe.schemaInfo.queryRuleSources.UnRegisterQueryRuleSource(rulesName)

	if err := tsv.qe.schemaInfo.queryRuleSources.SetRules(rulesName, rules); err != nil {
		t.Fatalf("failed to set rule, error: %v", err)
	}

	qre := newTestQueryExecutor(ctx, tsv, query, 0)
	defer tsv.StopService()

	checkPlanID(t, planbuilder.PlanPassSelect, qre.plan.PlanID)
	_, err := qre.Execute()
	if err == nil {
		t.Fatal("got: nil, want: error")
	}
	got, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("got: %v, want: *TabletError", err)
	}
	if got.ErrorCode != vtrpcpb.ErrorCode_QUERY_NOT_SERVED {
		t.Fatalf("got: %s, want: QUERY_NOT_SERVED", got.ErrorCode)
	}
}

type executorFlags int64

const (
	noFlags      executorFlags = 0
	enableStrict               = 1 << iota
	enableStrictTableACL
	smallTxPool
	noTwopc
	shortTwopcAge
)

// newTestQueryExecutor uses a package level variable testTabletServer defined in tabletserver_test.go
func newTestTabletServer(ctx context.Context, flags executorFlags, db *fakesqldb.DB) *TabletServer {
	randID := rand.Int63()
	config := DefaultQsConfig
	config.StatsPrefix = fmt.Sprintf("Stats-%d-", randID)
	config.DebugURLPrefix = fmt.Sprintf("/debug-%d-", randID)
	config.PoolNamePrefix = fmt.Sprintf("Pool-%d-", randID)
	config.PoolSize = 100
	if flags&smallTxPool > 0 {
		config.TransactionCap = 3
	} else {
		config.TransactionCap = 100
	}
	config.EnablePublishStats = false
	config.EnableAutoCommit = true

	if flags&enableStrict > 0 {
		config.StrictMode = true
	} else {
		config.StrictMode = false
	}
	if flags&enableStrictTableACL > 0 {
		config.StrictTableAcl = true
	} else {
		config.StrictTableAcl = false
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
	tsv := NewTabletServer(config)
	testUtils := newTestUtils()
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	return tsv
}

func newTransaction(tsv *TabletServer) int64 {
	transactionID, err := tsv.Begin(context.Background(), &tsv.target)
	if err != nil {
		panic(fmt.Errorf("failed to start a transaction: %v", err))
	}
	return transactionID
}

func newTestQueryExecutor(ctx context.Context, tsv *TabletServer, sql string, txID int64) *QueryExecutor {
	logStats := NewLogStats(ctx, "TestQueryExecutor")
	return &QueryExecutor{
		ctx:           ctx,
		query:         sql,
		bindVars:      make(map[string]interface{}),
		transactionID: txID,
		plan:          tsv.qe.schemaInfo.GetPlan(ctx, logStats, sql),
		logStats:      logStats,
		qe:            tsv.qe,
		te:            tsv.te,
		messager:      tsv.messager,
	}
}

func testCommitHelper(t *testing.T, tsv *TabletServer, queryExecutor *QueryExecutor) {
	if err := tsv.Commit(queryExecutor.ctx, &tsv.target, queryExecutor.transactionID); err != nil {
		t.Fatalf("failed to commit transaction: %d, err: %v", queryExecutor.transactionID, err)
	}
}

func setUpQueryExecutorTest() *fakesqldb.DB {
	db := fakesqldb.Register()
	initQueryExecutorTestDB(db)
	return db
}

func initQueryExecutorTestDB(db *fakesqldb.DB) {
	for query, result := range getQueryExecutorSupportedQueries() {
		db.AddQuery(query, result)
	}
}

func fetchRecordedQueries(qre *QueryExecutor) []string {
	conn, err := qre.te.txPool.Get(qre.transactionID, "for query")
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

func getQueryExecutorSupportedQueries() map[string]*sqltypes.Result {
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
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeTrusted(sqltypes.Int32, []byte("1427325875"))},
			},
		},
		"select @@global.sql_mode": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("STRICT_TRANS_TABLES"))},
			},
		},
		"select @@autocommit": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("1"))},
			},
		},
		baseShowTables: {
			RowsAffected: 2,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte("test_table")),
					sqltypes.MakeString([]byte("USER TABLE")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("1427325875")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("2")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("3")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("4")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("5")),
				},
				{
					sqltypes.MakeString([]byte("seq")),
					sqltypes.MakeString([]byte("USER TABLE")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("1427325875")),
					sqltypes.MakeString([]byte("vitess_sequence")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("2")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("3")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("4")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("5")),
				},
				{
					sqltypes.MakeString([]byte("msg")),
					sqltypes.MakeString([]byte("USER TABLE")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("1427325875")),
					sqltypes.MakeString([]byte("vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("2")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("3")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("4")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("5")),
				},
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
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
				{
					sqltypes.MakeString([]byte("name")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
				{
					sqltypes.MakeString([]byte("addr")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
			},
		},
		// for SplitQuery because it needs a primary key column
		"show index from test_table": {
			RowsAffected: 2,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("PRIMARY")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("300")),
				},
				{
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("index")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("name")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("300")),
				},
			},
		},
		"begin":  {},
		"commit": {},
		baseShowTables + " and table_name = 'test_table'": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte("test_table")),
					sqltypes.MakeString([]byte("USER TABLE")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("1427325875")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("2")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("3")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("4")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("5")),
				},
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
			RowsAffected: 4,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte("id")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
				{
					sqltypes.MakeString([]byte("next_id")),
					sqltypes.MakeString([]byte("bigint")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
				{
					sqltypes.MakeString([]byte("cache")),
					sqltypes.MakeString([]byte("bigint")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
				{
					sqltypes.MakeString([]byte("increment")),
					sqltypes.MakeString([]byte("bigint")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
			},
		},
		"show index from seq": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("PRIMARY")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("id")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("300")),
				},
			},
		},
		baseShowTables + " and table_name = 'seq'": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte("seq")),
					sqltypes.MakeString([]byte("USER TABLE")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("1427325875")),
					sqltypes.MakeString([]byte("vitess_sequence")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("2")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("3")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("4")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("5")),
				},
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
			RowsAffected: 4,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte("time_scheduled")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
				{
					sqltypes.MakeString([]byte("id")),
					sqltypes.MakeString([]byte("bigint")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
				{
					sqltypes.MakeString([]byte("time_next")),
					sqltypes.MakeString([]byte("bigint")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
				{
					sqltypes.MakeString([]byte("epoch")),
					sqltypes.MakeString([]byte("bigint")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
				{
					sqltypes.MakeString([]byte("time_created")),
					sqltypes.MakeString([]byte("bigint")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
				{
					sqltypes.MakeString([]byte("time_acked")),
					sqltypes.MakeString([]byte("bigint")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
				{
					sqltypes.MakeString([]byte("message")),
					sqltypes.MakeString([]byte("bigint")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
			},
		},
		"show index from msg": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("PRIMARY")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("time_scheduled")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("300")),
				},
				{
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("PRIMARY")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("id")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("300")),
				},
			},
		},
		baseShowTables + " and table_name = 'msg'": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte("msg")),
					sqltypes.MakeString([]byte("USER TABLE")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("1427325875")),
					sqltypes.MakeString([]byte("vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("2")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("3")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("4")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("5")),
				},
			},
		},
		fmt.Sprintf(sqlReadAllRedo, "`_vt`", "`_vt`"): {},
	}
}
