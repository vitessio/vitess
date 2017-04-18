package vtgate

import (
	"bytes"
	"context"
	"html/template"
	"reflect"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

func TestExecutorTransactions(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	session := &vtgatepb.Session{TargetString: "@master"}

	// begin.
	_, err := executor.Execute(context.Background(), "begin", nil, session)
	if err != nil {
		t.Fatal(err)
	}
	wantSession := &vtgatepb.Session{InTransaction: true, TargetString: "@master"}
	if !reflect.DeepEqual(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
	}
	if commitCount := sbclookup.CommitCount.Get(); commitCount != 0 {
		t.Errorf("want 0, got %d", commitCount)
	}

	// commit.
	_, err = executor.Execute(context.Background(), "select id from main1", nil, session)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(context.Background(), "commit", nil, session)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{TargetString: "@master"}
	if !reflect.DeepEqual(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
	}
	if commitCount := sbclookup.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}

	// rollback.
	_, err = executor.Execute(context.Background(), "begin", nil, session)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(context.Background(), "select id from main1", nil, session)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(context.Background(), "rollback", nil, session)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{TargetString: "@master"}
	if !reflect.DeepEqual(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
	}
	if rollbackCount := sbclookup.RollbackCount.Get(); rollbackCount != 1 {
		t.Errorf("want 1, got %d", rollbackCount)
	}
}

func TestExecutorSet(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	session := &vtgatepb.Session{TargetString: "@master"}

	// set.
	_, err := executor.Execute(context.Background(), "set autocommit=1", nil, session)
	if err != nil {
		t.Fatal(err)
	}
	wantSession := &vtgatepb.Session{Autocommit: true, TargetString: "@master"}
	if !reflect.DeepEqual(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
	}
	_, err = executor.Execute(context.Background(), "set AUTOCOMMIT = 0", nil, session)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{TargetString: "@master"}
	if !reflect.DeepEqual(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
	}

	// complex set
	_, err = executor.Execute(context.Background(), "set autocommit=1+1", nil, session)
	wantErr := "invalid syntax: 1 + 1"
	if err == nil || err.Error() != wantErr {
		t.Errorf("Execute: %v, want %s", err, wantErr)
	}

	// multi-set
	_, err = executor.Execute(context.Background(), "set autocommit=1, a = 2", nil, session)
	wantErr = "too many set values: set autocommit=1, a = 2"
	if err == nil || err.Error() != wantErr {
		t.Errorf("Execute: %v, want %s", err, wantErr)
	}

	// unsupported set
	_, err = executor.Execute(context.Background(), "set a = 2", nil, session)
	wantErr = "unsupported construct: set a = 2"
	if err == nil || err.Error() != wantErr {
		t.Errorf("Execute: %v, want %s", err, wantErr)
	}
}

func TestExecutorAutocommit(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	session := &vtgatepb.Session{TargetString: "@master"}

	// autocommit = 0
	_, err := executor.Execute(context.Background(), "select id from main1", nil, session)
	if err != nil {
		t.Fatal(err)
	}
	wantSession := &vtgatepb.Session{TargetString: "@master"}
	if !reflect.DeepEqual(session, wantSession) {
		t.Errorf("autocommit=0: %v, want %v", session, wantSession)
	}

	// autocommit = 1
	_, err = executor.Execute(context.Background(), "set autocommit=1", nil, session)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(context.Background(), "update main1 set id=1", nil, session)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{Autocommit: true, TargetString: "@master"}
	if !reflect.DeepEqual(session, wantSession) {
		t.Errorf("autocommit=1: %v, want %v", session, wantSession)
	}
	if commitCount := sbclookup.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}

	// autocommit = 1, "begin"
	_, err = executor.Execute(context.Background(), "begin", nil, session)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(context.Background(), "update main1 set id=1", nil, session)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{InTransaction: true, Autocommit: true, TargetString: "@master"}
	testSession := *session
	testSession.ShardSessions = nil
	if !reflect.DeepEqual(&testSession, wantSession) {
		t.Errorf("autocommit=1: %v, want %v", &testSession, wantSession)
	}
	if commitCount := sbclookup.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}
	_, err = executor.Execute(context.Background(), "commit", nil, session)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{Autocommit: true, TargetString: "@master"}
	if !reflect.DeepEqual(session, wantSession) {
		t.Errorf("autocommit=1: %v, want %v", session, wantSession)
	}
	if commitCount := sbclookup.CommitCount.Get(); commitCount != 2 {
		t.Errorf("want 2, got %d", commitCount)
	}
}

func TestExecutorShow(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	session := &vtgatepb.Session{TargetString: "@master"}

	for _, query := range []string{"show databases", "show vitess_keyspaces"} {
		qr, err := executor.Execute(context.Background(), query, nil, session)
		if err != nil {
			t.Error(err)
		}
		wantqr := &sqltypes.Result{
			Fields: buildVarCharFields("Databases"),
			Rows: [][]sqltypes.Value{
				buildVarCharRow("TestBadSharding"),
				buildVarCharRow("TestExecutor"),
				buildVarCharRow(KsTestSharded),
				buildVarCharRow(KsTestUnsharded),
			},
			RowsAffected: 4,
		}
		if !reflect.DeepEqual(qr, wantqr) {
			t.Errorf("show databases:\n%+v, want\n%+v", qr, wantqr)
		}
	}

	qr, err := executor.Execute(context.Background(), "show vitess_shards", nil, session)
	if err != nil {
		t.Error(err)
	}
	// Just test for first & last.
	qr.Rows = [][]sqltypes.Value{qr.Rows[0], qr.Rows[len(qr.Rows)-1]}
	wantqr := &sqltypes.Result{
		Fields: buildVarCharFields("Shards"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("TestBadSharding/-20"),
			buildVarCharRow(KsTestUnsharded + "/0"),
		},
		RowsAffected: 25,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show databases:\n%+v, want\n%+v", qr, wantqr)
	}

	session = &vtgatepb.Session{TargetString: KsTestUnsharded}
	qr, err = executor.Execute(context.Background(), "show vschema_tables", nil, session)
	if err != nil {
		t.Error(err)
	}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Tables"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("main1"),
			buildVarCharRow("music_user_map"),
			buildVarCharRow("name_user_map"),
			buildVarCharRow("simple"),
			buildVarCharRow("user_seq"),
		},
		RowsAffected: 5,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show databases:\n%+v, want\n%+v", qr, wantqr)
	}

	session = &vtgatepb.Session{}
	qr, err = executor.Execute(context.Background(), "show vschema_tables", nil, session)
	want := "No keyspace selected"
	if err == nil || err.Error() != want {
		t.Errorf("show vschema_tables: %v, want %v", err, want)
	}

	qr, err = executor.Execute(context.Background(), "show 10", nil, session)
	want = "syntax error at position 8 near '10'"
	if err == nil || err.Error() != want {
		t.Errorf("show vschema_tables: %v, want %v", err, want)
	}

	session = &vtgatepb.Session{TargetString: "no_such_keyspace"}
	qr, err = executor.Execute(context.Background(), "show vschema_tables", nil, session)
	want = "keyspace no_such_keyspace not found in vschema"
	if err == nil || err.Error() != want {
		t.Errorf("show vschema_tables: %v, want %v", err, want)
	}
}

func TestExecutorOther(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()

	stmts := []string{
		"show other",
		"analyze",
		"describe",
		"explain",
		"repair",
		"optimize",
		"truncate",
	}
	wantCount := []int64{0, 0, 0}
	for _, stmt := range stmts {
		_, err := executor.Execute(context.Background(), stmt, nil, &vtgatepb.Session{TargetString: KsTestUnsharded})
		if err != nil {
			t.Error(err)
		}
		gotCount := []int64{
			sbc1.ExecCount.Get(),
			sbc2.ExecCount.Get(),
			sbclookup.ExecCount.Get(),
		}
		wantCount[2]++
		if !reflect.DeepEqual(gotCount, wantCount) {
			t.Errorf("Exec %s: %v, want %v", stmt, gotCount, wantCount)
		}

		_, err = executor.Execute(context.Background(), stmt, nil, &vtgatepb.Session{TargetString: "TestExecutor"})
		if err != nil {
			t.Error(err)
		}
		gotCount = []int64{
			sbc1.ExecCount.Get(),
			sbc2.ExecCount.Get(),
			sbclookup.ExecCount.Get(),
		}
		wantCount[0]++
		if !reflect.DeepEqual(gotCount, wantCount) {
			t.Errorf("Exec %s: %v, want %v", stmt, gotCount, wantCount)
		}
	}

	_, err := executor.Execute(context.Background(), "analyze", nil, &vtgatepb.Session{})
	want := "No keyspace selected"
	if err == nil || err.Error() != want {
		t.Errorf("show vschema_tables: %v, want %v", err, want)
	}
}

func TestExecutorDDL(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()

	stmts := []string{
		"create",
		"alter",
		"rename",
		"drop",
	}
	wantCount := []int64{0, 0, 0}
	for _, stmt := range stmts {
		_, err := executor.Execute(context.Background(), stmt, nil, &vtgatepb.Session{TargetString: KsTestUnsharded})
		if err != nil {
			t.Error(err)
		}
		gotCount := []int64{
			sbc1.ExecCount.Get(),
			sbc2.ExecCount.Get(),
			sbclookup.ExecCount.Get(),
		}
		wantCount[2]++
		if !reflect.DeepEqual(gotCount, wantCount) {
			t.Errorf("Exec %s: %v, want %v", stmt, gotCount, wantCount)
		}

		_, err = executor.Execute(context.Background(), stmt, nil, &vtgatepb.Session{TargetString: "TestExecutor"})
		if err != nil {
			t.Error(err)
		}
		gotCount = []int64{
			sbc1.ExecCount.Get(),
			sbc2.ExecCount.Get(),
			sbclookup.ExecCount.Get(),
		}
		wantCount[0]++
		wantCount[1]++
		if !reflect.DeepEqual(gotCount, wantCount) {
			t.Errorf("Exec %s: %v, want %v", stmt, gotCount, wantCount)
		}

		_, err = executor.Execute(context.Background(), stmt, nil, &vtgatepb.Session{TargetString: "TestExecutor/-20"})
		if err != nil {
			t.Error(err)
		}
		gotCount = []int64{
			sbc1.ExecCount.Get(),
			sbc2.ExecCount.Get(),
			sbclookup.ExecCount.Get(),
		}
		wantCount[0]++
		if !reflect.DeepEqual(gotCount, wantCount) {
			t.Errorf("Exec %s: %v, want %v", stmt, gotCount, wantCount)
		}
	}

	_, err := executor.Execute(context.Background(), "create", nil, &vtgatepb.Session{})
	want := "No keyspace selected"
	if err == nil || err.Error() != want {
		t.Errorf("show vschema_tables: %v, want %v", err, want)
	}
}

func TestExecutorUnrecognized(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	_, err := executor.Execute(context.Background(), "invalid statement", nil, &vtgatepb.Session{})
	want := "unrecognized statement: invalid statement"
	if err == nil || err.Error() != want {
		t.Errorf("show vschema_tables: %v, want %v", err, want)
	}
}

func TestExecutorMessageAckSharded(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	// Constant in IN is just a number, not a bind variable.
	ids := []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("1"),
	}}
	count, err := executor.MessageAck(context.Background(), "", "user", ids)
	if err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Errorf("count: %d, want 1", count)
	}
	if !reflect.DeepEqual(sbc1.MessageIDs, ids) {
		t.Errorf("sbc1.MessageIDs: %v, want %v", sbc1.MessageIDs, ids)
	}
	if sbc2.MessageIDs != nil {
		t.Errorf("sbc2.MessageIDs: %+v, want nil\n", sbc2.MessageIDs)
	}

	// Constant in IN is just a couple numbers, not bind variables.
	// They result in two different MessageIDs on two shards.
	sbc1.MessageIDs = nil
	sbc2.MessageIDs = nil
	ids = []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("1"),
	}, {
		Type:  sqltypes.VarChar,
		Value: []byte("3"),
	}}
	count, err = executor.MessageAck(context.Background(), "", "user", ids)
	if err != nil {
		t.Error(err)
	}
	if count != 2 {
		t.Errorf("count: %d, want 2", count)
	}
	wantids := []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("1"),
	}}
	if !reflect.DeepEqual(sbc1.MessageIDs, wantids) {
		t.Errorf("sbc1.MessageIDs: %+v, want %+v\n", sbc1.MessageIDs, wantids)
	}
	wantids = []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("3"),
	}}
	if !reflect.DeepEqual(sbc2.MessageIDs, wantids) {
		t.Errorf("sbc2.MessageIDs: %+v, want %+v\n", sbc2.MessageIDs, wantids)
	}
}

// TestVSchemaStats makes sure the building and displaying of the
// VSchemaStats works.
func TestVSchemaStats(t *testing.T) {
	r, _, _, _ := createExecutorEnv()

	stats := r.VSchemaStats()

	templ := template.New("")
	templ, err := templ.Parse(VSchemaTemplate)
	if err != nil {
		t.Fatalf("error parsing template: %v", err)
	}
	wr := &bytes.Buffer{}
	if err := templ.Execute(wr, stats); err != nil {
		t.Fatalf("error executing template: %v", err)
	}
	result := wr.String()
	if !strings.Contains(result, "<td>TestBadSharding</td>") ||
		!strings.Contains(result, "<td>TestUnsharded</td>") {
		t.Errorf("invalid html result: %v", result)
	}
}

func TestGetPlanUnnormalized(t *testing.T) {
	r, _, _, _ := createExecutorEnv()
	query1 := "select * from music_user_map where id = 1"
	plan1, err := r.getPlan(query1, "", map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	plan2, err := r.getPlan(query1, "", map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	if plan1 != plan2 {
		t.Errorf("getPlan(query1): plans must be equal: %p %p", plan1, plan2)
	}
	want := []string{
		query1,
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}
	plan3, err := r.getPlan(query1, KsTestUnsharded, map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	if plan1 == plan3 {
		t.Errorf("getPlan(query1, ks): plans must not be equal: %p %p", plan1, plan3)
	}
	plan4, err := r.getPlan(query1, KsTestUnsharded, map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	if plan3 != plan4 {
		t.Errorf("getPlan(query1, ks): plans must be equal: %p %p", plan3, plan4)
	}
	want = []string{
		KsTestUnsharded + ":" + query1,
		query1,
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}
}

func TestGetPlanNormalized(t *testing.T) {
	r, _, _, _ := createExecutorEnv()
	r.normalize = true
	query1 := "select * from music_user_map where id = 1"
	query2 := "select * from music_user_map where id = 2"
	normalized := "select * from music_user_map where id = :vtg1"
	plan1, err := r.getPlan(query1, "", map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	plan2, err := r.getPlan(query1, "", map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	if plan1 != plan2 {
		t.Errorf("getPlan(query1): plans must be equal: %p %p", plan1, plan2)
	}
	want := []string{
		normalized,
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}
	plan3, err := r.getPlan(query2, "", map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	if plan1 != plan3 {
		t.Errorf("getPlan(query2): plans must be equal: %p %p", plan1, plan3)
	}
	plan4, err := r.getPlan(normalized, "", map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	if plan1 != plan4 {
		t.Errorf("getPlan(normalized): plans must be equal: %p %p", plan1, plan4)
	}

	plan3, err = r.getPlan(query1, KsTestUnsharded, map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	if plan1 == plan3 {
		t.Errorf("getPlan(query1, ks): plans must not be equal: %p %p", plan1, plan3)
	}
	plan4, err = r.getPlan(query1, KsTestUnsharded, map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	if plan3 != plan4 {
		t.Errorf("getPlan(query1, ks): plans must be equal: %p %p", plan3, plan4)
	}
	want = []string{
		KsTestUnsharded + ":" + normalized,
		normalized,
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}

	// Errors
	_, err = r.getPlan("syntax", "", map[string]interface{}{})
	wantErr := "syntax error at position 7 near 'syntax'"
	if err == nil || err.Error() != wantErr {
		t.Errorf("getPlan(syntax): %v, want %s", err, wantErr)
	}
	_, err = r.getPlan("create table a(id int)", "", map[string]interface{}{})
	wantErr = "unsupported construct: ddl"
	if err == nil || err.Error() != wantErr {
		t.Errorf("getPlan(syntax): %v, want %s", err, wantErr)
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}
}

func TestParseTarget(t *testing.T) {
	testcases := []struct {
		targetString string
		target       querypb.Target
	}{{
		targetString: "ks",
		target: querypb.Target{
			Keyspace:   "ks",
			TabletType: topodatapb.TabletType_MASTER,
		},
	}, {
		targetString: "ks/-80",
		target: querypb.Target{
			Keyspace:   "ks",
			Shard:      "-80",
			TabletType: topodatapb.TabletType_MASTER,
		},
	}, {
		targetString: "ks:-80",
		target: querypb.Target{
			Keyspace:   "ks",
			Shard:      "-80",
			TabletType: topodatapb.TabletType_MASTER,
		},
	}, {
		targetString: "ks@replica",
		target: querypb.Target{
			Keyspace:   "ks",
			TabletType: topodatapb.TabletType_REPLICA,
		},
	}, {
		targetString: "ks:-80@replica",
		target: querypb.Target{
			Keyspace:   "ks",
			Shard:      "-80",
			TabletType: topodatapb.TabletType_REPLICA,
		},
	}, {
		targetString: "@replica",
		target: querypb.Target{
			TabletType: topodatapb.TabletType_REPLICA,
		},
	}, {
		targetString: "@bad",
		target: querypb.Target{
			TabletType: topodatapb.TabletType_UNKNOWN,
		},
	}}

	for _, tcase := range testcases {
		if target := parseTarget(tcase.targetString); target != tcase.target {
			t.Errorf("parseKeyspaceShard(%s): %v, want %v", tcase.targetString, target, tcase.target)
		}
	}
}
