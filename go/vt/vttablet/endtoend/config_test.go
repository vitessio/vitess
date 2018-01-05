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

package endtoend

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/endtoend/framework"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// compareIntDiff returns an error if end[tag] != start[tag]+diff.
func compareIntDiff(end map[string]interface{}, tag string, start map[string]interface{}, diff int) error {
	return verifyIntValue(end, tag, framework.FetchInt(start, tag)+diff)
}

// verifyIntValue retuns an error if values[tag] != want.
func verifyIntValue(values map[string]interface{}, tag string, want int) error {
	got := framework.FetchInt(values, tag)
	if got != want {
		return fmt.Errorf("%s: %d, want %d", tag, got, want)
	}
	return nil
}

func TestConfigVars(t *testing.T) {
	vars := framework.DebugVars()
	cases := []struct {
		tag string
		val int
	}{{
		tag: "BeginTimeout",
		val: int(tabletenv.Config.TxPoolTimeout * 1e9),
	}, {
		tag: "ConnPoolAvailable",
		val: tabletenv.Config.PoolSize,
	}, {
		tag: "ConnPoolCapacity",
		val: tabletenv.Config.PoolSize,
	}, {
		tag: "ConnPoolIdleTimeout",
		val: int(tabletenv.Config.IdleTimeout * 1e9),
	}, {
		tag: "ConnPoolMaxCap",
		val: tabletenv.Config.PoolSize,
	}, {
		tag: "MaxDMLRows",
		val: tabletenv.Config.MaxDMLRows,
	}, {
		tag: "MaxResultSize",
		val: tabletenv.Config.MaxResultSize,
	}, {
		tag: "WarnResultSize",
		val: tabletenv.Config.WarnResultSize,
	}, {
		tag: "QueryCacheCapacity",
		val: tabletenv.Config.QueryPlanCacheSize,
	}, {
		tag: "QueryTimeout",
		val: int(tabletenv.Config.QueryTimeout * 1e9),
	}, {
		tag: "SchemaReloadTime",
		val: int(tabletenv.Config.SchemaReloadTime * 1e9),
	}, {
		tag: "StreamBufferSize",
		val: tabletenv.Config.StreamBufferSize,
	}, {
		tag: "StreamConnPoolAvailable",
		val: tabletenv.Config.StreamPoolSize,
	}, {
		tag: "StreamConnPoolCapacity",
		val: tabletenv.Config.StreamPoolSize,
	}, {
		tag: "StreamConnPoolIdleTimeout",
		val: int(tabletenv.Config.IdleTimeout * 1e9),
	}, {
		tag: "StreamConnPoolMaxCap",
		val: tabletenv.Config.StreamPoolSize,
	}, {
		tag: "TransactionPoolAvailable",
		val: tabletenv.Config.TransactionCap,
	}, {
		tag: "TransactionPoolCapacity",
		val: tabletenv.Config.TransactionCap,
	}, {
		tag: "TransactionPoolIdleTimeout",
		val: int(tabletenv.Config.IdleTimeout * 1e9),
	}, {
		tag: "TransactionPoolMaxCap",
		val: tabletenv.Config.TransactionCap,
	}, {
		tag: "TransactionPoolTimeout",
		val: int(tabletenv.Config.TransactionTimeout * 1e9),
	}}
	for _, tcase := range cases {
		if err := verifyIntValue(vars, tcase.tag, tcase.val); err != nil {
			t.Error(err)
		}
	}
}

func TestPoolSize(t *testing.T) {
	defer framework.Server.SetPoolSize(framework.Server.PoolSize())
	framework.Server.SetPoolSize(1)

	vstart := framework.DebugVars()
	if err := verifyIntValue(vstart, "ConnPoolCapacity", 1); err != nil {
		t.Error(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		framework.NewClient().Execute("select sleep(0.5) from dual", nil)
		wg.Done()
	}()
	// The queries have to be different so consolidator doesn't kick in.
	go func() {
		framework.NewClient().Execute("select sleep(0.49) from dual", nil)
		wg.Done()
	}()
	wg.Wait()

	// Parallel plan building can cause multiple conn pool waits.
	// Check that the wait count was at least incremented once so
	// we know it's working.
	tag := "ConnPoolWaitCount"
	got := framework.FetchInt(framework.DebugVars(), tag)
	want := framework.FetchInt(vstart, tag)
	if got <= want {
		t.Errorf("%s: %d, must be greater than %d", tag, got, want)
	}
}

func TestQueryPlanCache(t *testing.T) {
	defer framework.Server.SetQueryPlanCacheCap(framework.Server.QueryPlanCacheCap())
	framework.Server.SetQueryPlanCacheCap(1)

	bindVars := map[string]*querypb.BindVariable{
		"ival1": sqltypes.Int64BindVariable(1),
		"ival2": sqltypes.Int64BindVariable(1),
	}
	client := framework.NewClient()
	_, _ = client.Execute("select * from vitess_test where intval=:ival1", bindVars)
	_, _ = client.Execute("select * from vitess_test where intval=:ival2", bindVars)
	vend := framework.DebugVars()
	if err := verifyIntValue(vend, "QueryCacheLength", 1); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vend, "QueryCacheSize", 1); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vend, "QueryCacheCapacity", 1); err != nil {
		t.Error(err)
	}

	framework.Server.SetQueryPlanCacheCap(10)
	_, _ = client.Execute("select * from vitess_test where intval=:ival1", bindVars)
	vend = framework.DebugVars()
	if err := verifyIntValue(vend, "QueryCacheLength", 2); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vend, "QueryCacheSize", 2); err != nil {
		t.Error(err)
	}

	_, _ = client.Execute("select * from vitess_test where intval=1", bindVars)
	vend = framework.DebugVars()
	if err := verifyIntValue(vend, "QueryCacheLength", 3); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vend, "QueryCacheSize", 3); err != nil {
		t.Error(err)
	}
}

func TestMaxResultSize(t *testing.T) {
	defer framework.Server.SetMaxResultSize(framework.Server.MaxResultSize())
	framework.Server.SetMaxResultSize(2)

	client := framework.NewClient()
	query := "select * from vitess_test"
	_, err := client.Execute(query, nil)
	want := "Row count exceeded"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Error: %v, must start with %s", err, want)
	}
	if err := verifyIntValue(framework.DebugVars(), "MaxResultSize", 2); err != nil {
		t.Error(err)
	}

	framework.Server.SetMaxResultSize(10)
	_, err = client.Execute(query, nil)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestWarnResultSize(t *testing.T) {
	defer framework.Server.SetWarnResultSize(framework.Server.WarnResultSize())
	framework.Server.SetWarnResultSize(2)
	client := framework.NewClient()

	originalWarningsResultsExceededCount := framework.FetchInt(framework.DebugVars(), "Warnings/ResultsExceeded")
	query := "select * from vitess_test"
	_, _ = client.Execute(query, nil)
	newWarningsResultsExceededCount := framework.FetchInt(framework.DebugVars(), "Warnings/ResultsExceeded")
	exceededCountDiff := newWarningsResultsExceededCount - originalWarningsResultsExceededCount
	if exceededCountDiff != 1 {
		t.Errorf("Warnings.ResultsExceeded counter should have increased by 1, instead got %v", exceededCountDiff)
	}

	if err := verifyIntValue(framework.DebugVars(), "WarnResultSize", 2); err != nil {
		t.Error(err)
	}

	framework.Server.SetWarnResultSize(10)
	_, _ = client.Execute(query, nil)
	newerWarningsResultsExceededCount := framework.FetchInt(framework.DebugVars(), "Warnings/ResultsExceeded")
	exceededCountDiff = newerWarningsResultsExceededCount - newWarningsResultsExceededCount
	if exceededCountDiff != 0 {
		t.Errorf("Warnings.ResultsExceeded counter should not have increased, instead got %v", exceededCountDiff)
	}
}

func TestMaxDMLRows(t *testing.T) {
	client := framework.NewClient()
	_, err := client.Execute(
		"insert into vitess_a(eid, id, name, foo) values "+
			"(3, 1, '', ''), (3, 2, '', ''), (3, 3, '', '')",
		nil,
	)
	catcher := framework.NewQueryCatcher()
	defer catcher.Close()

	// Verify all three rows are updated in a single DML.
	_, err = client.Execute("update vitess_a set foo='fghi' where eid = 3", nil)
	if err != nil {
		t.Error(err)
		return
	}
	queryInfo, err := catcher.Next()
	if err != nil {
		t.Error(err)
		return
	}
	want := "begin; " +
		"select eid, id from vitess_a where eid = 3 limit 10001 for update; " +
		"update vitess_a set foo = 'fghi' where " +
		"(eid = 3 and id = 1) or (eid = 3 and id = 2) or (eid = 3 and id = 3) " +
		"/* _stream vitess_a (eid id ) (3 1 ) (3 2 ) (3 3 ); */; " +
		"commit"
	if queryInfo.RewrittenSQL() != want {
		t.Errorf("Query info: \n%s, want \n%s", queryInfo.RewrittenSQL(), want)
	}

	// Verify that rows get split, and if pk changes, those values are also
	// split correctly.
	defer framework.Server.SetMaxDMLRows(framework.Server.MaxDMLRows())
	framework.Server.SetMaxDMLRows(2)
	_, err = client.Execute("update vitess_a set eid=2 where eid = 3", nil)
	if err != nil {
		t.Error(err)
		return
	}
	queryInfo, err = catcher.Next()
	if err != nil {
		t.Error(err)
		return
	}
	want = "begin; " +
		"select eid, id from vitess_a where eid = 3 limit 10001 for update; " +
		"update vitess_a set eid = 2 where " +
		"(eid = 3 and id = 1) or (eid = 3 and id = 2) " +
		"/* _stream vitess_a (eid id ) (3 1 ) (3 2 ) (2 1 ) (2 2 ); */; " +
		"update vitess_a set eid = 2 where (eid = 3 and id = 3) " +
		"/* _stream vitess_a (eid id ) (3 3 ) (2 3 ); */; " +
		"commit"
	if queryInfo.RewrittenSQL() != want {
		t.Errorf("Query info: \n%s, want \n%s", queryInfo.RewrittenSQL(), want)
	}

	// Verify that a normal update is split correctly.
	_, err = client.Execute("update vitess_a set foo='fghi' where eid = 2", nil)
	if err != nil {
		t.Error(err)
		return
	}
	queryInfo, err = catcher.Next()
	if err != nil {
		t.Error(err)
		return
	}
	want = "begin; " +
		"select eid, id from vitess_a where eid = 2 limit 10001 for update; " +
		"update vitess_a set foo = 'fghi' where (eid = 2 and id = 1) or " +
		"(eid = 2 and id = 2) /* _stream vitess_a (eid id ) (2 1 ) (2 2 ); */; " +
		"update vitess_a set foo = 'fghi' where (eid = 2 and id = 3) " +
		"/* _stream vitess_a (eid id ) (2 3 ); */; " +
		"commit"
	if queryInfo.RewrittenSQL() != want {
		t.Errorf("Query info: \n%s, want \n%s", queryInfo.RewrittenSQL(), want)
	}

	// Verufy that a delete is split correctly.
	_, err = client.Execute("delete from vitess_a where eid = 2", nil)
	if err != nil {
		t.Error(err)
		return
	}
	queryInfo, err = catcher.Next()
	if err != nil {
		t.Error(err)
		return
	}
	want = "begin; " +
		"select eid, id from vitess_a where eid = 2 limit 10001 for update; " +
		"delete from vitess_a where (eid = 2 and id = 1) or (eid = 2 and id = 2) " +
		"/* _stream vitess_a (eid id ) (2 1 ) (2 2 ); */; " +
		"delete from vitess_a where (eid = 2 and id = 3) " +
		"/* _stream vitess_a (eid id ) (2 3 ); */; " +
		"commit"
	if queryInfo.RewrittenSQL() != want {
		t.Errorf("Query info: \n%s, want \n%s", queryInfo.RewrittenSQL(), want)
	}
}

func TestQueryTimeout(t *testing.T) {
	vstart := framework.DebugVars()
	defer framework.Server.QueryTimeout.Set(framework.Server.QueryTimeout.Get())
	framework.Server.QueryTimeout.Set(100 * time.Millisecond)

	client := framework.NewClient()
	err := client.Begin(false)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = client.Execute("select sleep(1) from vitess_test", nil)
	if code := vterrors.Code(err); code != vtrpcpb.Code_DEADLINE_EXCEEDED {
		t.Errorf("Error code: %v, want %v", code, vtrpcpb.Code_DEADLINE_EXCEEDED)
	}
	_, err = client.Execute("select 1 from dual", nil)
	if code := vterrors.Code(err); code != vtrpcpb.Code_ABORTED {
		t.Errorf("Error code: %v, want %v", code, vtrpcpb.Code_ABORTED)
	}
	vend := framework.DebugVars()
	if err := verifyIntValue(vend, "QueryTimeout", int(100*time.Millisecond)); err != nil {
		t.Error(err)
	}
	if err := compareIntDiff(vend, "Kills/Queries", vstart, 1); err != nil {
		t.Error(err)
	}
}
