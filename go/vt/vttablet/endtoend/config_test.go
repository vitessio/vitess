// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/vttablet/endtoend/framework"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"github.com/youtube/vitess/go/vt/vterrors"
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
		tag: "QueryCacheCapacity",
		val: tabletenv.Config.QueryCacheSize,
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

func TestQueryCache(t *testing.T) {
	defer framework.Server.SetQueryCacheCap(framework.Server.QueryCacheCap())
	framework.Server.SetQueryCacheCap(1)

	bindVars := map[string]interface{}{"ival1": 1, "ival2": 1}
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

	framework.Server.SetQueryCacheCap(10)
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

func TestMexResultSize(t *testing.T) {
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
	err := client.Begin()
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

func TestStrictMode(t *testing.T) {
	queries := []string{
		"insert into vitess_a(eid, id, name, foo) values (7, 1+1, '', '')",
		"insert into vitess_d(eid, id) values (1, 1)",
		"update vitess_a set eid = 1+1 where eid = 1 and id = 1",
		"insert into vitess_d(eid, id) values (1, 1)",
		"insert into upsert_test(id1, id2) values " +
			"(1, 1), (2, 2) on duplicate key update id1 = 1",
		"insert into upsert_test(id1, id2) select eid, id " +
			"from vitess_a limit 1 on duplicate key update id2 = id1",
		"insert into upsert_test(id1, id2) values " +
			"(1, 1) on duplicate key update id1 = 2+1",
	}

	// Strict mode on.
	func() {
		client := framework.NewClient()
		err := client.Begin()
		if err != nil {
			t.Error(err)
			return
		}
		defer client.Rollback()

		want := "DML too complex"
		for _, query := range queries {
			_, err = client.Execute(query, nil)
			if err == nil || err.Error() != want {
				t.Errorf("Execute(%s): %v, want %s", query, err, want)
			}
		}
	}()

	// Strict mode off.
	func() {
		framework.Server.SetStrictMode(false)
		defer framework.Server.SetStrictMode(true)

		for _, query := range queries {
			client := framework.NewClient()
			err := client.Begin()
			if err != nil {
				t.Error(err)
				return
			}
			_, err = client.Execute(query, nil)
			if err != nil {
				t.Error(err)
			}
			client.Rollback()
		}
	}()
}
