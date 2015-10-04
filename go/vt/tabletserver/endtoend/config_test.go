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

	"github.com/youtube/vitess/go/vt/tabletserver/endtoend/framework"
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
		val: int(framework.BaseConfig.TxPoolTimeout * 1e9),
	}, {
		tag: "ConnPoolAvailable",
		val: framework.BaseConfig.PoolSize,
	}, {
		tag: "ConnPoolCapacity",
		val: framework.BaseConfig.PoolSize,
	}, {
		tag: "ConnPoolIdleTimeout",
		val: int(framework.BaseConfig.IdleTimeout * 1e9),
	}, {
		tag: "ConnPoolMaxCap",
		val: framework.BaseConfig.PoolSize,
	}, {
		tag: "MaxDMLRows",
		val: framework.BaseConfig.MaxDMLRows,
	}, {
		tag: "MaxResultSize",
		val: framework.BaseConfig.MaxResultSize,
	}, {
		tag: "QueryCacheCapacity",
		val: framework.BaseConfig.QueryCacheSize,
	}, {
		tag: "QueryTimeout",
		val: int(framework.BaseConfig.QueryTimeout * 1e9),
	}, {
		tag: "RowcacheConnPoolAvailable",
		val: framework.BaseConfig.RowCache.Connections - 50,
	}, {
		tag: "RowcacheConnPoolCapacity",
		val: framework.BaseConfig.RowCache.Connections - 50,
	}, {
		tag: "RowcacheConnPoolIdleTimeout",
		val: int(framework.BaseConfig.IdleTimeout * 1e9),
	}, {
		tag: "RowcacheConnPoolMaxCap",
		val: framework.BaseConfig.RowCache.Connections - 50,
	}, {
		tag: "SchemaReloadTime",
		val: int(framework.BaseConfig.SchemaReloadTime * 1e9),
	}, {
		tag: "StreamBufferSize",
		val: framework.BaseConfig.StreamBufferSize,
	}, {
		tag: "StreamConnPoolAvailable",
		val: framework.BaseConfig.StreamPoolSize,
	}, {
		tag: "StreamConnPoolCapacity",
		val: framework.BaseConfig.StreamPoolSize,
	}, {
		tag: "StreamConnPoolIdleTimeout",
		val: int(framework.BaseConfig.IdleTimeout * 1e9),
	}, {
		tag: "StreamConnPoolMaxCap",
		val: framework.BaseConfig.StreamPoolSize,
	}, {
		tag: "TransactionPoolAvailable",
		val: framework.BaseConfig.TransactionCap,
	}, {
		tag: "TransactionPoolCapacity",
		val: framework.BaseConfig.TransactionCap,
	}, {
		tag: "TransactionPoolIdleTimeout",
		val: int(framework.BaseConfig.IdleTimeout * 1e9),
	}, {
		tag: "TransactionPoolMaxCap",
		val: framework.BaseConfig.TransactionCap,
	}, {
		tag: "TransactionPoolTimeout",
		val: int(framework.BaseConfig.TransactionTimeout * 1e9),
	}}
	for _, tcase := range cases {
		if err := verifyIntValue(vars, tcase.tag, tcase.val); err != nil {
			t.Error(err)
		}
	}
}

func TestPoolSize(t *testing.T) {
	vstart := framework.DebugVars()
	defer framework.DefaultServer.SetPoolSize(framework.DefaultServer.PoolSize())
	framework.DefaultServer.SetPoolSize(1)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		framework.NewDefaultClient().Execute("select sleep(0.25) from dual", nil)
		wg.Done()
	}()
	// The queries have to be different so consolidator doesn't kick in.
	go func() {
		framework.NewDefaultClient().Execute("select sleep(0.24) from dual", nil)
		wg.Done()
	}()
	wg.Wait()

	vend := framework.DebugVars()
	if err := verifyIntValue(vend, "ConnPoolCapacity", 1); err != nil {
		t.Error(err)
	}
	if err := compareIntDiff(vend, "ConnPoolWaitCount", vstart, 1); err != nil {
		t.Error(err)
	}
}

func TestQueryCache(t *testing.T) {
	defer framework.DefaultServer.SetQueryCacheCap(framework.DefaultServer.QueryCacheCap())
	framework.DefaultServer.SetQueryCacheCap(1)

	bindVars := map[string]interface{}{"ival1": 1, "ival2": 1}
	client := framework.NewDefaultClient()
	_, _ = client.Execute("select * from vtocc_test where intval=:ival1", bindVars)
	_, _ = client.Execute("select * from vtocc_test where intval=:ival2", bindVars)
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

	framework.DefaultServer.SetQueryCacheCap(10)
	_, _ = client.Execute("select * from vtocc_test where intval=:ival1", bindVars)
	vend = framework.DebugVars()
	if err := verifyIntValue(vend, "QueryCacheLength", 2); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vend, "QueryCacheSize", 2); err != nil {
		t.Error(err)
	}

	_, _ = client.Execute("select * from vtocc_test where intval=1", bindVars)
	vend = framework.DebugVars()
	if err := verifyIntValue(vend, "QueryCacheLength", 3); err != nil {
		t.Error(err)
	}
	if err := verifyIntValue(vend, "QueryCacheSize", 3); err != nil {
		t.Error(err)
	}
}

func TestMexResultSize(t *testing.T) {
	defer framework.DefaultServer.SetMaxResultSize(framework.DefaultServer.MaxResultSize())
	framework.DefaultServer.SetMaxResultSize(2)

	client := framework.NewDefaultClient()
	query := "select * from vtocc_test"
	_, err := client.Execute(query, nil)
	want := "error: Row count exceeded"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Error: %v, must start with %s", err, want)
	}
	if err := verifyIntValue(framework.DebugVars(), "MaxResultSize", 2); err != nil {
		t.Error(err)
	}

	framework.DefaultServer.SetMaxResultSize(10)
	_, err = client.Execute(query, nil)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestMaxDMLRows(t *testing.T) {
	client := framework.NewDefaultClient()
	_, err := client.Execute(
		"insert into vtocc_a(eid, id, name, foo) values "+
			"(3, 1, '', ''), (3, 2, '', ''), (3, 3, '', '')",
		nil,
	)
	catcher := framework.NewQueryCatcher()
	defer catcher.Close()

	// Verify all three rows are updated in a single DML.
	_, err = client.Execute("update vtocc_a set foo='fghi' where eid = 3", nil)
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
		"select eid, id from vtocc_a where eid = 3 limit 10001 for update; " +
		"update vtocc_a set foo = 'fghi' where " +
		"(eid = 3 and id = 1) or (eid = 3 and id = 2) or (eid = 3 and id = 3) " +
		"/* _stream vtocc_a (eid id ) (3 1 ) (3 2 ) (3 3 ); */; " +
		"commit"
	if queryInfo.RewrittenSQL() != want {
		t.Errorf("Query info: \n%s, want \n%s", queryInfo.RewrittenSQL(), want)
	}

	// Verify that rows get split, and if pk changes, those values are also
	// split correctly.
	defer framework.DefaultServer.SetMaxDMLRows(framework.DefaultServer.MaxDMLRows())
	framework.DefaultServer.SetMaxDMLRows(2)
	_, err = client.Execute("update vtocc_a set eid=2 where eid = 3", nil)
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
		"select eid, id from vtocc_a where eid = 3 limit 10001 for update; " +
		"update vtocc_a set eid = 2 where " +
		"(eid = 3 and id = 1) or (eid = 3 and id = 2) " +
		"/* _stream vtocc_a (eid id ) (3 1 ) (3 2 ) (2 1 ) (2 2 ); */; " +
		"update vtocc_a set eid = 2 where (eid = 3 and id = 3) " +
		"/* _stream vtocc_a (eid id ) (3 3 ) (2 3 ); */; " +
		"commit"
	if queryInfo.RewrittenSQL() != want {
		t.Errorf("Query info: \n%s, want \n%s", queryInfo.RewrittenSQL(), want)
	}

	// Verify that a normal update is split correctly.
	_, err = client.Execute("update vtocc_a set foo='fghi' where eid = 2", nil)
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
		"select eid, id from vtocc_a where eid = 2 limit 10001 for update; " +
		"update vtocc_a set foo = 'fghi' where (eid = 2 and id = 1) or " +
		"(eid = 2 and id = 2) /* _stream vtocc_a (eid id ) (2 1 ) (2 2 ); */; " +
		"update vtocc_a set foo = 'fghi' where (eid = 2 and id = 3) " +
		"/* _stream vtocc_a (eid id ) (2 3 ); */; " +
		"commit"
	if queryInfo.RewrittenSQL() != want {
		t.Errorf("Query info: \n%s, want \n%s", queryInfo.RewrittenSQL(), want)
	}

	// Verufy that a delete is split correctly.
	_, err = client.Execute("delete from vtocc_a where eid = 2", nil)
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
		"select eid, id from vtocc_a where eid = 2 limit 10001 for update; " +
		"delete from vtocc_a where (eid = 2 and id = 1) or (eid = 2 and id = 2) " +
		"/* _stream vtocc_a (eid id ) (2 1 ) (2 2 ); */; " +
		"delete from vtocc_a where (eid = 2 and id = 3) " +
		"/* _stream vtocc_a (eid id ) (2 3 ); */; " +
		"commit"
	if queryInfo.RewrittenSQL() != want {
		t.Errorf("Query info: \n%s, want \n%s", queryInfo.RewrittenSQL(), want)
	}
}

func TestQueryTimeout(t *testing.T) {
	vstart := framework.DebugVars()
	defer framework.DefaultServer.QueryTimeout.Set(framework.DefaultServer.QueryTimeout.Get())
	framework.DefaultServer.QueryTimeout.Set(10 * time.Millisecond)

	client := framework.NewDefaultClient()
	err := client.Begin()
	if err != nil {
		t.Error(err)
		return
	}
	_, err = client.Execute("select sleep(0.5) from vtocc_test", nil)
	want := "error: the query was killed"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Error: %v, must start with %s", err, want)
	}
	_, err = client.Execute("select 1 from dual", nil)
	want = "not_in_tx: Transaction"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Error: %v, must start with %s", err, want)
	}
	vend := framework.DebugVars()
	if err := verifyIntValue(vend, "QueryTimeout", int(10*time.Millisecond)); err != nil {
		t.Error(err)
	}
	if err := compareIntDiff(vend, "Kills.Queries", vstart, 1); err != nil {
		t.Error(err)
	}
}
