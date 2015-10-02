// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/youtube/vitess/go/mysql"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
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

func TestSimpleRead(t *testing.T) {
	vstart := framework.DebugVars()
	_, err := framework.NewDefaultClient().Execute("select * from vtocc_test where intval=1", nil)
	if err != nil {
		t.Error(err)
		return
	}
	vend := framework.DebugVars()
	if err := compareIntDiff(vend, "Queries.TotalCount", vstart, 1); err != nil {
		t.Error(err)
	}
	if err := compareIntDiff(vend, "Queries.Histograms.PASS_SELECT.Count", vstart, 1); err != nil {
		t.Error(err)
	}
}

func TestBinary(t *testing.T) {
	client := framework.NewDefaultClient()
	defer client.Execute("delete from vtocc_test where intval in (4,5)", nil)

	binaryData := "\x00'\"\b\n\r\t\x1a\\\x00\x0f\xf0\xff"
	// Test without bindvars.
	_, err := client.Execute(
		"insert into vtocc_test values "+
			"(4, null, null, '\\0\\'\\\"\\b\\n\\r\\t\\Z\\\\\x00\x0f\xf0\xff')",
		nil,
	)
	if err != nil {
		t.Error(err)
		return
	}
	qr, err := client.Execute("select binval from vtocc_test where intval=4", nil)
	if err != nil {
		t.Error(err)
		return
	}
	want := mproto.QueryResult{
		Fields: []mproto.Field{
			{
				Name:  "binval",
				Type:  mysql.TypeVarString,
				Flags: mysql.FlagBinary,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.Value{Inner: sqltypes.String(binaryData)},
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}

	// Test with bindvars.
	_, err = client.Execute(
		"insert into vtocc_test values(5, null, null, :bindata)",
		map[string]interface{}{"bindata": binaryData},
	)
	if err != nil {
		t.Error(err)
		return
	}
	qr, err = client.Execute("select binval from vtocc_test where intval=5", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}
}

func TestNocacheListArgs(t *testing.T) {
	client := framework.NewDefaultClient()
	query := "select * from vtocc_test where intval in ::list"

	qr, err := client.Execute(
		query,
		map[string]interface{}{
			"list": []interface{}{2, 3, 4},
		},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 2 {
		t.Errorf("rows affected: %d, want 2", qr.RowsAffected)
	}

	qr, err = client.Execute(
		query,
		map[string]interface{}{
			"list": []interface{}{3, 4},
		},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 1 {
		t.Errorf("rows affected: %d, want 1", qr.RowsAffected)
	}

	qr, err = client.Execute(
		query,
		map[string]interface{}{
			"list": []interface{}{3},
		},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 1 {
		t.Errorf("rows affected: %d, want 1", qr.RowsAffected)
	}

	// Error case
	_, err = client.Execute(
		query,
		map[string]interface{}{
			"list": []interface{}{},
		},
	)
	want := "error: empty list supplied for list"
	if err == nil || err.Error() != want {
		t.Errorf("error returned: %v, want %s", err, want)
		return
	}
}

func TestIntegrityError(t *testing.T) {
	vstart := framework.DebugVars()
	client := framework.NewDefaultClient()
	_, err := client.Execute("insert into vtocc_test values(1, null, null, null)", nil)
	want := "error: Duplicate entry '1'"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Error: %v, want prefix %s", err, want)
	}
	if err := compareIntDiff(framework.DebugVars(), "InfoErrors.DupKey", vstart, 1); err != nil {
		t.Error(err)
	}
}

func TestTrailingComment(t *testing.T) {
	vstart := framework.DebugVars()
	v1 := framework.FetchInt(vstart, "QueryCacheLength")

	bindVars := map[string]interface{}{"ival": 1}
	client := framework.NewDefaultClient()

	for _, query := range []string{
		"select * from vtocc_test where intval=:ival",
		"select * from vtocc_test where intval=:ival /* comment */",
		"select * from vtocc_test where intval=:ival /* comment1 */ /* comment2 */",
	} {
		_, err := client.Execute(query, bindVars)
		if err != nil {
			t.Error(err)
			return
		}
		v2 := framework.FetchInt(framework.DebugVars(), "QueryCacheLength")
		if v2 != v1+1 {
			t.Errorf("QueryCacheLength(%s): %d, want %d", query, v2, v1+1)
		}
	}
}

func TestStrictMode(t *testing.T) {
	queries := []string{
		"insert into vtocc_a(eid, id, name, foo) values (7, 1+1, '', '')",
		"insert into vtocc_d(eid, id) values (1, 1)",
		"update vtocc_a set eid = 1+1 where eid = 1 and id = 1",
		"insert into vtocc_d(eid, id) values (1, 1)",
		"insert into upsert_test(id1, id2) values " +
			"(1, 1), (2, 2) on duplicate key update id1 = 1",
		"insert into upsert_test(id1, id2) select eid, id " +
			"from vtocc_a limit 1 on duplicate key update id2 = id1",
		"insert into upsert_test(id1, id2) values " +
			"(1, 1) on duplicate key update id1 = 2+1",
	}

	// Strict mode on.
	func() {
		client := framework.NewDefaultClient()
		err := client.Begin()
		if err != nil {
			t.Error(err)
			return
		}
		defer client.Rollback()

		want := "error: DML too complex"
		for _, query := range queries {
			_, err = client.Execute(query, nil)
			if err == nil || err.Error() != want {
				t.Errorf("Execute(%s): %v, want %s", query, err, want)
			}
		}
	}()

	// Strict mode off.
	func() {
		framework.DefaultServer.SetStrictMode(false)
		defer framework.DefaultServer.SetStrictMode(true)

		for _, query := range queries {
			client := framework.NewDefaultClient()
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

func TestUpsertNonPKHit(t *testing.T) {
	client := framework.NewDefaultClient()
	err := client.Begin()
	if err != nil {
		t.Error(err)
		return
	}
	defer client.Rollback()

	_, err = client.Execute("insert into upsert_test(id1, id2) values (1, 1)", nil)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = client.Execute(
		"insert into upsert_test(id1, id2) values "+
			"(2, 1) on duplicate key update id2 = 2",
		nil,
	)
	want := "error: Duplicate entry '1' for key 'id2_idx'"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Execute: %v, must start with %s", err, want)
	}
}

func TestPoolSize(t *testing.T) {
	vstart := framework.DebugVars()
	defer framework.DefaultServer.SetPoolSize(framework.DefaultServer.PoolSize())
	framework.DefaultServer.SetPoolSize(1)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		framework.NewDefaultClient().Execute("select sleep(1) from dual", nil)
		wg.Done()
	}()
	// The queries have to be different so consolidator doesn't kick in.
	go func() {
		framework.NewDefaultClient().Execute("select sleep(0.5) from dual", nil)
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

func TestSchemaReload(t *testing.T) {
	conn, err := mysql.Connect(connParams)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = conn.ExecuteFetch("create table vtocc_temp(intval int)", 10, false)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		_, _ = conn.ExecuteFetch("drop table vtocc_temp", 10, false)
		conn.Close()
	}()
	framework.DefaultServer.ReloadSchema()
	client := framework.NewDefaultClient()
	waitTime := 50 * time.Millisecond
	for i := 0; i < 10; i++ {
		time.Sleep(waitTime)
		waitTime += 50 * time.Millisecond
		_, err = client.Execute("select * from vtocc_temp", nil)
		if err == nil {
			return
		}
		want := "error: table vtocc_temp not found in schema"
		if err.Error() != want {
			t.Errorf("Error: %v, want %s", err, want)
			return
		}
	}
	t.Error("schema did not reload")
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
