// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"fmt"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/tabletserver/endtoend/framework"
)

func TestUncacheableTables(t *testing.T) {
	client := framework.NewClient()

	nocacheTables := []struct {
		name   string
		create string
		drop   string
	}{{
		create: "create table vitess_nocache(eid int, primary key (eid)) comment 'vitess_nocache'",
		drop:   "drop table vitess_nocache",
	}, {
		create: "create table vitess_nocache(somecol int)",
		drop:   "drop table vitess_nocache",
	}, {
		create: "create table vitess_nocache(charcol varchar(10), primary key(charcol))",
		drop:   "drop table vitess_nocache",
	}}
	for _, tcase := range nocacheTables {
		_, err := client.Execute(tcase.create, nil)
		if err != nil {
			t.Error(err)
			return
		}
		table, ok := framework.DebugSchema()["vitess_nocache"]
		client.Execute(tcase.drop, nil)
		if !ok {
			t.Errorf("%s: table vitess_nocache not found in schema", tcase.create)
			continue
		}
		if table.Type != schema.CacheNone {
			t.Errorf("Type: %d, want %d", table.Type, schema.CacheNone)
		}
	}
}

func TestOverrideTables(t *testing.T) {
	testCases := []struct {
		table     string
		cacheType int
	}{{
		table:     "vitess_cached2",
		cacheType: schema.CacheRW,
	}, {
		table:     "vitess_view",
		cacheType: schema.CacheRW,
	}, {
		table:     "vitess_part1",
		cacheType: schema.CacheW,
	}, {
		table:     "vitess_part2",
		cacheType: schema.CacheW,
	}}
	for _, tcase := range testCases {
		table, ok := framework.DebugSchema()[tcase.table]
		if !ok {
			t.Errorf("Table %s not found in schema", tcase.table)
			return
		}
		if table.Type != tcase.cacheType {
			t.Errorf("Type: %d, want %d", table.Type, tcase.cacheType)
		}
	}
}

func TestCacheDisallows(t *testing.T) {
	client := framework.NewClient()
	testCases := []struct {
		query string
		bv    map[string]interface{}
		err   string
	}{{
		query: "select bid, eid from vitess_cached2 where eid = 1 and bid = 1",
		err:   "error: type mismatch",
	}, {
		query: "select * from vitess_cached2 where eid = 2 and bid = 'foo' limit :a",
		bv:    map[string]interface{}{"a": -1},
		err:   "error: negative limit",
	}}
	for _, tcase := range testCases {
		_, err := client.Execute(tcase.query, tcase.bv)
		if err == nil || !strings.HasPrefix(err.Error(), tcase.err) {
			t.Errorf("Error: %v, want %s", err, tcase.err)
			return
		}
	}
}

func TestCacheListArgs(t *testing.T) {
	client := framework.NewClient()
	query := "select * from vitess_cached1 where eid in ::list"
	successCases := []struct {
		bv       map[string]interface{}
		rowcount uint64
	}{{
		bv:       map[string]interface{}{"list": []interface{}{3, 4, 32768}},
		rowcount: 2,
	}, {
		bv:       map[string]interface{}{"list": []interface{}{3, 4}},
		rowcount: 2,
	}, {
		bv:       map[string]interface{}{"list": []interface{}{3}},
		rowcount: 1,
	}}
	for _, success := range successCases {
		qr, err := client.Execute(query, success.bv)
		if err != nil {
			t.Error(err)
			continue
		}
		if qr.RowsAffected != success.rowcount {
			t.Errorf("RowsAffected: %d, want %d", qr.RowsAffected, success.rowcount)
		}
	}

	_, err := client.Execute(query, map[string]interface{}{"list": []interface{}{}})
	want := "error: empty list supplied"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Error: %v, want %s", err, want)
		return
	}
}

func verifyvitessCached2(t *testing.T, table string) error {
	client := framework.NewClient()
	query := fmt.Sprintf("select * from %s where eid = 2 and bid = 'foo'", table)
	_, err := client.Execute(query, nil)
	if err != nil {
		return err
	}
	tstart := framework.TableStats()[table]
	_, err = client.Execute(query, nil)
	if err != nil {
		return err
	}
	tend := framework.TableStats()[table]
	if tend.Hits != tstart.Hits+1 {
		return fmt.Errorf("Hits: %d, want %d", tend.Hits, tstart.Hits+1)
	}
	return nil
}

func TestUncache(t *testing.T) {
	// Verify rowcache is working vitess_cached2
	err := verifyvitessCached2(t, "vitess_cached2")
	if err != nil {
		t.Error(err)
		return
	}

	// Disable rowcache for vitess_cached2
	client := framework.NewClient()
	_, err = client.Execute("alter table vitess_cached2 comment 'vitess_nocache'", nil)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = client.Execute("select * from vitess_cached2 where eid = 2 and bid = 'foo'", nil)
	if err != nil {
		t.Error(err)
	}
	if tstat, ok := framework.TableStats()["vitess_cached2"]; ok {
		t.Errorf("table stats was found: %v, want not found", tstat)
	}

	// Re-enable rowcache and verify it's working
	_, err = client.Execute("alter table vitess_cached2 comment ''", nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = verifyvitessCached2(t, "vitess_cached2")
	if err != nil {
		t.Error(err)
		return
	}
}

func TestRename(t *testing.T) {
	// Verify rowcache is working vitess_cached2
	err := verifyvitessCached2(t, "vitess_cached2")
	if err != nil {
		t.Error(err)
		return
	}

	// Rename & test
	client := framework.NewClient()
	_, err = client.Execute("alter table vitess_cached2 rename to vitess_renamed", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if tstat, ok := framework.TableStats()["vitess_cached2"]; ok {
		t.Errorf("table stats was found: %v, want not found", tstat)
	}

	err = verifyvitessCached2(t, "vitess_renamed")
	if err != nil {
		t.Error(err)
		return
	}

	// Rename back & verify
	_, err = client.Execute("rename table vitess_renamed to vitess_cached2", nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = verifyvitessCached2(t, "vitess_cached2")
	if err != nil {
		t.Error(err)
		return
	}
}

func TestSpotCheck(t *testing.T) {
	vstart := framework.DebugVars()
	client := framework.NewClient()
	_, err := client.Execute("select * from vitess_cached2 where eid = 2 and bid = 'foo'", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if err := compareIntDiff(framework.DebugVars(), "RowcacheSpotCheckCount", vstart, 0); err != nil {
		t.Error(err)
	}

	defer framework.Server.SetSpotCheckRatio(framework.Server.SpotCheckRatio())
	framework.Server.SetSpotCheckRatio(1)
	if err := verifyIntValue(framework.DebugVars(), "RowcacheSpotCheckRatio", 1); err != nil {
		t.Error(err)
	}

	vstart = framework.DebugVars()
	_, err = client.Execute("select * from vitess_cached2 where eid = 2 and bid = 'foo'", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if err := compareIntDiff(framework.DebugVars(), "RowcacheSpotCheckCount", vstart, 1); err != nil {
		t.Error(err)
	}

	vstart = framework.DebugVars()
	_, err = client.Execute("select * from vitess_cached1 where eid in (9)", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if err := compareIntDiff(framework.DebugVars(), "RowcacheSpotCheckCount", vstart, 0); err != nil {
		t.Error(err)
	}
	_, err = client.Execute("select * from vitess_cached1 where eid in (9)", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if err := compareIntDiff(framework.DebugVars(), "RowcacheSpotCheckCount", vstart, 1); err != nil {
		t.Error(err)
	}
}

func TestCacheTypes(t *testing.T) {
	client := framework.NewClient()
	badRequests := []struct {
		query string
		bv    map[string]interface{}
		out   string
	}{{
		query: "select * from vitess_cached2 where eid = 'str' and bid = 'str'",
		out:   "error: strconv.ParseInt",
	}, {
		query: "select * from vitess_cached2 where eid = :str and bid = :str",
		bv:    map[string]interface{}{"str": "str"},
		out:   "error: strconv.ParseInt",
	}, {
		query: "select * from vitess_cached2 where eid = 1 and bid = 1",
		out:   "error: type mismatch",
	}, {
		query: "select * from vitess_cached2 where eid = :id and bid = :id",
		bv:    map[string]interface{}{"id": 1},
		out:   "error: type mismatch",
	}, {
		query: "select * from vitess_cached2 where eid = 1.2 and bid = 1.2",
		out:   "error: type mismatch",
	}, {
		query: "select * from vitess_cached2 where eid = :fl and bid = :fl",
		bv:    map[string]interface{}{"fl": 1.2},
		out:   "error: type mismatch",
	}}
	for _, tcase := range badRequests {
		_, err := client.Execute(tcase.query, tcase.bv)
		if err == nil || !strings.HasPrefix(err.Error(), tcase.out) {
			t.Errorf("%s: %v, want %s", tcase.query, err, tcase.out)
		}
	}
}

func TestNoData(t *testing.T) {
	qr, err := framework.NewClient().Execute("select * from vitess_cached2 where eid = 6 and name = 'bar'", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 0 {
		t.Errorf("RowsAffected: %d, want 0", qr.RowsAffected)
	}
}

func TestCacheStats(t *testing.T) {
	client := framework.NewClient()
	query := "select * from vitess_cached2 where eid = 2 and bid = 'foo'"
	_, err := client.Execute(query, nil)
	if err != nil {
		t.Error(err)
		return
	}
	vstart := framework.DebugVars()
	_, err = client.Execute(query, nil)
	if err != nil {
		t.Error(err)
		return
	}
	if err := compareIntDiff(framework.DebugVars(), "RowcacheStats/vitess_cached2.Hits", vstart, 1); err != nil {
		t.Error(err)
	}

	vstart = framework.DebugVars()
	_, err = client.Execute("update vitess_part2 set data2 = 2 where key3 = 1", nil)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = client.Execute("select * from vitess_view where key2 = 1", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if err := compareIntDiff(framework.DebugVars(), "RowcacheStats/vitess_view.Misses", vstart, 1); err != nil {
		t.Error(err)
	}
}
