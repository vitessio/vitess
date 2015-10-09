// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"strings"
	"testing"

	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/tabletserver/endtoend/framework"
)

func TestUncacheableTables(t *testing.T) {
	client := framework.NewDefaultClient()

	nocacheTables := []struct {
		name   string
		create string
		drop   string
	}{{
		create: "create table vtocc_nocache(eid int, primary key (eid)) comment 'vtocc_nocache'",
		drop:   "drop table vtocc_nocache",
	}, {
		create: "create table vtocc_nocache(somecol int)",
		drop:   "drop table vtocc_nocache",
	}, {
		create: "create table vtocc_nocache(charcol varchar(10), primary key(charcol))",
		drop:   "drop table vtocc_nocache",
	}}
	for _, tcase := range nocacheTables {
		_, err := client.Execute(tcase.create, nil)
		if err != nil {
			t.Error(err)
			return
		}
		table, ok := framework.DebugSchema()["vtocc_nocache"]
		client.Execute(tcase.drop, nil)
		if !ok {
			t.Errorf("%s: table vtocc_nocache not found in schema", tcase.create)
			continue
		}
		if table.CacheType != schema.CACHE_NONE {
			t.Errorf("CacheType: %d, want %d", table.CacheType, schema.CACHE_NONE)
		}
	}
}

func TestOverrideTables(t *testing.T) {
	testCases := []struct {
		table     string
		cacheType int
	}{{
		table:     "vtocc_cached2",
		cacheType: schema.CACHE_RW,
	}, {
		table:     "vtocc_view",
		cacheType: schema.CACHE_RW,
	}, {
		table:     "vtocc_part1",
		cacheType: schema.CACHE_W,
	}, {
		table:     "vtocc_part2",
		cacheType: schema.CACHE_W,
	}}
	for _, tcase := range testCases {
		table, ok := framework.DebugSchema()[tcase.table]
		if !ok {
			t.Errorf("Table %s not found in schema", tcase.table)
			return
		}
		if table.CacheType != tcase.cacheType {
			t.Errorf("CacheType: %d, want %d", table.CacheType, tcase.cacheType)
		}
	}
}

func TestCacheDisallows(t *testing.T) {
	client := framework.NewDefaultClient()
	testCases := []struct {
		query string
		bv    map[string]interface{}
		err   string
	}{{
		query: "select bid, eid from vtocc_cached2 where eid = 1 and bid = 1",
		err:   "error: type mismatch",
	}, {
		query: "select * from vtocc_cached2 where eid = 2 and bid = 'foo' limit :a",
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
	client := framework.NewDefaultClient()
	query := "select * from vtocc_cached1 where eid in ::list"
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
