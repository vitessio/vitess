// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"reflect"
	"testing"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/schema"
)

func TestMessagerEngineState(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TransactionCap = 1
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()

	me := NewMessagerEngine(tsv.qe)
	if err := me.Open(); err != nil {
		t.Fatal(err)
	}
	if l := len(tsv.qe.schemaInfo.notifiers); l != 1 {
		t.Errorf("len(notifiers): %d, want 1", l)
	}
	if err := me.Open(); err != nil {
		t.Fatal(err)
	}
	if l := len(tsv.qe.schemaInfo.notifiers); l != 1 {
		t.Errorf("len(notifiers) after reopen: %d, want 1", l)
	}

	me.Close()
	if l := len(tsv.qe.schemaInfo.notifiers); l != 0 {
		t.Errorf("len(notifiers) after close: %d, want 0", l)
	}

	me.Close()
	if l := len(tsv.qe.schemaInfo.notifiers); l != 0 {
		t.Errorf("len(notifiers) after close: %d, want 0", l)
	}
}

func TestMessagerEngineSchemaChanged(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TransactionCap = 1
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()

	me := NewMessagerEngine(tsv.qe)
	if err := me.Open(); err != nil {
		t.Fatal(err)
	}
	tables := map[string]*schema.Table{
		"t1": {
			Type: schema.Message,
		},
		"t2": {
			Type: schema.NoType,
		},
	}
	me.schemaChanged(tables)
	want := []string{"t1"}
	if !reflect.DeepEqual(me.msgTables, want) {
		t.Errorf("me.msgTables: %+v, want %+v", me.msgTables, want)
	}
	tables = map[string]*schema.Table{
		"t1": {
			Type: schema.Message,
		},
		"t2": {
			Type: schema.NoType,
		},
		"t3": {
			Type: schema.Message,
		},
	}
	me.schemaChanged(tables)
	want = []string{"t1", "t3"}
	if !reflect.DeepEqual(me.msgTables, want) {
		t.Errorf("me.msgTables: %+v, want %+v", me.msgTables, want)
	}
	tables = map[string]*schema.Table{
		"t1": {
			Type: schema.Message,
		},
		"t2": {
			Type: schema.NoType,
		},
		"t4": {
			Type: schema.Message,
		},
	}
	me.schemaChanged(tables)
	// schemaChanged is only additive.
	want = []string{"t1", "t3", "t4"}
	if !reflect.DeepEqual(me.msgTables, want) {
		t.Errorf("me.msgTables: %+v, want %+v", me.msgTables, want)
	}
}
