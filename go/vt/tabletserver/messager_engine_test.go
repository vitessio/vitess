// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/tabletserver/engines/schema"
)

var meTable = &schema.Table{
	Type:        schema.Message,
	MessageInfo: mmTable.MessageInfo,
}

func TestMESchemaChanged(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TransactionCap = 1
	tsv := NewTabletServerWithNilTopoServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()

	me := tsv.messager
	tables := map[string]*schema.Table{
		"t1": meTable,
		"t2": {
			Type: schema.NoType,
		},
	}
	me.schemaChanged(tables, []string{"t1", "t2"}, nil, nil)
	got := extractManagerNames(me.managers)
	want := map[string]bool{"msg": true, "t1": true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got: %+v, want %+v", got, want)
	}
	tables = map[string]*schema.Table{
		"t1": meTable,
		"t2": {
			Type: schema.NoType,
		},
		"t3": meTable,
	}
	me.schemaChanged(tables, []string{"t3"}, nil, nil)
	got = extractManagerNames(me.managers)
	want = map[string]bool{"msg": true, "t1": true, "t3": true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got: %+v, want %+v", got, want)
	}
	tables = map[string]*schema.Table{
		"t1": meTable,
		"t2": {
			Type: schema.NoType,
		},
		"t4": meTable,
	}
	me.schemaChanged(tables, []string{"t4"}, nil, []string{"t3"})
	got = extractManagerNames(me.managers)
	// schemaChanged is only additive.
	want = map[string]bool{"msg": true, "t1": true, "t4": true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got: %+v, want %+v", got, want)
	}
}

func extractManagerNames(in map[string]*MessageManager) map[string]bool {
	out := make(map[string]bool)
	for k := range in {
		out[k] = true
	}
	return out
}

func TestSubscribe(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TransactionCap = 1
	tsv := NewTabletServerWithNilTopoServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()

	me := tsv.messager
	tables := map[string]*schema.Table{
		"t1": meTable,
		"t2": meTable,
	}
	me.schemaChanged(tables, []string{"t1", "t2"}, nil, nil)
	r1 := newTestReceiver(1)
	r2 := newTestReceiver(1)
	// Each receiver is subscribed to different managers.
	me.Subscribe("t1", r1.rcv)
	<-r1.ch
	me.Subscribe("t2", r2.rcv)
	<-r2.ch
	me.managers["t1"].Add(&MessageRow{ID: sqltypes.MakeString([]byte("1"))})
	me.managers["t2"].Add(&MessageRow{ID: sqltypes.MakeString([]byte("2"))})
	<-r1.ch
	<-r2.ch

	// Error case.
	want := "message table t3 not found"
	err = me.Subscribe("t3", r1.rcv)
	if err == nil || err.Error() != want {
		t.Errorf("Subscribe: %v, want %s", err, want)
	}
}

func TestLockDB(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TransactionCap = 1
	tsv := NewTabletServerWithNilTopoServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()

	me := tsv.messager
	tables := map[string]*schema.Table{
		"t1": meTable,
		"t2": meTable,
	}
	me.schemaChanged(tables, []string{"t1", "t2"}, nil, nil)
	r1 := newTestReceiver(0)
	me.Subscribe("t1", r1.rcv)
	<-r1.ch

	row1 := &MessageRow{
		ID: sqltypes.MakeString([]byte("1")),
	}
	row2 := &MessageRow{
		TimeNext: time.Now().UnixNano() + int64(10*time.Minute),
		ID:       sqltypes.MakeString([]byte("2")),
	}
	newMessages := map[string][]*MessageRow{"t1": {row1, row2}, "t3": {row1}}
	unlock := me.LockDB(newMessages, nil)
	me.UpdateCaches(newMessages, nil)
	unlock()
	<-r1.ch
	runtime.Gosched()
	// row2 should not be sent.
	select {
	case mr := <-r1.ch:
		t.Errorf("Unexpected message: %v", mr)
	default:
	}

	r2 := newTestReceiver(0)
	me.Subscribe("t2", r2.rcv)
	<-r2.ch
	mm := me.managers["t2"]
	mm.Add(&MessageRow{ID: sqltypes.MakeString([]byte("1"))})
	// Make sure the first message is enqueued.
	r2.WaitForCount(2)
	// "2" will be in the cache.
	mm.Add(&MessageRow{ID: sqltypes.MakeString([]byte("2"))})
	changedMessages := map[string][]string{"t2": {"2"}, "t3": {"2"}}
	unlock = me.LockDB(nil, changedMessages)
	// This should delete "2".
	me.UpdateCaches(nil, changedMessages)
	unlock()
	<-r2.ch
	runtime.Gosched()
	// There should be no more messages.
	select {
	case mr := <-r2.ch:
		t.Errorf("Unexpected message: %v", mr)
	default:
	}
}

func TestMESendDiscard(t *testing.T) {
	// This is a manual test because the discard happens
	// asynchronously, which makes the test flaky.
	t.Skip()
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TransactionCap = 1
	tsv := NewTabletServerWithNilTopoServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()

	me := tsv.messager
	r1 := newTestReceiver(0)
	me.Subscribe("msg", r1.rcv)
	<-r1.ch

	db.AddQuery(
		"select time_scheduled, id from msg where id in ('1') and time_acked is null limit 10001 for update",
		&sqltypes.Result{
			Fields: []*querypb.Field{
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
			},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{{
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("1")),
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("1")),
			}},
		},
	)
	db.AddQueryPattern("update msg set time_next = .*", &sqltypes.Result{RowsAffected: 1})
	me.managers["msg"].Add(&MessageRow{ID: sqltypes.MakeString([]byte("1"))})
	<-r1.ch
	for i := 0; i < 10; i++ {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
		if _, ok := me.managers["msg"].cache.messages["1"]; !ok {
			return
		}
	}
	t.Error("Message 1 is still present in cache")
}

func TestMEGenerate(t *testing.T) {
	db := setUpTabletServerTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TransactionCap = 1
	tsv := NewTabletServerWithNilTopoServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()

	me := tsv.messager
	me.schemaChanged(map[string]*schema.Table{
		"t1": meTable,
	}, []string{"t1"}, nil, nil)
	if _, _, err := me.GenerateAckQuery("t1", []string{"1"}); err != nil {
		t.Error(err)
	}
	want := "message table t2 not found in schema"
	if _, _, err := me.GenerateAckQuery("t2", []string{"1"}); err == nil || err.Error() != want {
		t.Errorf("me.GenerateAckQuery(invalid): %v, want %s", err, want)
	}

	if _, _, err := me.GeneratePostponeQuery("t1", []string{"1"}); err != nil {
		t.Error(err)
	}
	if _, _, err := me.GeneratePostponeQuery("t2", []string{"1"}); err == nil || err.Error() != want {
		t.Errorf("me.GeneratePostponeQuery(invalid): %v, want %s", err, want)
	}

	if _, _, err := me.GeneratePurgeQuery("t1", 0); err != nil {
		t.Error(err)
	}
	if _, _, err := me.GeneratePurgeQuery("t2", 0); err == nil || err.Error() != want {
		t.Errorf("me.GeneratePurgeQuery(invalid): %v, want %s", err, want)
	}
}
