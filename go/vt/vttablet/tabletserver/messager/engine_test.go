// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package messager

import (
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/youtube/vitess/go/mysqlconn/fakesqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

var meTable = &schema.Table{
	Type:        schema.Message,
	MessageInfo: mmTable.MessageInfo,
}

func TestEngineSchemaChanged(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	engine := newTestEngine(db)
	defer engine.Close()
	tables := map[string]*schema.Table{
		"t1": meTable,
		"t2": {
			Type: schema.NoType,
		},
	}
	engine.schemaChanged(tables, []string{"t1", "t2"}, nil, nil)
	got := extractManagerNames(engine.managers)
	want := map[string]bool{"t1": true}
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
	engine.schemaChanged(tables, []string{"t3"}, nil, nil)
	got = extractManagerNames(engine.managers)
	want = map[string]bool{"t1": true, "t3": true}
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
	engine.schemaChanged(tables, []string{"t4"}, nil, []string{"t3", "t5"})
	got = extractManagerNames(engine.managers)
	// schemaChanged is only additive.
	want = map[string]bool{"t1": true, "t4": true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got: %+v, want %+v", got, want)
	}
}

func extractManagerNames(in map[string]*messageManager) map[string]bool {
	out := make(map[string]bool)
	for k := range in {
		out[k] = true
	}
	return out
}

func TestSubscribe(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	engine := newTestEngine(db)
	defer engine.Close()
	tables := map[string]*schema.Table{
		"t1": meTable,
		"t2": meTable,
	}
	engine.schemaChanged(tables, []string{"t1", "t2"}, nil, nil)
	f1, ch1 := newEngineReceiver()
	f2, ch2 := newEngineReceiver()
	// Each receiver is subscribed to different managers.
	engine.Subscribe("t1", f1)
	<-ch1
	engine.Subscribe("t2", f2)
	<-ch2
	engine.managers["t1"].Add(&MessageRow{ID: sqltypes.MakeString([]byte("1"))})
	engine.managers["t2"].Add(&MessageRow{ID: sqltypes.MakeString([]byte("2"))})
	<-ch1
	<-ch2

	// Error case.
	want := "message table t3 not found"
	_, err := engine.Subscribe("t3", f1)
	if err == nil || err.Error() != want {
		t.Errorf("Subscribe: %v, want %s", err, want)
	}
}

func TestLockDB(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	engine := newTestEngine(db)
	defer engine.Close()
	tables := map[string]*schema.Table{
		"t1": meTable,
		"t2": meTable,
	}
	engine.schemaChanged(tables, []string{"t1", "t2"}, nil, nil)
	f1, ch1 := newEngineReceiver()
	engine.Subscribe("t1", f1)
	<-ch1

	row1 := &MessageRow{
		ID: sqltypes.MakeString([]byte("1")),
	}
	row2 := &MessageRow{
		TimeNext: time.Now().UnixNano() + int64(10*time.Minute),
		ID:       sqltypes.MakeString([]byte("2")),
	}
	newMessages := map[string][]*MessageRow{"t1": {row1, row2}, "t3": {row1}}
	unlock := engine.LockDB(newMessages, nil)
	engine.UpdateCaches(newMessages, nil)
	unlock()
	<-ch1
	runtime.Gosched()
	// row2 should not be sent.
	select {
	case mr := <-ch1:
		t.Errorf("Unexpected message: %v", mr)
	default:
	}

	ch2 := make(chan *sqltypes.Result)
	var count sync2.AtomicInt64
	engine.Subscribe("t2", func(qr *sqltypes.Result) error {
		count.Add(1)
		ch2 <- qr
		return nil
	})
	<-ch2
	mm := engine.managers["t2"]
	mm.Add(&MessageRow{ID: sqltypes.MakeString([]byte("1"))})
	// Make sure the message is enqueued.
	for {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
		if count.Get() == int64(2) {
			break
		}
	}
	// "2" will be in the cache.
	mm.Add(&MessageRow{ID: sqltypes.MakeString([]byte("2"))})
	changedMessages := map[string][]string{"t2": {"2"}, "t3": {"2"}}
	unlock = engine.LockDB(nil, changedMessages)
	// This should delete "2".
	engine.UpdateCaches(nil, changedMessages)
	unlock()
	<-ch2
	runtime.Gosched()
	// There should be no more messages.
	select {
	case mr := <-ch2:
		t.Errorf("Unexpected message: %v", mr)
	default:
	}
}

func TestEngineGenerate(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	engine := newTestEngine(db)
	defer engine.Close()
	engine.schemaChanged(map[string]*schema.Table{
		"t1": meTable,
	}, []string{"t1"}, nil, nil)
	if _, _, err := engine.GenerateAckQuery("t1", []string{"1"}); err != nil {
		t.Error(err)
	}
	want := "message table t2 not found in schema"
	if _, _, err := engine.GenerateAckQuery("t2", []string{"1"}); err == nil || err.Error() != want {
		t.Errorf("engine.GenerateAckQuery(invalid): %v, want %s", err, want)
	}

	if _, _, err := engine.GeneratePostponeQuery("t1", []string{"1"}); err != nil {
		t.Error(err)
	}
	if _, _, err := engine.GeneratePostponeQuery("t2", []string{"1"}); err == nil || err.Error() != want {
		t.Errorf("engine.GeneratePostponeQuery(invalid): %v, want %s", err, want)
	}

	if _, _, err := engine.GeneratePurgeQuery("t1", 0); err != nil {
		t.Error(err)
	}
	if _, _, err := engine.GeneratePurgeQuery("t2", 0); err == nil || err.Error() != want {
		t.Errorf("engine.GeneratePurgeQuery(invalid): %v, want %s", err, want)
	}
}

func newTestEngine(db *fakesqldb.DB) *Engine {
	randID := rand.Int63()
	config := tabletenv.DefaultQsConfig
	config.PoolNamePrefix = fmt.Sprintf("Pool-%d-", randID)
	tsv := newFakeTabletServer()
	se := schema.NewEngine(tsv, config)
	te := NewEngine(tsv, se, config)
	dbconfigs := dbconfigs.DBConfigs{
		App:           *db.ConnParams(),
		SidecarDBName: "_vt",
	}
	te.Open(dbconfigs)
	return te
}

func newEngineReceiver() (f func(qr *sqltypes.Result) error, ch chan *sqltypes.Result) {
	ch = make(chan *sqltypes.Result)
	return func(qr *sqltypes.Result) error {
		ch <- qr
		return nil
	}, ch
}
