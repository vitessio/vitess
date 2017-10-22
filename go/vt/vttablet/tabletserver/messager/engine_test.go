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

package messager

import (
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql/fakesqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vterrors"
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
	tables := map[string]*schema.Table{
		"t1": meTable,
		"t2": meTable,
	}
	engine.schemaChanged(tables, []string{"t1", "t2"}, nil, nil)
	f1, ch1 := newEngineReceiver()
	f2, ch2 := newEngineReceiver()
	// Each receiver is subscribed to different managers.
	engine.Subscribe(context.Background(), "t1", f1)
	<-ch1
	engine.Subscribe(context.Background(), "t2", f2)
	<-ch2
	engine.managers["t1"].Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("1")}})
	engine.managers["t2"].Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("2")}})
	<-ch1
	<-ch2

	// Error case.
	want := "message table t3 not found"
	_, err := engine.Subscribe(context.Background(), "t3", f1)
	if err == nil || err.Error() != want {
		t.Errorf("Subscribe: %v, want %s", err, want)
	}

	// After close, Subscribe should return a closed channel.
	engine.Close()
	_, err = engine.Subscribe(context.Background(), "t1", nil)
	if got, want := vterrors.Code(err), vtrpcpb.Code_UNAVAILABLE; got != want {
		t.Errorf("Subscribed on closed engine error code: %v, want %v", got, want)
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
	engine.Subscribe(context.Background(), "t1", f1)
	<-ch1

	row1 := &MessageRow{
		Row: []sqltypes.Value{sqltypes.NewVarBinary("1")},
	}
	row2 := &MessageRow{
		TimeNext: time.Now().UnixNano() + int64(10*time.Minute),
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("2")},
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
	engine.Subscribe(context.Background(), "t2", func(qr *sqltypes.Result) error {
		count.Add(1)
		ch2 <- qr
		return nil
	})
	<-ch2
	mm := engine.managers["t2"]
	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("1")}})
	// Make sure the message is enqueued.
	for {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
		if count.Get() == int64(2) {
			break
		}
	}
	// "2" will be in the cache.
	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("2")}})
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

func TestGenerateLoadMessagesQuery(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	engine := newTestEngine(db)
	defer engine.Close()

	table := *meTable
	table.Name = sqlparser.NewTableIdent("t1")
	engine.schemaChanged(map[string]*schema.Table{
		"t1": &table,
	}, []string{"t1"}, nil, nil)

	q, err := engine.GenerateLoadMessagesQuery("t1")
	if err != nil {
		t.Error(err)
	}
	want := "select time_next, epoch, time_created, id, time_scheduled, message from t1 where :#pk"
	if q.Query != want {
		t.Errorf("GenerateLoadMessagesQuery: %s, want %s", q.Query, want)
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
	te.InitDBConfig(dbconfigs.DBConfigs{
		App:           *db.ConnParams(),
		SidecarDBName: "_vt",
	})
	te.Open()
	return te
}

func newEngineReceiver() (f func(qr *sqltypes.Result) error, ch chan *sqltypes.Result) {
	ch = make(chan *sqltypes.Result)
	return func(qr *sqltypes.Result) error {
		ch <- qr
		return nil
	}, ch
}
