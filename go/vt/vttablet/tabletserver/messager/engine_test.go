/*
Copyright 2019 The Vitess Authors.

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
	"reflect"
	"testing"

	"context"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

var meTable = &schema.Table{
	Type:        schema.Message,
	MessageInfo: newMMTable().MessageInfo,
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
	want = map[string]bool{"t1": true, "t4": true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got: %+v, want %+v", got, want)
	}
	// Test update
	tables = map[string]*schema.Table{
		"t1": meTable,
		"t2": meTable,
		"t4": {
			Type: schema.NoType,
		},
	}
	engine.schemaChanged(tables, nil, []string{"t2", "t4"}, nil)
	got = extractManagerNames(engine.managers)
	want = map[string]bool{"t1": true, "t2": true}
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
	config := tabletenv.NewDefaultConfig()
	tsv := &fakeTabletServer{
		Env: tabletenv.NewEnv(config, "MessagerTest"),
	}
	se := schema.NewEngine(tsv)
	te := NewEngine(tsv, se, newFakeVStreamer())
	te.Open()
	return te
}

func newEngineReceiver() (f func(qr *sqltypes.Result) error, ch chan *sqltypes.Result) {
	ch = make(chan *sqltypes.Result, 1)
	return func(qr *sqltypes.Result) error {
		ch <- qr
		return nil
	}, ch
}
