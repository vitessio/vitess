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

	"vitess.io/vitess/go/sqltypes"
)

func TestMessagerCacheOrder(t *testing.T) {
	mc := newCache(10)
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}) {
		t.Fatal("Add returned false")
	}
	if !mc.Add(&MessageRow{
		TimeNext: 2,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row02")},
	}) {
		t.Fatal("Add returned false")
	}
	if !mc.Add(&MessageRow{
		TimeNext: 2,
		Epoch:    1,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row12")},
	}) {
		t.Fatal("Add returned false")
	}
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    1,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row11")},
	}) {
		t.Fatal("Add returned false")
	}
	if !mc.Add(&MessageRow{
		TimeNext: 3,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row03")},
	}) {
		t.Fatal("Add returned false")
	}
	var rows []string
	for i := 0; i < 5; i++ {
		rows = append(rows, mc.Pop().Row[0].ToString())
	}
	want := []string{
		"row03",
		"row02",
		"row01",
		"row12",
		"row11",
	}
	if !reflect.DeepEqual(rows, want) {
		t.Errorf("Pop order: %+v, want %+v", rows, want)
	}
}

func TestMessagerCacheDupKey(t *testing.T) {
	mc := newCache(10)
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}) {
		t.Fatal("Add returned false")
	}
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}) {
		t.Error("Add(dup): returned false, want true")
	}
	_ = mc.Pop()
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}) {
		t.Error("Add(dup): returned false, want true")
	}
	mc.Discard([]string{"row01"})
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}) {
		t.Fatal("Add returned false")
	}
}

func TestMessagerCacheDiscard(t *testing.T) {
	mc := newCache(10)
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}) {
		t.Fatal("Add returned false")
	}
	mc.Discard([]string{"row01"})
	if row := mc.Pop(); row != nil {
		t.Errorf("Pop: want nil, got %v", row.Row[0])
	}
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}) {
		t.Fatal("Add returned false")
	}
	if row := mc.Pop(); row == nil || row.Row[0].ToString() != "row01" {
		t.Errorf("Pop: want row01, got %v", row)
	}

	// Add will be a no-op.
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}) {
		t.Fatal("Add returned false")
	}
	if row := mc.Pop(); row != nil {
		t.Errorf("Pop: want nil, got %v", row.Row[0])
	}
	mc.Discard([]string{"row01"})

	// Now we can add.
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}) {
		t.Fatal("Add returned false")
	}
	if row := mc.Pop(); row == nil || row.Row[0].ToString() != "row01" {
		t.Errorf("Pop: want row01, got %v", row)
	}
}

func TestMessagerCacheFull(t *testing.T) {
	mc := newCache(2)
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}) {
		t.Fatal("Add returned false")
	}
	if !mc.Add(&MessageRow{
		TimeNext: 2,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row02")},
	}) {
		t.Fatal("Add returned false")
	}
	if mc.Add(&MessageRow{
		TimeNext: 2,
		Epoch:    1,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row12")},
	}) {
		t.Error("Add(full): returned true, want false")
	}
}

func TestMessagerCacheEmpty(t *testing.T) {
	mc := newCache(2)
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}) {
		t.Fatal("Add returned false")
	}
	mc.Clear()
	if row := mc.Pop(); row != nil {
		t.Errorf("Pop(empty): %v, want nil", row)
	}
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}) {
		t.Fatal("Add returned false")
	}
	if row := mc.Pop(); row == nil {
		t.Errorf("Pop(non-empty): nil, want %v", row)
	}
}
