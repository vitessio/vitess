// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package messager

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
)

func TestMessagerCacheOrder(t *testing.T) {
	mc := newCache(10)
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		ID:       sqltypes.MakeString([]byte("row01")),
	}) {
		t.Fatal("Add returned false")
	}
	if !mc.Add(&MessageRow{
		TimeNext: 2,
		Epoch:    0,
		ID:       sqltypes.MakeString([]byte("row02")),
	}) {
		t.Fatal("Add returned false")
	}
	if !mc.Add(&MessageRow{
		TimeNext: 2,
		Epoch:    1,
		ID:       sqltypes.MakeString([]byte("row12")),
	}) {
		t.Fatal("Add returned false")
	}
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    1,
		ID:       sqltypes.MakeString([]byte("row11")),
	}) {
		t.Fatal("Add returned false")
	}
	if !mc.Add(&MessageRow{
		TimeNext: 3,
		Epoch:    0,
		ID:       sqltypes.MakeString([]byte("row03")),
	}) {
		t.Fatal("Add returned false")
	}
	var rows []string
	for i := 0; i < 5; i++ {
		rows = append(rows, mc.Pop().ID.String())
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
		ID:       sqltypes.MakeString([]byte("row01")),
	}) {
		t.Fatal("Add returned false")
	}
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		ID:       sqltypes.MakeString([]byte("row01")),
	}) {
		t.Error("Add(dup): returned false, want true")
	}
	_ = mc.Pop()
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		ID:       sqltypes.MakeString([]byte("row01")),
	}) {
		t.Error("Add(dup): returned false, want true")
	}
	mc.Discard([]string{"row01"})
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		ID:       sqltypes.MakeString([]byte("row01")),
	}) {
		t.Fatal("Add returned false")
	}
}

func TestMessagerCacheDiscard(t *testing.T) {
	mc := newCache(10)
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		ID:       sqltypes.MakeString([]byte("row01")),
	}) {
		t.Fatal("Add returned false")
	}
	mc.Discard([]string{"row01"})
	if row := mc.Pop(); row != nil {
		t.Errorf("Pop: want nil, got %s", row.ID.String())
	}
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		ID:       sqltypes.MakeString([]byte("row01")),
	}) {
		t.Fatal("Add returned false")
	}
	if row := mc.Pop(); row == nil || row.ID.String() != "row01" {
		t.Errorf("Pop: want row01, got %v", row)
	}

	// Add will be a no-op.
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		ID:       sqltypes.MakeString([]byte("row01")),
	}) {
		t.Fatal("Add returned false")
	}
	if row := mc.Pop(); row != nil {
		t.Errorf("Pop: want nil, got %s", row.ID.String())
	}
	mc.Discard([]string{"row01"})

	// Now we can add.
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		ID:       sqltypes.MakeString([]byte("row01")),
	}) {
		t.Fatal("Add returned false")
	}
	if row := mc.Pop(); row == nil || row.ID.String() != "row01" {
		t.Errorf("Pop: want row01, got %v", row)
	}
}

func TestMessagerCacheFull(t *testing.T) {
	mc := newCache(2)
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		ID:       sqltypes.MakeString([]byte("row01")),
	}) {
		t.Fatal("Add returned false")
	}
	if !mc.Add(&MessageRow{
		TimeNext: 2,
		Epoch:    0,
		ID:       sqltypes.MakeString([]byte("row02")),
	}) {
		t.Fatal("Add returned false")
	}
	if mc.Add(&MessageRow{
		TimeNext: 2,
		Epoch:    1,
		ID:       sqltypes.MakeString([]byte("row12")),
	}) {
		t.Error("Add(full): returned true, want false")
	}
}

func TestMessagerCacheEmpty(t *testing.T) {
	mc := newCache(2)
	if !mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		ID:       sqltypes.MakeString([]byte("row01")),
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
		ID:       sqltypes.MakeString([]byte("row01")),
	}) {
		t.Fatal("Add returned false")
	}
	if row := mc.Pop(); row == nil {
		t.Errorf("Pop(non-empty): nil, want %v", row)
	}
}
