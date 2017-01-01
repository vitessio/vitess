// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"reflect"
	"testing"
)

func TestMessagerCacheOrder(t *testing.T) {
	mc := NewMessagerCache(10)
	if err := mc.Add(&MessageRow{
		TimeScheduled: 1,
		Epoch:         0,
		id:            "row01",
	}); err != nil {
		t.Fatal(err)
	}
	if err := mc.Add(&MessageRow{
		TimeScheduled: 2,
		Epoch:         0,
		id:            "row02",
	}); err != nil {
		t.Fatal(err)
	}
	if err := mc.Add(&MessageRow{
		TimeScheduled: 2,
		Epoch:         1,
		id:            "row12",
	}); err != nil {
		t.Fatal(err)
	}
	if err := mc.Add(&MessageRow{
		TimeScheduled: 1,
		Epoch:         1,
		id:            "row11",
	}); err != nil {
		t.Fatal(err)
	}
	if err := mc.Add(&MessageRow{
		TimeScheduled: 3,
		Epoch:         0,
		id:            "row03",
	}); err != nil {
		t.Fatal(err)
	}
	var rows []string
	for {
		row := mc.Pop()
		if row == nil {
			break
		}
		rows = append(rows, row.id)
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
	mc := NewMessagerCache(10)
	if err := mc.Add(&MessageRow{
		TimeScheduled: 1,
		Epoch:         0,
		id:            "row01",
	}); err != nil {
		t.Fatal(err)
	}
	want := "duplicate key"
	if err := mc.Add(&MessageRow{
		TimeScheduled: 1,
		Epoch:         0,
		id:            "row01",
	}); err == nil || err.Error() != want {
		t.Errorf("Add(dup): %v, want %s", err, want)
	}
	_ = mc.Pop()
	if err := mc.Add(&MessageRow{
		TimeScheduled: 1,
		Epoch:         0,
		id:            "row01",
	}); err == nil || err.Error() != want {
		t.Errorf("Add(dup): %v, want %s", err, want)
	}
	mc.Discard("row01")
	if err := mc.Add(&MessageRow{
		TimeScheduled: 1,
		Epoch:         0,
		id:            "row01",
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMessagerCacheFull(t *testing.T) {
	mc := NewMessagerCache(2)
	if err := mc.Add(&MessageRow{
		TimeScheduled: 1,
		Epoch:         0,
		id:            "row01",
	}); err != nil {
		t.Fatal(err)
	}
	if err := mc.Add(&MessageRow{
		TimeScheduled: 2,
		Epoch:         0,
		id:            "row02",
	}); err != nil {
		t.Fatal(err)
	}
	want := "queue is full"
	if err := mc.Add(&MessageRow{
		TimeScheduled: 2,
		Epoch:         1,
		id:            "row12",
	}); err == nil || err.Error() != want {
		t.Errorf("Add(full): %v, want %s", err, want)
	}
}
