// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package events

import (
	"testing"

	"github.com/youtube/vitess/go/event"
)

type testEvent struct {
	StatusUpdater
}

func TestUpdateInit(t *testing.T) {
	want := "status"
	ev := &testEvent{}
	ev.Update("status")

	if ev.Status != want {
		t.Errorf("ev.Status = %#v, want %#v", ev.Status, want)
	}
	if ev.EventID == 0 {
		t.Errorf("ev.EventID wasn't initialized")
	}
}

func TestUpdateEventID(t *testing.T) {
	want := int64(12345)
	ev := &testEvent{}
	ev.EventID = 12345

	ev.Update("status")

	if ev.EventID != want {
		t.Errorf("ev.EventID = %v, want %v", ev.EventID, want)
	}
}

func TestUpdateDispatch(t *testing.T) {
	triggered := false
	event.AddListener(func(ev *testEvent) {
		triggered = true
	})

	want := "status"
	ev := &testEvent{}
	event.DispatchUpdate(ev, "status")

	if ev.Status != want {
		t.Errorf("ev.Status = %#v, want %#v", ev.Status, want)
	}
	if !triggered {
		t.Errorf("listener wasn't triggered on Dispatch()")
	}
}
