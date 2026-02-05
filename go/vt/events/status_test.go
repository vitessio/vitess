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

package events

import (
	"testing"

	"vitess.io/vitess/go/event"
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
