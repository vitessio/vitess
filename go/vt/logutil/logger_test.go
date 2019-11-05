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

package logutil

import (
	"testing"
	"time"

	"vitess.io/vitess/go/race"
	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
)

func TestLogEvent(t *testing.T) {
	testValues := []struct {
		event    *logutilpb.Event
		expected string
	}{
		{
			event: &logutilpb.Event{
				Time:  TimeToProto(time.Date(2014, time.November, 10, 23, 30, 12, 123456000, time.UTC)),
				Level: logutilpb.Level_INFO,
				File:  "file.go",
				Line:  123,
				Value: "message",
			},
			expected: "I1110 23:30:12.123456 file.go:123] message",
		},
		{
			event: &logutilpb.Event{
				Time:  TimeToProto(time.Date(2014, time.January, 20, 23, 30, 12, 0, time.UTC)),
				Level: logutilpb.Level_WARNING,
				File:  "file2.go",
				Line:  567,
				Value: "message %v %v",
			},
			expected: "W0120 23:30:12.000000 file2.go:567] message %v %v",
		},
		{
			event: &logutilpb.Event{
				Time:  TimeToProto(time.Date(2014, time.January, 20, 23, 30, 12, 0, time.UTC)),
				Level: logutilpb.Level_ERROR,
				File:  "file2.go",
				Line:  567,
				Value: "message %v %v",
			},
			expected: "E0120 23:30:12.000000 file2.go:567] message %v %v",
		},
		{
			event: &logutilpb.Event{
				Time:  TimeToProto(time.Date(2014, time.January, 20, 23, 30, 12, 0, time.UTC)),
				Level: logutilpb.Level_CONSOLE,
				File:  "file2.go",
				Line:  567,
				Value: "message %v %v",
			},
			expected: "message %v %v",
		},
	}
	ml := NewMemoryLogger()
	for i, testValue := range testValues {
		LogEvent(ml, testValue.event)
		if got, want := ml.Events[i].Value, testValue.expected; got != want {
			t.Errorf("ml.Events[%v].Value = %q, want %q", i, got, want)
		}
		// Skip the check below if go test -race is run because then the stack
		// is shifted by one and the test would fail.
		if !race.Enabled {
			if got, want := ml.Events[i].File, "logger_test.go"; got != want && ml.Events[i].Level != logutilpb.Level_CONSOLE {
				t.Errorf("ml.Events[%v].File = %q (line = %v), want %q", i, got, ml.Events[i].Line, want)
			}
		}
	}
}

func TestMemoryLogger(t *testing.T) {
	ml := NewMemoryLogger()
	ml.Infof("test %v", 123)
	if got, want := len(ml.Events), 1; got != want {
		t.Fatalf("len(ml.Events) = %v, want %v", got, want)
	}
	if got, want := ml.Events[0].File, "logger_test.go"; got != want {
		t.Errorf("ml.Events[0].File = %q, want %q", got, want)
	}
	ml.Warningf("test %v", 456)
	if got, want := len(ml.Events), 2; got != want {
		t.Fatalf("len(ml.Events) = %v, want %v", got, want)
	}
	if got, want := ml.Events[1].File, "logger_test.go"; got != want {
		t.Errorf("ml.Events[1].File = %q, want %q", got, want)
	}
	ml.Errorf("test %v", 789)
	if got, want := len(ml.Events), 3; got != want {
		t.Fatalf("len(ml.Events) = %v, want %v", got, want)
	}
	if got, want := ml.Events[2].File, "logger_test.go"; got != want {
		t.Errorf("ml.Events[2].File = %q, want %q", got, want)
	}
}

func TestChannelLogger(t *testing.T) {
	cl := NewChannelLogger(10)
	cl.Infof("test %v", 123)
	cl.Warningf("test %v", 123)
	cl.Errorf("test %v", 123)
	cl.Printf("test %v", 123)
	close(cl.C)

	count := 0
	for e := range cl.C {
		if got, want := e.Value, "test 123"; got != want {
			t.Errorf("e.Value = %q, want %q", got, want)
		}
		if e.File != "logger_test.go" {
			t.Errorf("Invalid file name: %v", e.File)
		}
		count++
	}
	if got, want := count, 4; got != want {
		t.Errorf("count = %v, want %v", got, want)
	}
}

func TestTeeLogger(t *testing.T) {
	ml := NewMemoryLogger()
	cl := NewChannelLogger(10)
	tl := NewTeeLogger(ml, cl)

	tl.Infof("test infof %v %v", 1, 2)
	tl.Warningf("test warningf %v %v", 2, 3)
	tl.Errorf("test errorf %v %v", 3, 4)
	tl.Printf("test printf %v %v", 4, 5)
	close(cl.C)

	clEvents := []*logutilpb.Event{}
	for e := range cl.C {
		clEvents = append(clEvents, e)
	}

	wantEvents := []*logutilpb.Event{
		{Level: logutilpb.Level_INFO, Value: "test infof 1 2"},
		{Level: logutilpb.Level_WARNING, Value: "test warningf 2 3"},
		{Level: logutilpb.Level_ERROR, Value: "test errorf 3 4"},
		{Level: logutilpb.Level_CONSOLE, Value: "test printf 4 5"},
	}
	wantFile := "logger_test.go"

	for i, events := range [][]*logutilpb.Event{ml.Events, clEvents} {
		if got, want := len(events), len(wantEvents); got != want {
			t.Fatalf("[%v] len(events) = %v, want %v", i, got, want)
		}
		for j, got := range events {
			want := wantEvents[j]
			if got.Level != want.Level {
				t.Errorf("[%v] events[%v].Level = %s, want %s", i, j, got.Level, want.Level)
			}
			if got.Value != want.Value {
				t.Errorf("[%v] events[%v].Value = %q, want %q", i, j, got.Value, want.Value)
			}
			// Skip the check below if go test -race is run because then the stack
			// is shifted by one and the test would fail.
			if !race.Enabled {
				if got.File != wantFile && got.Level != logutilpb.Level_CONSOLE {
					t.Errorf("[%v] events[%v].File = %q, want %q", i, j, got.File, wantFile)
				}
			}
		}
	}
}
