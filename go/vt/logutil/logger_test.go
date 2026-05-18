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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/protoutil"
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
				Time:  protoutil.TimeToProto(time.Date(2014, time.November, 10, 23, 30, 12, 123456000, time.UTC)),
				Level: logutilpb.Level_INFO,
				File:  "file.go",
				Line:  123,
				Value: "message",
			},
			expected: "I1110 23:30:12.123456 file.go:123] message",
		},
		{
			event: &logutilpb.Event{
				Time:  protoutil.TimeToProto(time.Date(2014, time.January, 20, 23, 30, 12, 0, time.UTC)),
				Level: logutilpb.Level_WARNING,
				File:  "file2.go",
				Line:  567,
				Value: "message %v %v",
			},
			expected: "W0120 23:30:12.000000 file2.go:567] message %v %v",
		},
		{
			event: &logutilpb.Event{
				Time:  protoutil.TimeToProto(time.Date(2014, time.January, 20, 23, 30, 12, 0, time.UTC)),
				Level: logutilpb.Level_ERROR,
				File:  "file2.go",
				Line:  567,
				Value: "message %v %v",
			},
			expected: "E0120 23:30:12.000000 file2.go:567] message %v %v",
		},
		{
			event: &logutilpb.Event{
				Time:  protoutil.TimeToProto(time.Date(2014, time.January, 20, 23, 30, 12, 0, time.UTC)),
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
		assert.Equalf(t, testValue.expected, ml.Events[i].Value, "ml.Events[%v].Value", i)
		// Skip the check below if go test -race is run because then the stack
		// is shifted by one and the test would fail.
		if !race.Enabled {
			if ml.Events[i].Level != logutilpb.Level_CONSOLE {
				assert.Equalf(t, "logger_test.go", ml.Events[i].File, "ml.Events[%v].File (line = %v)", i, ml.Events[i].Line)
			}
		}
	}
}

func TestMemoryLogger(t *testing.T) {
	ml := NewMemoryLogger()
	ml.Infof("test %v", 123)
	require.Len(t, ml.Events, 1)
	assert.Equal(t, "logger_test.go", ml.Events[0].File, "ml.Events[0].File")
	ml.Warningf("test %v", 456)
	require.Len(t, ml.Events, 2)
	assert.Equal(t, "logger_test.go", ml.Events[1].File, "ml.Events[1].File")
	ml.Errorf("test %v", 789)
	require.Len(t, ml.Events, 3)
	assert.Equal(t, "logger_test.go", ml.Events[2].File, "ml.Events[2].File")
}

func TestTeeLogger(t *testing.T) {
	ml1 := NewMemoryLogger()
	ml2 := NewMemoryLogger()
	tl := NewTeeLogger(ml1, ml2)

	tl.Infof("test infof %v %v", 1, 2)
	tl.Warningf("test warningf %v %v", 2, 3)
	tl.Errorf("test errorf %v %v", 3, 4)
	tl.Printf("test printf %v %v", 4, 5)

	wantEvents := []*logutilpb.Event{
		{Level: logutilpb.Level_INFO, Value: "test infof 1 2"},
		{Level: logutilpb.Level_WARNING, Value: "test warningf 2 3"},
		{Level: logutilpb.Level_ERROR, Value: "test errorf 3 4"},
		{Level: logutilpb.Level_CONSOLE, Value: "test printf 4 5"},
	}
	wantFile := "logger_test.go"

	for i, events := range [][]*logutilpb.Event{ml1.Events, ml2.Events} {
		require.Lenf(t, events, len(wantEvents), "[%v] len(events)", i)
		for j, got := range events {
			want := wantEvents[j]
			assert.Equalf(t, want.Level, got.Level, "[%v] events[%v].Level", i, j)
			assert.Equalf(t, want.Value, got.Value, "[%v] events[%v].Value", i, j)
			// Skip the check below if go test -race is run because then the stack
			// is shifted by one and the test would fail.
			if !race.Enabled {
				if got.Level != logutilpb.Level_CONSOLE {
					assert.Equalf(t, wantFile, got.File, "[%v] events[%v].File", i, j)
				}
			}
		}
	}
}
