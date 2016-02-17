package logutil

import (
	"testing"
	"time"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
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
		if got, want := ml.Events[i].File, "logger_test.go"; got != want && ml.Events[i].Level != logutilpb.Level_CONSOLE {
			t.Errorf("ml.Events[%v].File = %q, want %q", i, got, want)
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
	close(cl)

	count := 0
	for e := range cl {
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
	ml1 := NewMemoryLogger()
	ml2 := NewMemoryLogger()
	tl := NewTeeLogger(ml1, ml2)
	tl.Infof("test infof %v %v", 1, 2)
	tl.Warningf("test warningf %v %v", 2, 3)
	tl.Errorf("test errorf %v %v", 3, 4)
	tl.Printf("test printf %v %v", 4, 5)
	tl.InfoDepth(0, "test infoDepth")
	tl.WarningDepth(0, "test warningDepth")
	tl.ErrorDepth(0, "test errorDepth")
	for i, ml := range []*MemoryLogger{ml1, ml2} {
		if len(ml.Events) != 7 {
			t.Fatalf("Invalid ml%v size: %v", i+1, ml)
		}
		if ml.Events[0].Value != "test infof 1 2" {
			t.Errorf("Invalid ml%v[0]: %v", i+1, ml.Events[0].Value)
		}
		if ml.Events[0].Level != logutilpb.Level_INFO {
			t.Errorf("Invalid ml%v[0].level: %v", i+1, ml.Events[0].Level)
		}
		if ml.Events[1].Value != "test warningf 2 3" {
			t.Errorf("Invalid ml%v[0]: %v", i+1, ml.Events[1].Value)
		}
		if ml.Events[1].Level != logutilpb.Level_WARNING {
			t.Errorf("Invalid ml%v[0].level: %v", i+1, ml.Events[1].Level)
		}
		if ml.Events[2].Value != "test errorf 3 4" {
			t.Errorf("Invalid ml%v[0]: %v", i+1, ml.Events[2].Value)
		}
		if ml.Events[2].Level != logutilpb.Level_ERROR {
			t.Errorf("Invalid ml%v[0].level: %v", i+1, ml.Events[2].Level)
		}
		if ml.Events[3].Value != "test printf 4 5" {
			t.Errorf("Invalid ml%v[0]: %v", i+1, ml.Events[3].Value)
		}
		if ml.Events[3].Level != logutilpb.Level_CONSOLE {
			t.Errorf("Invalid ml%v[0].level: %v", i+1, ml.Events[3].Level)
		}
		if ml.Events[4].Value != "test infoDepth" {
			t.Errorf("Invalid ml%v[0]: %v", i+1, ml.Events[4].Value)
		}
		if ml.Events[4].Level != logutilpb.Level_INFO {
			t.Errorf("Invalid ml%v[0].level: %v", i+1, ml.Events[4].Level)
		}
		if ml.Events[5].Value != "test warningDepth" {
			t.Errorf("Invalid ml%v[0]: %v", i+1, ml.Events[5].Value)
		}
		if ml.Events[5].Level != logutilpb.Level_WARNING {
			t.Errorf("Invalid ml%v[0].level: %v", i+1, ml.Events[5].Level)
		}
		if ml.Events[6].Value != "test errorDepth" {
			t.Errorf("Invalid ml%v[0]: %v", i+1, ml.Events[6].Value)
		}
		if ml.Events[6].Level != logutilpb.Level_ERROR {
			t.Errorf("Invalid ml%v[0].level: %v", i+1, ml.Events[6].Level)
		}

		for j := range ml.Events {
			if got, want := ml.Events[j].File, "logger_test.go"; got != want && ml.Events[j].Level != logutilpb.Level_CONSOLE {
				t.Errorf("ml%v[%v].File = %q, want %q", i+1, j, got, want)
			}
		}
	}
}
