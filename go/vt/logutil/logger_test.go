package logutil

import (
	"testing"
	"time"
)

func TestLoggerEventFormat(t *testing.T) {
	testValues := []struct {
		event    LoggerEvent
		expected string
	}{
		{
			event: LoggerEvent{
				Time:  time.Date(2014, time.November, 10, 23, 30, 12, 123456000, time.UTC),
				Level: LOGGER_INFO,
				File:  "file.go",
				Line:  123,
				Value: "message",
			},
			expected: "I1110 23:30:12.123456 file.go:123] message",
		},
		{
			event: LoggerEvent{
				Time:  time.Date(2014, time.January, 20, 23, 30, 12, 0, time.UTC),
				Level: LOGGER_WARNING,
				File:  "file2.go",
				Line:  567,
				Value: "message %v %v",
			},
			expected: "W0120 23:30:12.000000 file2.go:567] message %v %v",
		},
	}
	for _, testValue := range testValues {
		got := testValue.event.String()
		if testValue.expected != got {
			t.Errorf("invalid printing of %v: expected '%v' got '%v'", testValue.event, testValue.expected, got)
		}
	}
}

func TestMemoryLogger(t *testing.T) {
	ml := NewMemoryLogger()
	ml.Infof("test %v", 123)
	if len(ml.Events) != 1 {
		t.Fatalf("Invalid MemoryLogger size: %v", ml)
	}
	if ml.Events[0].File != "logger_test.go" {
		t.Errorf("Invalid file name: %v", ml.Events[0].File)
	}
	ml.Errorf("test %v", 456)
	if len(ml.Events) != 2 {
		t.Fatalf("Invalid MemoryLogger size: %v", ml)
	}
	if ml.Events[1].File != "logger_test.go" {
		t.Errorf("Invalid file name: %v", ml.Events[1].File)
	}
}

func TestChannelLogger(t *testing.T) {
	cl := NewChannelLogger(10)
	cl.Warningf("test %v", 123)

	e := <-cl
	if e.File != "logger_test.go" {
		t.Errorf("Invalid file name: %v", e.File)
	}
}
