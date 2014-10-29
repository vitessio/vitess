package logutil

import (
	"fmt"
	"testing"
	"time"
)

func TestThrottledLogger(t *testing.T) {
	// Install a fake log func for testing.
	log := make(chan string)
	infof = func(format string, args ...interface{}) {
		log <- fmt.Sprintf(format, args...)
	}
	interval := 100 * time.Millisecond
	tl := NewThrottledLogger("name", interval)

	start := time.Now()

	go tl.Infof("test %v", 1)
	if got, want := <-log, "name:test 1"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}

	go func() {
		tl.Infof("test %v", 2)
		if tl.skippedCount != 1 {
			t.Errorf("skippedCount is %v but was expecting 1", tl.skippedCount)
		}
	}()
	if got, want := <-log, "name: skipped 1 log messages"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if tl.skippedCount != 0 {
		t.Errorf("skippedCount is %v but was expecting 0 after waiting", tl.skippedCount)
	}
	if got := time.Now().Sub(start); got < interval {
		t.Errorf("didn't wait long enough before logging, got %v, want >= %v", got, interval)
	}

	go tl.Infof("test %v", 3)
	if got, want := <-log, "name:test 3"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if tl.skippedCount != 0 {
		t.Errorf("skippedCount is %v but was expecting 0", tl.skippedCount)
	}
}
