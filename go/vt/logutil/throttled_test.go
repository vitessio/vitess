package logutil

import (
	"fmt"
	"testing"
	"time"
)

func skippedCount(tl *ThrottledLogger) int {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return tl.skippedCount
}

func TestThrottledLogger(t *testing.T) {
	// Install a fake log func for testing.
	log := make(chan string)
	infoDepth = func(depth int, args ...interface{}) {
		log <- fmt.Sprint(args...)
	}
	interval := 100 * time.Millisecond
	tl := NewThrottledLogger("name", interval)

	start := time.Now()

	go tl.Infof("test %v", 1)
	if got, want := <-log, "name: test 1"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}

	go tl.Infof("test %v", 2)
	if got, want := <-log, "name: skipped 1 log messages"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got, want := skippedCount(tl), 0; got != want {
		t.Errorf("skippedCount is %v but was expecting %v after waiting", got, want)
	}
	if got := time.Now().Sub(start); got < interval {
		t.Errorf("didn't wait long enough before logging, got %v, want >= %v", got, interval)
	}

	go tl.Infof("test %v", 3)
	if got, want := <-log, "name: test 3"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got, want := skippedCount(tl), 0; got != want {
		t.Errorf("skippedCount is %v but was expecting %v", got, want)
	}
}
