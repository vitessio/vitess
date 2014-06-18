package logutil

import (
	"testing"
	"time"
)

func TestThrottledLogger(t *testing.T) {
	tl := NewThrottledLogger("test", 100*time.Millisecond)

	tl.Infof("test %v", 1)
	tl.Infof("test %v", 2)
	if tl.skippedCount != 1 {
		t.Fatalf("skippedCount is %v but was expecting 1", tl.skippedCount)
	}

	time.Sleep(100 * time.Millisecond)
	tl.Infof("test %v", 3)
	if tl.skippedCount != 0 {
		t.Fatalf("skippedCount is %v but was expecting 0 after reset", tl.skippedCount)
	}
}
