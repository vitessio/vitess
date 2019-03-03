/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
	if got := time.Since(start); got < interval {
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
