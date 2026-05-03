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
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func skippedCount(tl *ThrottledLogger) int {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return tl.skippedCount
}

func TestThrottledLogger(t *testing.T) {
	// Install a fake log func for testing.
	log := make(chan string)
	infoDepth = func(depth int, msg string, attrs ...slog.Attr) {
		log <- msg
	}
	interval := 100 * time.Millisecond
	tl := NewThrottledLogger("name", interval)

	start := time.Now()

	go tl.Infof("test %v", 1)
	assert.Equal(t, "name: test 1", <-log)

	go tl.Infof("test %v", 2)
	assert.Equal(t, "name: skipped 1 log messages", <-log)
	assert.Equal(t, 0, skippedCount(tl), "skippedCount after waiting")
	assert.GreaterOrEqual(t, time.Since(start), interval, "should have waited at least one interval")

	go tl.Infof("test %v", 3)
	assert.Equal(t, "name: test 3", <-log)
	assert.Equal(t, 0, skippedCount(tl))
}
