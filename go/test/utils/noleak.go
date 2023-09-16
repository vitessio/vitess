/*
Copyright 2023 The Vitess Authors.

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

package utils

import (
	"context"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// LeakCheckContext returns a Context that will be automatically cancelled at the end
// of this test. If the test has finished successfully, it will be checked for goroutine
// leaks after context cancellation.
func LeakCheckContext(t testing.TB) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		EnsureNoLeaks(t)
	})
	return ctx
}

// LeakCheckContextTimeout behaves like LeakCheckContext but the returned Context will
// be cancelled after `timeout`, or after the test finishes, whichever happens first.
func LeakCheckContextTimeout(t testing.TB, timeout time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(func() {
		cancel()
		EnsureNoLeaks(t)
	})
	return ctx
}

// EnsureNoLeaks checks for goroutine and socket leaks and fails the test if any are found.
func EnsureNoLeaks(t testing.TB) {
	if t.Failed() {
		return
	}
	if err := ensureNoLeaks(); err != nil {
		t.Fatal(err)
	}
}

// GetLeaks checks for goroutine and socket leaks and returns an error if any are found.
// One use case is in TestMain()s to ensure that all tests are cleaned up.
func GetLeaks() error {
	return ensureNoLeaks()
}

func ensureNoLeaks() error {
	if err := ensureNoGoroutines(); err != nil {
		return err
	}
	return nil
}

func ensureNoGoroutines() error {
	var ignored = []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("vitess.io/vitess/go/vt/dbconfigs.init.0.func1"),
		goleak.IgnoreTopFunction("vitess.io/vitess/go/vt/vtgate.resetAggregators"),
		goleak.IgnoreTopFunction("vitess.io/vitess/go/vt/vtgate.processQueryInfo"),
		goleak.IgnoreTopFunction("github.com/patrickmn/go-cache.(*janitor).Run"),
		goleak.IgnoreTopFunction("vitess.io/vitess/go/vt/logutil.(*ThrottledLogger).log.func1"),
		goleak.IgnoreTopFunction("vitess.io/vitess/go/vt/vttablet/tabletserver/throttle.initThrottleTicker.func1.1"),
		goleak.IgnoreTopFunction("vitess.io/vitess/go/vt/vttablet/tabletserver/throttle.NewBackgroundClient.initThrottleTicker.func1.1"),
		goleak.IgnoreTopFunction("testing.tRunner.func1"),
	}

	var err error
	for i := 0; i < 5; i++ {
		err = goleak.Find(ignored...)
		if err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return err
}
