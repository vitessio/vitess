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
	"context"
	"log/slog"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vtlog "vitess.io/vitess/go/vt/log"
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
	t.Cleanup(func() { infoDepth = vtlog.InfoDepth })
	interval := 100 * time.Millisecond
	tl := NewThrottledLogger("name", interval)

	start := time.Now()

	go tl.Infof("test %v", 1)
	if got, want := <-log, "name: test 1"; got != want {
		assert.Equalf(t, want, got, "got %q, want %q", got, want)
	}

	go tl.Infof("test %v", 2)
	if got, want := <-log, "name: skipped 1 log messages"; got != want {
		assert.Equalf(t, want, got, "got %q, want %q", got, want)
	}
	if got, want := skippedCount(tl), 0; got != want {
		assert.Equalf(t, want, got, "skippedCount is %v but was expecting %v after waiting", got, want)
	}
	if got := time.Since(start); got < interval {
		assert.Failf(t, "did not wait long enough", "didn't wait long enough before logging, got %v, want >= %v", got, interval)
	}

	go tl.Infof("test %v", 3)
	if got, want := <-log, "name: test 3"; got != want {
		assert.Equalf(t, want, got, "got %q, want %q", got, want)
	}
	if got, want := skippedCount(tl), 0; got != want {
		assert.Equalf(t, want, got, "skippedCount is %v but was expecting %v", got, want)
	}
}

func TestThrottledLoggerAttrs(t *testing.T) {
	type record struct {
		depth int
		msg   string
		attrs []slog.Attr
	}
	log := make(chan record)
	infoDepth = func(depth int, msg string, attrs ...slog.Attr) {
		log <- record{depth, msg, attrs}
	}
	t.Cleanup(func() { infoDepth = vtlog.InfoDepth })
	interval := 100 * time.Millisecond
	tl := NewThrottledLogger("name", interval)

	start := time.Now()

	go tl.Info("test", slog.Int("n", 1))
	got := <-log
	assert.Equal(t, 2, got.depth)
	assert.Equal(t, "test", got.msg)
	assertAttrs(t, []slog.Attr{slog.String("logger", "name"), slog.Int("n", 1)}, got.attrs)

	go tl.Info("test", slog.Int("n", 2))
	got = <-log
	assert.Equal(t, 0, got.depth)
	assert.Equal(t, "skipped log messages", got.msg)
	assertAttrs(t, []slog.Attr{slog.String("logger", "name"), slog.Int("skipped", 1)}, got.attrs)
	assert.Equal(t, 0, skippedCount(tl))
	assert.GreaterOrEqual(t, time.Since(start), interval, "did not wait long enough before logging")

	go tl.Info("test", slog.Int("n", 3))
	got = <-log
	assert.Equal(t, "test", got.msg)
	assertAttrs(t, []slog.Attr{slog.String("logger", "name"), slog.Int("n", 3)}, got.attrs)
	assert.Equal(t, 0, skippedCount(tl))
}

// TestThrottledLoggerWriteOutsideLock checks that a call that is throttled
// doesn't wait for a write in progress.
func TestThrottledLoggerWriteOutsideLock(t *testing.T) {
	type record struct {
		msg   string
		attrs []slog.Attr
	}
	tests := []struct {
		name    string
		log     func(tl *ThrottledLogger)
		msg     string
		skipped record
	}{
		{"Info", func(tl *ThrottledLogger) { tl.Info("test") }, "test", record{"skipped log messages", []slog.Attr{slog.String("logger", "name"), slog.Int("skipped", 1)}}},
		{"Infof", func(tl *ThrottledLogger) { tl.Infof("test") }, "name: test", record{"name: skipped 1 log messages", nil}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			written := make(chan record, 2)
			first := make(chan struct{}, 1)
			first <- struct{}{}
			release := make(chan struct{})
			releaseWrite := sync.OnceFunc(func() { close(release) })
			t.Cleanup(releaseWrite)
			infoDepth = func(depth int, msg string, attrs ...slog.Attr) {
				written <- record{msg, attrs}
				select {
				case <-first:
					<-release
				default:
				}
			}
			t.Cleanup(func() { infoDepth = vtlog.InfoDepth })
			tl := NewThrottledLogger("name", 100*time.Millisecond)

			go tt.log(tl)
			assert.Equal(t, tt.msg, (<-written).msg)

			done := make(chan struct{})
			go func() {
				tt.log(tl)
				close(done)
			}()
			require.Eventually(t, func() bool {
				select {
				case <-done:
					return true
				default:
					return false
				}
			}, 30*time.Second, time.Millisecond, "throttled call waited for the write in progress")

			releaseWrite()
			got := <-written
			assert.Equal(t, tt.skipped.msg, got.msg)
			assertAttrs(t, tt.skipped.attrs, got.attrs)
		})
	}
}

// assertAttrs compares with Attr.Equal, since reflect.DeepEqual compares only
// the first byte of string values.
func assertAttrs(t *testing.T, want, got []slog.Attr) {
	t.Helper()
	if !assert.Len(t, got, len(want)) {
		return
	}
	for i := range want {
		assert.Truef(t, want[i].Equal(got[i]), "attr %d: got %v, want %v", i, got[i], want[i])
	}
}

// recordHandler sends each record it handles to records.
type recordHandler struct {
	slog.Handler
	records chan slog.Record
}

func (recordHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h recordHandler) Handle(_ context.Context, r slog.Record) error {
	h.records <- r.Clone()
	return nil
}

func TestThrottledLoggerOutput(t *testing.T) {
	h := recordHandler{Handler: slog.DiscardHandler, records: make(chan slog.Record, 1)}
	saved := vtlog.SwapLogger(slog.New(h))
	t.Cleanup(func() { vtlog.SwapLogger(saved) })

	attrs := []slog.Attr{slog.String("logger", "name"), slog.Int("n", 1)}
	tests := []struct {
		name  string
		log   func(tl *ThrottledLogger) int
		level slog.Level
		msg   string
		attrs []slog.Attr
	}{
		{"Info", func(tl *ThrottledLogger) int { tl.Info("test", slog.Int("n", 1)); return line() }, slog.LevelInfo, "test", attrs},
		{"Warn", func(tl *ThrottledLogger) int { tl.Warn("test", slog.Int("n", 1)); return line() }, slog.LevelWarn, "test", attrs},
		{"Error", func(tl *ThrottledLogger) int { tl.Error("test", slog.Int("n", 1)); return line() }, slog.LevelError, "test", attrs},
		{"Infof", func(tl *ThrottledLogger) int { tl.Infof("test %d", 1); return line() }, slog.LevelInfo, "name: test 1", nil},
		{"Warningf", func(tl *ThrottledLogger) int { tl.Warningf("test %d", 1); return line() }, slog.LevelWarn, "name: test 1", nil},
		{"Errorf", func(tl *ThrottledLogger) int { tl.Errorf("test %d", 1); return line() }, slog.LevelError, "name: test 1", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantLine := tt.log(NewThrottledLogger("name", time.Minute))
			r := <-h.records
			assert.Equal(t, tt.level, r.Level)
			assert.Equal(t, tt.msg, r.Message)
			var got []slog.Attr
			r.Attrs(func(a slog.Attr) bool {
				got = append(got, a)
				return true
			})
			assertAttrs(t, tt.attrs, got)
			frame, _ := runtime.CallersFrames([]uintptr{r.PC}).Next()
			assert.Equal(t, wantLine, frame.Line)
		})
	}
}

// line returns the line of its caller.
func line() int {
	_, _, l, ok := runtime.Caller(1)
	if !ok {
		panic("runtime.Caller failed")
	}
	return l
}
