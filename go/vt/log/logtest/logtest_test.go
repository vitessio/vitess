/*
Copyright 2026 The Vitess Authors.

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

package logtest

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

// sink is a handler that counts every record reaching the bottom of the chain,
// so that a test can tell whether installing a capture dropped anything.
type sink struct {
	count atomic.Int64
}

func (s *sink) Enabled(ctx context.Context, level slog.Level) bool { return true }

func (s *sink) Handle(ctx context.Context, r slog.Record) error {
	s.count.Add(1)
	return nil
}

func (s *sink) WithAttrs(attrs []slog.Attr) slog.Handler { return s }

func (s *sink) WithGroup(name string) slog.Handler { return s }

// installSink makes s the bottom of the logger chain for the test.
func installSink(t *testing.T, s *sink) *slog.Logger {
	installed := slog.New(s)
	previous := log.SwapLogger(installed)
	t.Cleanup(func() {
		log.SwapLogger(previous)
	})

	return installed
}

func TestCaptureRecordsMessagesAndAttrs(t *testing.T) {
	recorder := Capture(t, slog.LevelWarn)

	log.Warn("a warning", slog.String("name", "value"))

	records := recorder.Records()
	require.Len(t, records, 1)
	assert.Equal(t, slog.LevelWarn, records[0].Level)
	assert.Equal(t, "a warning", records[0].Message)
	require.Len(t, records[0].Attrs, 1)
	assert.Equal(t, "name", records[0].Attrs[0].Key)
	assert.Equal(t, "value", records[0].Attrs[0].Value.String())
}

func TestCaptureFiltersLevels(t *testing.T) {
	recorder := Capture(t, slog.LevelError)

	log.Info("an info")
	log.Warn("a warning")
	log.Error("an error")

	assert.Equal(t, "an error", recorder.String())
}

func TestCaptureWithoutLevelsRecordsEverything(t *testing.T) {
	recorder := Capture(t)

	log.Info("an info")
	log.Error("an error")

	assert.Equal(t, "an info\nan error", recorder.String())
}

func TestCaptureRecordsDisabledLevels(t *testing.T) {
	// the default logger is at info level, so it drops debug records.
	recorder := Capture(t, slog.LevelDebug)

	log.Debug("a debug message")

	assert.Equal(t, "a debug message", recorder.String())
}

func TestCaptureForwardsToThePreviousLogger(t *testing.T) {
	var s sink
	installSink(t, &s)

	Capture(t, slog.LevelWarn)
	log.Warn("a warning")

	assert.Equal(t, int64(1), s.count.Load())
}

func TestClear(t *testing.T) {
	recorder := Capture(t, slog.LevelWarn)

	log.Warn("first")
	recorder.Clear()
	log.Warn("second")

	assert.Equal(t, "second", recorder.String())
}

func TestCaptureRestoresPreviousLogger(t *testing.T) {
	var s sink
	installed := installSink(t, &s)
	outer := Capture(t, slog.LevelWarn)

	t.Run("inner", func(t *testing.T) {
		inner := Capture(t, slog.LevelWarn)

		log.Warn("logged while nested")

		assert.Equal(t, "logged while nested", inner.String())
	})

	log.Warn("logged after the inner test")

	assert.Equal(t, "logged while nested\nlogged after the inner test", outer.String())

	outer.Stop()
	assert.Equal(t, installed, log.SwapLogger(installed), "captures unwound in order must restore the original logger")
}

func TestStopIsIdempotent(t *testing.T) {
	var s sink
	installed := installSink(t, &s)
	recorder := Capture(t, slog.LevelWarn)

	recorder.Stop()
	recorder.Stop()

	assert.Equal(t, installed, log.SwapLogger(installed))

	log.Warn("logged after stopping")
	assert.Empty(t, recorder.String())
	assert.Equal(t, int64(1), s.count.Load(), "the record must still reach the previous logger")
}

func TestOverlappingCapturesStoppedOutOfOrder(t *testing.T) {
	var s sink
	installSink(t, &s)

	first := Capture(t, slog.LevelWarn)
	second := Capture(t, slog.LevelWarn)

	// stopping the capture underneath must not disable the one on top of it.
	first.Stop()
	log.Warn("logged after the first stopped")

	assert.Empty(t, first.String())
	assert.Equal(t, "logged after the first stopped", second.String())

	second.Stop()
	log.Warn("logged after both stopped")

	assert.Empty(t, first.String())
	assert.Equal(t, "logged after the first stopped", second.String())
	assert.Equal(t, int64(2), s.count.Load(), "no record may be lost while captures are stopped")
}

func TestOverlappingCapturesRecordIndependently(t *testing.T) {
	warnings := Capture(t, slog.LevelWarn)
	errors := Capture(t, slog.LevelError)

	log.Warn("a warning")
	log.Error("an error")

	assert.Equal(t, "a warning", warnings.String())
	assert.Equal(t, "an error", errors.String())
}

func TestConcurrentCapturesDoNotDropOrLoseRecords(t *testing.T) {
	// under -race: captures are installed and stopped while another goroutine
	// logs continuously. every record must reach the sink at the bottom of the
	// chain, and every capture must see what it logged while it was active.
	var s sink
	installSink(t, &s)

	const (
		capturers = 8
		records   = 100
	)

	done := make(chan struct{})
	var logged atomic.Int64
	var logging sync.WaitGroup
	logging.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
				log.Warn("background")
				logged.Add(1)
			}
		}
	})

	var capturing sync.WaitGroup
	capturing.Add(capturers)
	for i := range capturers {
		go func() {
			defer capturing.Done()
			message := fmt.Sprintf("capture %d", i)
			for range records {
				recorder := Capture(t, slog.LevelWarn)
				log.Warn(message)
				logged.Add(1)
				assert.Contains(t, recorder.String(), message)
				recorder.Stop()
			}
		}()
	}
	capturing.Wait()

	close(done)
	logging.Wait()

	assert.Equal(t, logged.Load(), s.count.Load(), "records were dropped while captures were installed")
}
