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
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

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

func TestClear(t *testing.T) {
	recorder := Capture(t, slog.LevelWarn)

	log.Warn("first")
	recorder.Clear()
	log.Warn("second")

	assert.Equal(t, "second", recorder.String())
}

func TestCaptureRestoresPreviousLogger(t *testing.T) {
	outer := Capture(t, slog.LevelWarn)

	t.Run("inner", func(t *testing.T) {
		inner := Capture(t, slog.LevelWarn)

		log.Warn("logged while nested")

		assert.Equal(t, "logged while nested", inner.String())
	})

	log.Warn("logged after the inner test")

	assert.Equal(t, "logged while nested\nlogged after the inner test", outer.String())
}

func TestCaptureIsConcurrencySafe(t *testing.T) {
	// under -race: goroutines log while the test reads and clears the recorder.
	recorder := Capture(t, slog.LevelWarn)

	const goroutines = 8
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func() {
			defer wg.Done()
			for range 100 {
				log.Warn("concurrent", slog.Int("goroutine", i))
			}
		}()
	}

	for range 100 {
		_ = recorder.String()
		_ = recorder.Records()
		recorder.Clear()
	}

	wg.Wait()
}
