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

package log_test

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/logutil"
)

// TestLazyString checks that a LazyString is built only when its record is
// written, and that it formats as its value for slog and for fmt.
func TestLazyString(t *testing.T) {
	calls := 0
	value := log.LazyString(func() string {
		calls++
		return "expensive"
	})

	assert.Equal(t, "expensive", value.LogValue().String())
	assert.Equal(t, "expensive", value.String())
	// The glog backend formats attributes with fmt.
	assert.Equal(t, "value=expensive", fmt.Sprint(slog.Any("value", value)))

	t.Run("V(2) off", func(t *testing.T) {
		logs := logtest.Capture(t, slog.LevelInfo)
		calls = 0
		log.V(2).Info("lazy value", slog.Any("value", value))
		assert.Zero(t, calls)
		assert.Empty(t, logs.Records(t))
	})

	t.Run("V(2) on", func(t *testing.T) {
		logs := logtest.Capture(t, slog.LevelDebug)
		calls = 0
		log.V(2).Info("lazy value", slog.Any("value", value))
		assert.Equal(t, 1, calls)
		record := logs.Record(t, "lazy value")
		assert.Equal(t, "INFO", record["level"])
		assert.Equal(t, "expensive", record["value"])
	})

	t.Run("throttled", func(t *testing.T) {
		logs := logtest.Capture(t, slog.LevelInfo)
		calls = 0
		logger := logutil.NewThrottledLogger("LazyStringTest", time.Hour)
		logger.Error("lazy value", slog.Any("value", value))
		logger.Error("lazy value", slog.Any("value", value))
		assert.Equal(t, 1, calls)
		assert.Equal(t, "expensive", logs.Record(t, "lazy value")["value"])
	})
}
