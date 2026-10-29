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

package tabletserver

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/logutil"
)

// TestLazyString checks that lazyString is built only when a throttled logger
// writes the record, and that it formats as its value for slog and for fmt.
func TestLazyString(t *testing.T) {
	calls := 0
	value := lazyString(func() string {
		calls++
		return "expensive"
	})

	assert.Equal(t, "expensive", value.LogValue().String())
	assert.Equal(t, "expensive", value.String())
	// The glog backend formats attributes with fmt.
	assert.Equal(t, "value=expensive", fmt.Sprint(slog.Any("value", value)))

	logs := logtest.Capture(t, slog.LevelInfo)
	calls = 0
	logger := logutil.NewThrottledLogger("LazyStringTest", time.Hour)
	logger.Error("lazy value", slog.Any("value", value))
	logger.Error("lazy value", slog.Any("value", value))
	assert.Equal(t, 1, calls)
	assert.Equal(t, "expensive", logs.Record(t, "lazy value")["value"])
}
