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
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/log/logtest"
)

func TestSetErrorAttrs(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	var seen []string
	prev := log.SetErrorAttrs(func(r *slog.Record, v slog.Value) {
		err, ok := v.Any().(error)
		if !ok {
			return
		}
		seen = append(seen, err.Error())
		r.AddAttrs(slog.String("added", err.Error()))
	})
	t.Cleanup(func() { log.SetErrorAttrs(prev) })

	log.Error("two errors", slog.String("table", "t"), slog.Any("error", errors.New("first")), slog.Any("error", errors.New("second")))
	assert.Equal(t, []string{"first"}, seen)
	assert.Contains(t, logs.String(), `"added":"first"`)
	logs.Reset()

	log.Error("one error", slog.String("table", "t"), slog.Any("error", errors.New("only")))
	log.Info("no error", slog.Any("cause", errors.New("cause")))
	log.Debug("disabled", slog.Any("error", errors.New("disabled")))

	assert.Equal(t, []string{"first", "only"}, seen)
	assert.Equal(t, "only", logs.Record(t, "one error")["added"])
	assert.NotContains(t, logs.Record(t, "no error"), "added")

	assert.NotNil(t, log.SetErrorAttrs(nil))
	log.Error("unset", slog.Any("error", errors.New("unset")))
	assert.NotContains(t, logs.Record(t, "unset"), "added")
}
