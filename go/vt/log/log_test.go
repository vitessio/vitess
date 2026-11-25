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

package log

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnabled(t *testing.T) {
	t.Run("structured logger respects configured level", func(t *testing.T) {
		prev := SwapLogger(slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo})))
		t.Cleanup(func() { SwapLogger(prev) })

		assert.False(t, Enabled(slog.LevelDebug))
		assert.True(t, Enabled(slog.LevelInfo))
		assert.True(t, Enabled(slog.LevelWarn))
		assert.True(t, Enabled(slog.LevelError))

		SwapLogger(slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})))
		assert.True(t, Enabled(slog.LevelDebug))
		assert.True(t, Enabled(slog.LevelInfo))
	})

	t.Run("nil logger returns false", func(t *testing.T) {
		prev := SwapLogger(nil)
		t.Cleanup(func() { SwapLogger(prev) })

		assert.False(t, Enabled(slog.LevelInfo))
		assert.False(t, Enabled(slog.LevelError))
	})

	t.Run("glog fallback disables debug and enables info and above", func(t *testing.T) {
		prev := structured.Swap(false)
		t.Cleanup(func() { structured.Store(prev) })

		assert.False(t, Enabled(slog.LevelDebug))
		assert.True(t, Enabled(slog.LevelInfo))
		assert.True(t, Enabled(slog.LevelWarn))
		assert.True(t, Enabled(slog.LevelError))
	})
}
