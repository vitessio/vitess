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
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingHandler counts the records it handles and forwards them.
type countingHandler struct {
	count   *atomic.Int64
	wrapped slog.Handler
}

func (h *countingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return true
}

func (h *countingHandler) Handle(ctx context.Context, r slog.Record) error {
	h.count.Add(1)
	if !h.wrapped.Enabled(ctx, r.Level) {
		return nil
	}

	return h.wrapped.Handle(ctx, r)
}

func (h *countingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &countingHandler{count: h.count, wrapped: h.wrapped.WithAttrs(attrs)}
}

func (h *countingHandler) WithGroup(name string) slog.Handler {
	return &countingHandler{count: h.count, wrapped: h.wrapped.WithGroup(name)}
}

// restoreLogger reinstalls the logger that was installed when the test started.
func restoreLogger(t *testing.T) *slog.Logger {
	original := logger.Load()
	t.Cleanup(func() {
		logger.Store(original)
	})

	return original
}

func TestWrapLogger(t *testing.T) {
	original := restoreLogger(t)

	var count atomic.Int64
	previous, installed := WrapLogger(func(current *slog.Logger) *slog.Logger {
		require.Equal(t, original, current)
		return slog.New(&countingHandler{count: &count, wrapped: current.Handler()})
	})

	assert.Equal(t, original, previous)
	assert.Equal(t, installed, logger.Load())

	Info("logged")
	assert.Equal(t, int64(1), count.Load())
}

func TestWrapLoggerWithNoLoggerInstalled(t *testing.T) {
	restoreLogger(t)
	logger.Store(nil)

	var gotCurrent *slog.Logger
	var called bool
	previous, installed := WrapLogger(func(current *slog.Logger) *slog.Logger {
		gotCurrent, called = current, true
		return slog.New(slog.DiscardHandler)
	})

	assert.True(t, called)
	assert.Nil(t, gotCurrent)
	assert.Nil(t, previous)
	assert.Equal(t, installed, logger.Load())
}

func TestWrapLoggerKeepsEveryConcurrentWrapper(t *testing.T) {
	restoreLogger(t)

	// Every wrapper must end up in the chain: a lost update would silently drop
	// one caller's capture.
	const wrappers = 16
	counts := make([]atomic.Int64, wrappers)
	var wg sync.WaitGroup
	wg.Add(wrappers)
	for i := range wrappers {
		go func() {
			defer wg.Done()
			WrapLogger(func(current *slog.Logger) *slog.Logger {
				return slog.New(&countingHandler{count: &counts[i], wrapped: current.Handler()})
			})
		}()
	}
	wg.Wait()

	Info("logged")

	for i := range wrappers {
		assert.Equal(t, int64(1), counts[i].Load(), "wrapper %d did not see the record", i)
	}
}

func TestCompareAndSwapLogger(t *testing.T) {
	original := restoreLogger(t)
	other := slog.New(slog.DiscardHandler)

	assert.False(t, CompareAndSwapLogger(other, other))
	assert.Equal(t, original, logger.Load())

	assert.True(t, CompareAndSwapLogger(original, other))
	assert.Equal(t, other, logger.Load())
}
