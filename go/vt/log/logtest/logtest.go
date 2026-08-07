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

// Package logtest lets tests capture log output through [log.SwapLogger], which is
// atomic. Assigning to the log package's function variables instead (log.Warn = ...)
// races with every concurrent log call in the process.
package logtest

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"vitess.io/vitess/go/vt/log"
)

type (
	Record struct {
		Level   slog.Level
		Message string
		Attrs   []slog.Attr
	}

	Recorder struct {
		// a nil levels map captures every level.
		levels map[slog.Level]bool

		mu      sync.Mutex
		records []Record
	}

	handler struct {
		recorder *Recorder
		wrapped  slog.Handler
	}
)

// Capture records everything logged at the given levels, all levels if none are
// given, until the test ends. Records are still forwarded to the previous logger.
func Capture(t testing.TB, levels ...slog.Level) *Recorder {
	r := &Recorder{}
	if len(levels) > 0 {
		r.levels = make(map[slog.Level]bool, len(levels))
		for _, level := range levels {
			r.levels[level] = true
		}
	}

	// the log package has no accessor for the current logger, so swap in a
	// placeholder to get hold of the handler to wrap.
	var wrapped slog.Handler = slog.NewTextHandler(io.Discard, nil)
	previous := log.SwapLogger(slog.New(wrapped))
	if previous != nil {
		wrapped = previous.Handler()
	}
	log.SwapLogger(slog.New(&handler{recorder: r, wrapped: wrapped}))

	t.Cleanup(func() {
		log.SwapLogger(previous)
	})

	return r
}

func (r *Recorder) Records() []Record {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]Record(nil), r.records...)
}

// String returns the captured messages, one per line.
func (r *Recorder) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	messages := make([]string, 0, len(r.records))
	for _, record := range r.records {
		messages = append(messages, record.Message)
	}

	return strings.Join(messages, "\n")
}

func (r *Recorder) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.records = nil
}

func (r *Recorder) capturing(level slog.Level) bool {
	return r.levels == nil || r.levels[level]
}

func (r *Recorder) record(rec Record) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.records = append(r.records, rec)
}

func (h *handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.recorder.capturing(level) || h.wrapped.Enabled(ctx, level)
}

func (h *handler) Handle(ctx context.Context, r slog.Record) error {
	if h.recorder.capturing(r.Level) {
		rec := Record{
			Level:   r.Level,
			Message: r.Message,
			Attrs:   make([]slog.Attr, 0, r.NumAttrs()),
		}
		r.Attrs(func(attr slog.Attr) bool {
			rec.Attrs = append(rec.Attrs, attr)
			return true
		})
		h.recorder.record(rec)
	}

	if !h.wrapped.Enabled(ctx, r.Level) {
		return nil
	}

	return h.wrapped.Handle(ctx, r)
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &handler{recorder: h.recorder, wrapped: h.wrapped.WithAttrs(attrs)}
}

func (h *handler) WithGroup(name string) slog.Handler {
	return &handler{recorder: h.recorder, wrapped: h.wrapped.WithGroup(name)}
}
