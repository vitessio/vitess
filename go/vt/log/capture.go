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
)

// CaptureHandler records slog records in memory. Used for testing.
type CaptureHandler struct {
	mu sync.Mutex

	records []slog.Record
}

// NewCaptureHandler returns a CaptureHandler that records all log output.
func NewCaptureHandler() *CaptureHandler {
	return &CaptureHandler{}
}

// Enabled implements [slog.Handler].
func (h *CaptureHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return true
}

// Handle implements [slog.Handler].
func (h *CaptureHandler) Handle(ctx context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.records = append(h.records, record.Clone())
	return nil
}

// WithAttrs implements [slog.Handler].
func (h *CaptureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

// WithGroup implements [slog.Handler].
func (h *CaptureHandler) WithGroup(name string) slog.Handler {
	return h
}

// Records returns a copy of the captured records.
func (h *CaptureHandler) Records() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()

	records := make([]slog.Record, len(h.records))
	copy(records, h.records)

	return records
}

// Last returns the most recent captured record.
func (h *CaptureHandler) Last() (slog.Record, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.records) == 0 {
		return slog.Record{}, false
	}

	return h.records[len(h.records)-1], true
}

// Reset clears all captured records.
func (h *CaptureHandler) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.records = nil
}
