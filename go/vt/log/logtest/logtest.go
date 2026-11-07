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

// Package logtest captures the records logged through go/vt/log in tests.
package logtest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

// Logs collects JSON log records. Writes are serialized, as the code under
// test often logs from its own goroutines.
type Logs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Capture logs JSON records at level and above to a Logs for the rest of the
// test. It replaces the process-wide logger, so it can't be used in parallel
// tests.
func Capture(t testing.TB, level slog.Level) *Logs {
	t.Helper()
	l := &Logs{}
	saved := log.SwapLogger(slog.New(slog.NewJSONHandler(l, &slog.HandlerOptions{Level: level})))
	t.Cleanup(func() { log.SwapLogger(saved) })
	return l
}

func (l *Logs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

// String returns the records logged so far, one JSON object per line.
func (l *Logs) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// Reset discards the records logged so far.
func (l *Logs) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf.Reset()
}

// Records decodes every record logged so far. It fails if a record has the
// same key twice, which decoding into a map would hide.
func (l *Logs) Records(t require.TestingT) []map[string]any {
	var records []map[string]any
	dec := json.NewDecoder(strings.NewReader(l.String()))
	for dec.More() {
		var raw json.RawMessage
		require.NoError(t, dec.Decode(&raw))
		require.NoError(t, uniqueKeys(raw))
		var record map[string]any
		require.NoError(t, json.Unmarshal(raw, &record))
		records = append(records, record)
	}
	return records
}

// uniqueKeys returns an error if the JSON object raw has the same key twice at
// its top level.
func uniqueKeys(raw json.RawMessage) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if _, err := dec.Token(); err != nil {
		return err
	}
	seen := make(map[string]bool)
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		key, _ := tok.(string)
		if seen[key] {
			return fmt.Errorf("duplicate key %q in log record %s", key, raw)
		}
		seen[key] = true
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return err
		}
	}
	return nil
}

// Record returns the single record logged so far with msg.
func (l *Logs) Record(t testing.TB, msg string) map[string]any {
	t.Helper()
	var found []map[string]any
	for _, record := range l.Records(t) {
		if record["msg"] == msg {
			found = append(found, record)
		}
	}
	require.Len(t, found, 1, "expected a single %q log record", msg)
	return found[0]
}

// Wait waits up to 30 seconds for a record with msg, and returns the first one.
// Unlike Record, it doesn't require msg to be logged only once.
func (l *Logs) Wait(t testing.TB, msg string) map[string]any {
	t.Helper()
	var found map[string]any
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for _, record := range l.Records(c) {
			if record["msg"] == msg {
				found = record
				return
			}
		}
		assert.Failf(c, "log record not found", "msg %q, logs:\n%s", msg, l.String())
	}, 30*time.Second, 50*time.Millisecond)
	return found
}
