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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

func TestCapture(t *testing.T) {
	logs := Capture(t, slog.LevelInfo)
	log.Info("first", slog.String("key", "value"))
	log.V(2).Info("below the level")
	log.Warn("second")

	records := logs.Records(t)
	assert.Len(t, records, 2)
	record := logs.Record(t, "first")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, "value", record["key"])
	assert.Equal(t, "WARN", logs.Record(t, "second")["level"])

	logs.Reset()
	assert.Empty(t, logs.Records(t))
}

func TestWait(t *testing.T) {
	logs := Capture(t, slog.LevelInfo)
	go func() {
		time.Sleep(100 * time.Millisecond)
		log.Error("later", slog.Int("attempt", 1))
	}()
	record := logs.Wait(t, "later")
	assert.Equal(t, "ERROR", record["level"])
	assert.EqualValues(t, 1, record["attempt"])
}

func TestUniqueKeys(t *testing.T) {
	require.NoError(t, uniqueKeys([]byte(`{"msg":"m","a":{"a":1},"b":[1]}`)))
	require.ErrorContains(t, uniqueKeys([]byte(`{"msg":"m","a":1,"a":2}`)), `duplicate key "a"`)
}
