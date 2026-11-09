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

package http

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/log/logtest"
)

func TestNewJSONResponseLogs(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)

	NewJSONResponse(nil, assert.AnError)

	record := logs.Record(t, "request failed")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, assert.AnError.Error(), record["error"])
}
