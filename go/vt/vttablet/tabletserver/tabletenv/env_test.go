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

package tabletenv

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/vtenv"
)

func TestLogErrorPanic(t *testing.T) {
	env := NewEnv(vtenv.NewTestEnv(), NewDefaultConfig(), "TestLogErrorPanic")
	logs := logtest.Capture(t, slog.LevelInfo)

	func() {
		defer env.LogError()
		panic("boom")
	}()

	record := logs.Record(t, "Uncaught panic")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "boom", record["panic"])
	assert.Contains(t, record["stack"], "TestLogErrorPanic")
}
