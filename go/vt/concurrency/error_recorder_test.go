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

package concurrency

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/log/logtest"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestFirstErrorRecorderLogsLaterErrors(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	fer := &FirstErrorRecorder{}
	first := errors.New("first")
	fer.RecordError(first)
	fer.RecordError(vterrors.New(vtrpcpb.Code_UNAVAILABLE, "second"))

	assert.Equal(t, first, fer.Error())
	record := logs.Record(t, "FirstErrorRecorder: error")
	assert.Equal(t, "ERROR", record["level"])
	assert.EqualValues(t, 2, record["error_count"])
	assert.Equal(t, "second", record["error"])
	assert.Equal(t, "UNAVAILABLE", record["error_code"])
}
