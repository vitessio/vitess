/*
Copyright 2024 The Vitess Authors.

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

package grpcclient

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
)

func TestGlogger(t *testing.T) {
	gl := glogger{}
	logs := logtest.Capture(t, slog.LevelInfo)

	gl.Warning("warning")
	gl.Warningln("warningln")
	gl.Warningf("formatted %s", "warning")

	records := logs.Records(t)
	require.Len(t, records, 3)
	for i, want := range []string{"warning", "warningln", "formatted warning"} {
		assert.Equal(t, "gRPC warning", records[i]["msg"])
		assert.Equal(t, "WARN", records[i]["level"])
		assert.Equal(t, want, records[i]["grpc_message"])
	}
}

func TestGloggerError(t *testing.T) {
	gl := glogger{}
	logs := logtest.Capture(t, slog.LevelInfo)

	gl.Error("error message")
	gl.Errorln("error message line")
	gl.Errorf("this is a %s error message", "formatted")

	records := logs.Records(t)
	require.Len(t, records, 3)
	for i, want := range []string{"error message", "error message line", "this is a formatted error message"} {
		assert.Equal(t, "gRPC error", records[i]["msg"])
		assert.Equal(t, "ERROR", records[i]["level"])
		assert.Equal(t, want, records[i]["grpc_message"])
	}
}
