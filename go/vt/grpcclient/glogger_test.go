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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

func TestGlogger(t *testing.T) {
	gl := glogger{}

	origWarnDepth := log.WarnDepth
	t.Cleanup(func() { log.WarnDepth = origWarnDepth })

	var logMessage string
	log.WarnDepth = func(_ int, msg string, _ ...slog.Attr) {
		logMessage = msg
	}

	gl.Warning("warning")
	require.Contains(t, logMessage, "warning")

	logMessage = ""
	gl.Warningln("warningln")
	require.Contains(t, logMessage, "warningln")

	logMessage = ""
	gl.Warningf("formatted %s", "warning")
	require.Contains(t, logMessage, "formatted warning")
}

func TestGloggerError(t *testing.T) {
	gl := glogger{}

	origErrorDepth := log.ErrorDepth
	t.Cleanup(func() { log.ErrorDepth = origErrorDepth })

	var logMessage string
	log.ErrorDepth = func(_ int, msg string, _ ...slog.Attr) {
		logMessage = msg
	}

	gl.Error("error message")
	require.Contains(t, logMessage, "error message")

	logMessage = ""
	gl.Errorln("error message line")
	require.Contains(t, logMessage, "error message line")

	logMessage = ""
	gl.Errorf("this is a %s error message", "formatted")
	require.Contains(t, logMessage, "this is a formatted error message")
}
