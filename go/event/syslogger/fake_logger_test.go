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

package syslogger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetLogsForNoLogs(t *testing.T) {
	tl := NewTestLogger()
	errLoggerMsg := tl.getLog()

	want := loggerMsg{
		msg:   "no logs!",
		level: "ERROR",
	}

	assert.Equal(t, errLoggerMsg, want)
}

func TestGetAllLogs(t *testing.T) {
	tl := NewTestLogger()
	tl.recordInfof("Test info log")
	tl.recordErrorf("Test error log")
	tl.recordWarningf("Test warning log")

	want := []string{"INFO:Test info log", "ERROR:Test error log", "WARNING:Test warning log"}
	loggerMsgs := tl.GetAllLogs()

	assert.Equal(t, loggerMsgs, want)
}
