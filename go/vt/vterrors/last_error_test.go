/*
Copyright 2022 The Vitess Authors.

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

package vterrors

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
)

const (
	shortWait      = 1 * time.Millisecond
	longWait       = 150 * time.Millisecond
	maxTimeInError = 100 * time.Millisecond
)

// TestLastErrorZeroMaxTime tests maxTimeInError = 0, should always retry
func TestLastErrorZeroMaxTime(t *testing.T) {
	le := NewLastError("test", 0)
	err1 := errors.New("error1")
	le.Record(err1)
	require.True(t, le.ShouldRetry())
	time.Sleep(shortWait)
	require.True(t, le.ShouldRetry())
	time.Sleep(longWait)
	require.True(t, le.ShouldRetry())
}

// TestLastErrorNoError ensures that an uninitialized lastError always retries
func TestLastErrorNoError(t *testing.T) {
	le := NewLastError("test", maxTimeInError)
	require.True(t, le.ShouldRetry())
	err1 := errors.New("error1")
	le.Record(err1)
	require.True(t, le.ShouldRetry())
	le.Record(nil)
	require.True(t, le.ShouldRetry())
}

// TestLastErrorOneError validates that we retry an error if happening within the maxTimeInError, but not after
func TestLastErrorOneError(t *testing.T) {
	le := NewLastError("test", maxTimeInError)
	err1 := errors.New("error1")
	le.Record(err1)
	require.True(t, le.ShouldRetry())
	time.Sleep(shortWait)
	require.True(t, le.ShouldRetry())
	time.Sleep(shortWait)
	require.True(t, le.ShouldRetry())
	time.Sleep(longWait)
	require.False(t, le.ShouldRetry())
}

// TestLastErrorRepeatedError confirms that if same error is repeated we don't retry
// unless it happens after maxTimeInError
func TestLastErrorRepeatedError(t *testing.T) {
	le := NewLastError("test", maxTimeInError)
	err1 := errors.New("error1")
	le.Record(err1)
	require.True(t, le.ShouldRetry())
	for i := 1; i < 10; i++ {
		le.Record(err1)
		time.Sleep(shortWait)
	}
	require.True(t, le.ShouldRetry())

	// same error happens after maxTimeInError, so it should retry
	time.Sleep(longWait)
	logs := logtest.Capture(t, slog.LevelInfo)
	require.False(t, le.ShouldRetry())
	le.Record(err1)
	require.True(t, le.ShouldRetry())

	record := logs.Record(t, "the same error was encountered continuously, it is now assumed to be unrecoverable; any affected operations will need to be manually restarted once the error has been addressed")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "test", record["last_error_name"])
	assert.Equal(t, "error1", record["error"])
	firstSeen, err := time.Parse(time.RFC3339Nano, record["first_seen"].(string))
	require.NoError(t, err)
	assert.Equal(t, time.UTC, firstSeen.Location())

	record = logs.Record(t, "Resetting firstSeen, since it is too long since the last one")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, "test", record["last_error_name"])
}
