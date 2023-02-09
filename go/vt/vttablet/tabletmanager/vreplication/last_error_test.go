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

package vreplication

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const shortWait = 1 * time.Millisecond
const longWait = 150 * time.Millisecond
const maxTimeInError = 100 * time.Millisecond

// TestLastErrorZeroMaxTime tests maxTimeInError = 0, should always retry
func TestLastErrorZeroMaxTime(t *testing.T) {
	le := newLastError("test", 0)
	err1 := fmt.Errorf("error1")
	le.record(err1)
	require.True(t, le.shouldRetry())
	time.Sleep(shortWait)
	require.True(t, le.shouldRetry())
	time.Sleep(longWait)
	require.True(t, le.shouldRetry())
}

// TestLastErrorNoError ensures that an uninitialized lastError always retries
func TestLastErrorNoError(t *testing.T) {
	le := newLastError("test", maxTimeInError)
	require.True(t, le.shouldRetry())
	err1 := fmt.Errorf("error1")
	le.record(err1)
	require.True(t, le.shouldRetry())
	le.record(nil)
	require.True(t, le.shouldRetry())
}

// TestLastErrorOneError validates that we retry an error if happening within the maxTimeInError, but not after
func TestLastErrorOneError(t *testing.T) {
	le := newLastError("test", maxTimeInError)
	err1 := fmt.Errorf("error1")
	le.record(err1)
	require.True(t, le.shouldRetry())
	time.Sleep(shortWait)
	require.True(t, le.shouldRetry())
	time.Sleep(shortWait)
	require.True(t, le.shouldRetry())
	time.Sleep(longWait)
	require.False(t, le.shouldRetry())
}

// TestLastErrorRepeatedError confirms that if same error is repeated we don't retry
// unless it happens after maxTimeInError
func TestLastErrorRepeatedError(t *testing.T) {
	le := newLastError("test", maxTimeInError)
	err1 := fmt.Errorf("error1")
	le.record(err1)
	require.True(t, le.shouldRetry())
	for i := 1; i < 10; i++ {
		le.record(err1)
		time.Sleep(shortWait)
	}
	require.True(t, le.shouldRetry())

	// same error happens after maxTimeInError, so it should retry
	time.Sleep(longWait)
	require.False(t, le.shouldRetry())
	le.record(err1)
	require.True(t, le.shouldRetry())
}
