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

package logutil

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriterLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWriterLogger(&buf)

	logger.Infof("info message %d", 1)
	logger.Warningf("warning message %s", "test")
	logger.Errorf("error message")

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	require.Len(t, lines, 3)

	assert.True(t, strings.HasPrefix(lines[0], "I "), "expected info line to start with 'I ', got: %s", lines[0])
	assert.Contains(t, lines[0], "info message 1")

	assert.True(t, strings.HasPrefix(lines[1], "W "), "expected warning line to start with 'W ', got: %s", lines[1])
	assert.Contains(t, lines[1], "warning message test")

	assert.True(t, strings.HasPrefix(lines[2], "E "), "expected error line to start with 'E ', got: %s", lines[2])
	assert.Contains(t, lines[2], "error message")
}

func TestWriterLoggerDepthMethods(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWriterLogger(&buf)

	logger.InfoDepth(0, "depth info")
	logger.WarningDepth(0, "depth warning")
	logger.ErrorDepth(0, "depth error")

	output := buf.String()
	assert.Contains(t, output, "I ")
	assert.Contains(t, output, "depth info")
	assert.Contains(t, output, "W ")
	assert.Contains(t, output, "depth warning")
	assert.Contains(t, output, "E ")
	assert.Contains(t, output, "depth error")
}

func TestWriterLoggerError(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWriterLogger(&buf)

	err := errors.New("test error")
	logger.Error(err)
	assert.Contains(t, buf.String(), "test error")
}

func TestWriterLoggerErrorf2(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWriterLogger(&buf)

	err := errors.New("root cause")
	logger.Errorf2(err, "wrapping context")
	output := buf.String()
	assert.Contains(t, output, "wrapping context")
	assert.Contains(t, output, "root cause")
}

func TestWriterLoggerPrintf(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWriterLogger(&buf)

	logger.Printf("raw output %d", 42)
	assert.Equal(t, "raw output 42", buf.String())
}

func TestWriterLoggerThreadSafety(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWriterLogger(&buf)

	done := make(chan struct{})
	for i := range 10 {
		go func(n int) {
			defer func() { done <- struct{}{} }()
			for j := range 100 {
				logger.Infof("goroutine %d iteration %d", n, j)
			}
		}(i)
	}

	for range 10 {
		<-done
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	assert.Len(t, lines, 1000)
}
