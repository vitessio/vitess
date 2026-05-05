/*
Copyright 2019 The Vitess Authors.

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

package streamlog

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/servenv"
)

type logMessage struct {
	val string
}

func (l *logMessage) Format(params url.Values) string {
	return l.val + "\n"
}

func testLogf(w io.Writer, params url.Values, m any) error {
	_, err := io.WriteString(w, m.(*logMessage).Format(params))
	return err
}

func TestHTTP(t *testing.T) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- servenv.HTTPServe(l)
	}()
	defer func() {
		l.Close()
		// Wait for HTTPServe to return; ignore errClosed-style errors triggered by l.Close().
		<-serveErrCh
	}()

	logger := New[*logMessage]("logger", 1)
	logger.ServeLogs("/log", testLogf)

	// This should not block - there are no subscribers yet.
	logger.Send(&logMessage{"val1"})

	// Subscribe.
	resp, err := http.Get(fmt.Sprintf("http://%s/log", addr))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()
	body := bufio.NewReader(resp.Body)
	assert.Lenf(t, logger.subscribed, 1, "want 1, got %d", len(logger.subscribed))

	// Send some messages.
	for i := range 10 {
		msg := fmt.Sprint("msg", i)
		logger.Send(&logMessage{msg})
		val, err := body.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 && val == "val1\n" {
			// the value that was sent has been in the
			// channels and was actually processed after the
			// subscription took effect. This is fine.
			val, err = body.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
		}
		assert.Equalf(t, msg+"\n", val, "want %q, got %q", msg, val)
	}

	// Shutdown.
	resp.Body.Close()
	resp = nil
	body = nil

	// Due to multiple layers of buffering in http.Server, we must
	// send multiple messages to detect the client has gone away.
	// 4 seems to be a minimum, but doesn't always work. So 10 it is.
	logger.mu.Lock()
	assert.Lenf(t, logger.subscribed, 1, "len(logger.subscribed) = %v, want %v", len(logger.subscribed), 1)
	logger.mu.Unlock()
	for range 10 {
		logger.Send(&logMessage{"val3"})
		// Allow time for propagation (loopback interface - expected to be fast).
		time.Sleep(1 * time.Millisecond)
	}
	logger.mu.Lock()
	assert.Emptyf(t, logger.subscribed, "len(logger.subscribed) = %v, want %v", len(logger.subscribed), 0)
	logger.mu.Unlock()
}

func TestChannel(t *testing.T) {
	logger := New[*logMessage]("logger", 1)

	// Subscribe.
	ch := logger.Subscribe("test")
	defer func() {
		if ch != nil {
			logger.Unsubscribe(ch)
		}
	}()
	assert.Lenf(t, logger.subscribed, 1, "want 1, got %d", len(logger.subscribed))

	// Send/receive some messages, one at a time.
	for i := range 10 {
		msg := fmt.Sprint("msg", i)
		done := make(chan struct{})
		go func() {
			got := (<-ch).Format(nil)
			assert.Equalf(t, msg+"\n", got, "Unexpected message in log. got: %q, want: %q", got, msg+"\n")
			close(done)
		}()
		logger.Send(&logMessage{msg})
		<-done
	}

	// Send/receive many messages with asynchronous writer/reader.
	want := []string{"msg0", "msg1", "msg2", "msg3", "msg4", "msg5"}
	got := make([]string, 0, len(want))
	readDone := make(chan struct{})
	writeDone := make(chan struct{})
	go func() {
		for {
			select {
			case msg := <-ch:
				got = append(got, msg.Format(nil))
			case <-writeDone:
				close(readDone)
				return
			}
		}
	}()
	for _, x := range want {
		logger.Send(&logMessage{x})
		// Allow propagation delay (cpu/memory-bound - expected to be very fast).
		time.Sleep(1 * time.Millisecond)
	}
	close(writeDone)
	<-readDone
	if assert.Lenf(t, got, len(want), "Bad results length: got %d, want %d", len(got), len(want)) {
		for i := range want {
			assert.Equalf(t, want[i]+"\n", got[i], "Unexpected result in log: got %q, want %q", got[i], want[i]+"\n")
		}
	}

	// Shutdown.
	logger.Unsubscribe(ch)
	ch = nil
	assert.Emptyf(t, logger.subscribed, "want 0, got %d", len(logger.subscribed))
}

func TestFile(t *testing.T) {
	logger := New[*logMessage]("logger", 10)

	dir := t.TempDir()

	logPath := path.Join(dir, "test.log")
	logChan, err := logger.LogToFile(logPath, testLogf)
	defer logger.Unsubscribe(logChan)
	assert.NoError(t, err)

	logger.Send(&logMessage{"test 1"})
	logger.Send(&logMessage{"test 2"})

	// Allow time for propagation
	time.Sleep(100 * time.Millisecond)

	want := "test 1\ntest 2\n"
	contents, _ := os.ReadFile(logPath)
	got := string(contents)
	assert.Equalf(t, want, got, "streamlog file")

	// Rename and send another log which should go to the renamed file
	rotatedPath := path.Join(dir, "test.log.1")
	os.Rename(logPath, rotatedPath)

	logger.Send(&logMessage{"test 3"})
	time.Sleep(100 * time.Millisecond)

	want = "test 1\ntest 2\ntest 3\n"
	contents, _ = os.ReadFile(rotatedPath)
	got = string(contents)
	assert.Equalf(t, want, got, "streamlog file")

	// Send the rotate signal which should reopen the original file path
	// for new logs to go to
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR2); err != nil {
		t.Logf("failed to send streamlog rotate signal: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	logger.Send(&logMessage{"test 4"})
	time.Sleep(100 * time.Millisecond)

	want = "test 1\ntest 2\ntest 3\n"
	contents, _ = os.ReadFile(rotatedPath)
	got = string(contents)
	assert.Equalf(t, want, got, "streamlog file")

	want = "test 4\n"
	contents, _ = os.ReadFile(logPath)
	got = string(contents)
	assert.Equalf(t, want, got, "streamlog file")
}

func TestShouldSampleQuery(t *testing.T) {
	qlConfig := QueryLogConfig{sampleRate: -1}
	assert.False(t, qlConfig.shouldSampleQuery())

	qlConfig.sampleRate = 0
	assert.False(t, qlConfig.shouldSampleQuery())

	qlConfig.sampleRate = 1.0
	assert.True(t, qlConfig.shouldSampleQuery())

	qlConfig.sampleRate = 100.0
	assert.True(t, qlConfig.shouldSampleQuery())
}

func TestShouldEmitLog(t *testing.T) {
	tests := []struct {
		sql               string
		qLogFilterTag     string
		qLogRowThreshold  uint64
		qLogTimeThreshold time.Duration
		qLogSampleRate    float64
		qLogMode          string
		rowsAffected      uint64
		rowsReturned      uint64
		totalTime         time.Duration
		errored           bool
		ok                bool
		emitReason        string
	}{
		{
			sql:               "queryLogRowThreshold smaller than affected and returned",
			qLogFilterTag:     "",
			qLogRowThreshold:  2,
			qLogTimeThreshold: 0,
			qLogSampleRate:    0.0,
			rowsAffected:      7,
			rowsReturned:      7,
			totalTime:         1000,
			ok:                true,
			emitReason:        "row",
		},
		{
			sql:               "queryLogRowThreshold greater than affected and returned",
			qLogFilterTag:     "",
			qLogRowThreshold:  27,
			qLogTimeThreshold: 0,
			qLogSampleRate:    0.0,
			rowsAffected:      7,
			rowsReturned:      17,
			totalTime:         1000,
			ok:                false,
			emitReason:        "",
		},
		{
			sql:               "queryLogTimeThreshold smaller than total time and returned",
			qLogFilterTag:     "",
			qLogRowThreshold:  0,
			qLogTimeThreshold: 10,
			qLogSampleRate:    0.0,
			rowsAffected:      7,
			rowsReturned:      7,
			totalTime:         1000,
			ok:                true,
			emitReason:        "time",
		},
		{
			sql:               "queryLogTimeThreshold greater than total time and returned",
			qLogFilterTag:     "",
			qLogRowThreshold:  0,
			qLogTimeThreshold: 10000,
			qLogSampleRate:    0.0,
			rowsAffected:      7,
			rowsReturned:      17,
			totalTime:         1000,
			ok:                false,
			emitReason:        "",
		},
		{
			sql:               "this doesn't contains queryFilterTag: TAG",
			qLogFilterTag:     "special tag",
			qLogRowThreshold:  10,
			qLogTimeThreshold: 0,
			qLogSampleRate:    0.0,
			rowsAffected:      7,
			rowsReturned:      17,
			totalTime:         1000,
			ok:                false,
			emitReason:        "",
		},
		{
			sql:               "this contains queryFilterTag: TAG",
			qLogFilterTag:     "TAG",
			qLogRowThreshold:  0,
			qLogTimeThreshold: 0,
			qLogSampleRate:    0.0,
			rowsAffected:      7,
			rowsReturned:      17,
			totalTime:         1000,
			ok:                true,
			emitReason:        "filtertag",
		},
		{
			sql:               "this contains querySampleRate: 1.0",
			qLogFilterTag:     "",
			qLogRowThreshold:  0,
			qLogTimeThreshold: 0,
			qLogSampleRate:    1.0,
			rowsAffected:      7,
			rowsReturned:      17,
			totalTime:         1000,
			ok:                true,
			emitReason:        "sample",
		},
		{
			sql:               "this contains querySampleRate: 1.0 without expected queryFilterTag",
			qLogFilterTag:     "TAG",
			qLogRowThreshold:  0,
			qLogTimeThreshold: 0,
			qLogSampleRate:    1.0,
			rowsAffected:      7,
			rowsReturned:      17,
			totalTime:         1000,
			ok:                true,
			emitReason:        "sample",
		},
		{
			sql:        "log only error - no error",
			qLogMode:   "error",
			errored:    false,
			ok:         false,
			emitReason: "",
		},
		{
			sql:        "log only error - errored",
			qLogMode:   "error",
			errored:    true,
			ok:         true,
			emitReason: "error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			qlConfig := QueryLogConfig{
				FilterTag:     tt.qLogFilterTag,
				RowThreshold:  tt.qLogRowThreshold,
				TimeThreshold: tt.qLogTimeThreshold,
				sampleRate:    tt.qLogSampleRate,
				Mode:          tt.qLogMode,
			}
			shouldEmit, emitReason := qlConfig.ShouldEmitLog(tt.sql, tt.rowsAffected, tt.rowsReturned, tt.totalTime, tt.errored)
			require.Equal(t, tt.ok, shouldEmit)
			require.Equal(t, tt.emitReason, emitReason)
		})
	}
}

func TestShouldEmitAnyLog(t *testing.T) {
	tests := []struct {
		sql               string
		qLogFilterTag     string
		qLogRowThreshold  uint64
		qLogTimeThreshold time.Duration
		qLogSampleRate    float64
		qLogMode          string
		rowsAffected      uint64
		rowsReturned      uint64
		totalTime         time.Duration
		errored           bool
		ok                bool
		emitReason        string
	}{
		{
			sql:               "queryLogRowThreshold smaller than affected and returned",
			qLogFilterTag:     "",
			qLogRowThreshold:  2,
			qLogTimeThreshold: 0,
			qLogSampleRate:    0.0,
			rowsAffected:      7,
			rowsReturned:      7,
			totalTime:         1000,
			ok:                true,
			emitReason:        "row",
		},
		{
			sql:               "queryLogRowThreshold greater than affected and returned",
			qLogFilterTag:     "",
			qLogRowThreshold:  27,
			qLogTimeThreshold: 0,
			qLogSampleRate:    0.0,
			rowsAffected:      7,
			rowsReturned:      17,
			totalTime:         1000,
			ok:                false,
			emitReason:        "",
		},
		{
			sql:               "queryLogTimeThreshold smaller than total time and returned",
			qLogFilterTag:     "",
			qLogRowThreshold:  0,
			qLogTimeThreshold: 10,
			qLogSampleRate:    0.0,
			rowsAffected:      7,
			rowsReturned:      7,
			totalTime:         1000,
			ok:                true,
			emitReason:        "time",
		},
		{
			sql:               "queryLogTimeThreshold greater than total time and returned",
			qLogFilterTag:     "",
			qLogRowThreshold:  100,
			qLogTimeThreshold: 10000,
			qLogSampleRate:    0.0,
			rowsAffected:      7,
			rowsReturned:      17,
			totalTime:         1000,
			ok:                false,
			emitReason:        "",
		},
		{
			sql:               "this doesn't contains queryFilterTag: TAG",
			qLogFilterTag:     "special tag",
			qLogRowThreshold:  10,
			qLogTimeThreshold: 0,
			qLogSampleRate:    0.0,
			rowsAffected:      7,
			rowsReturned:      17,
			totalTime:         1000,
			ok:                true,
			emitReason:        "row",
		},
		{
			sql:               "this contains queryFilterTag: TAG",
			qLogFilterTag:     "TAG",
			qLogRowThreshold:  100,
			qLogTimeThreshold: 0,
			qLogSampleRate:    0.0,
			rowsAffected:      7,
			rowsReturned:      17,
			totalTime:         1000,
			ok:                true,
			emitReason:        "filtertag",
		},
		{
			sql:               "this contains queryFilterTag: TAG and queryLogTimeThreshold",
			qLogFilterTag:     "TAG",
			qLogRowThreshold:  100,
			qLogTimeThreshold: 10,
			qLogSampleRate:    0.0,
			rowsAffected:      7,
			rowsReturned:      17,
			totalTime:         20,
			ok:                true,
			emitReason:        "filtertag,time",
		},
		{
			sql:               "this contains querySampleRate: 1.0",
			qLogFilterTag:     "",
			qLogRowThreshold:  0,
			qLogTimeThreshold: 0,
			qLogSampleRate:    1.0,
			rowsAffected:      7,
			rowsReturned:      17,
			totalTime:         1000,
			ok:                true,
			emitReason:        "sample",
		},
		{
			sql:               "this contains querySampleRate: 1.0 without expected queryFilterTag",
			qLogFilterTag:     "TAG",
			qLogRowThreshold:  0,
			qLogTimeThreshold: 0,
			qLogSampleRate:    1.0,
			rowsAffected:      7,
			rowsReturned:      17,
			totalTime:         1000,
			ok:                true,
			emitReason:        "sample",
		},
		{
			sql:        "log only error - no error",
			qLogMode:   "error",
			errored:    false,
			ok:         false,
			emitReason: "",
		},
		{
			sql:        "log only error - errored",
			qLogMode:   "error",
			errored:    true,
			ok:         true,
			emitReason: "error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			qlConfig := QueryLogConfig{
				FilterTag:             tt.qLogFilterTag,
				RowThreshold:          tt.qLogRowThreshold,
				TimeThreshold:         tt.qLogTimeThreshold,
				sampleRate:            tt.qLogSampleRate,
				Mode:                  tt.qLogMode,
				EmitOnAnyConditionMet: true,
			}
			shouldEmit, emitReason := qlConfig.ShouldEmitLog(tt.sql, tt.rowsAffected, tt.rowsReturned, tt.totalTime, tt.errored)
			require.Equal(t, tt.ok, shouldEmit)
			require.Equal(t, tt.emitReason, emitReason)
		})
	}
}

func TestGetFormatter(t *testing.T) {
	tests := []struct {
		name           string
		logger         *StreamLogger[string]
		params         url.Values
		val            any
		expectedErr    string
		expectedOutput string
	}{
		{
			name: "unexpected value error",
			logger: &StreamLogger[string]{
				name: "test-logger",
			},
			params: url.Values{
				"keys": []string{"key1", "key2"},
			},
			val:            "temp val",
			expectedOutput: "Error: unexpected value of type string in test-logger!",
			expectedErr:    "",
		},
		{
			name: "mock formatter",
			logger: &StreamLogger[string]{
				name: "test-logger",
			},
			params: url.Values{
				"keys": []string{"key1", "key2"},
			},
			val:         &mockFormatter{err: errors.New("formatter error")},
			expectedErr: "formatter error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			logFormatterFunc := GetFormatter[string](tt.logger)
			err := logFormatterFunc(&buffer, tt.params, tt.val)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				require.Equal(t, tt.expectedOutput, buffer.String())
			} else {
				require.ErrorContains(t, err, tt.expectedErr)
			}
		})
	}
}

type mockFormatter struct {
	called bool
	err    error
}

func (mf *mockFormatter) Logf(w io.Writer, params url.Values) error {
	mf.called = true
	return mf.err
}
