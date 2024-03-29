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
	defer l.Close()
	addr := l.Addr().String()

	go func() {
		err := servenv.HTTPServe(l)
		if err != nil {
			t.Errorf("http serve returned unexpected error: %v", err)
		}
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
	if sz := len(logger.subscribed); sz != 1 {
		t.Errorf("want 1, got %d", sz)
	}

	// Send some messages.
	for i := 0; i < 10; i++ {
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
		if want := msg + "\n"; val != want {
			t.Errorf("want %q, got %q", msg, val)
		}
	}

	// Shutdown.
	resp.Body.Close()
	resp = nil
	body = nil

	// Due to multiple layers of buffering in http.Server, we must
	// send multiple messages to detect the client has gone away.
	// 4 seems to be a minimum, but doesn't always work. So 10 it is.
	logger.mu.Lock()
	if want, got := 1, len(logger.subscribed); want != got {
		t.Errorf("len(logger.subscribed) = %v, want %v", got, want)
	}
	logger.mu.Unlock()
	for i := 0; i < 10; i++ {
		logger.Send(&logMessage{"val3"})
		// Allow time for propagation (loopback interface - expected to be fast).
		time.Sleep(1 * time.Millisecond)
	}
	logger.mu.Lock()
	if want, got := 0, len(logger.subscribed); want != got {
		t.Errorf("len(logger.subscribed) = %v, want %v", got, want)
	}
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
	if sz := len(logger.subscribed); sz != 1 {
		t.Errorf("want 1, got %d", sz)
	}

	// Send/receive some messages, one at a time.
	for i := 0; i < 10; i++ {
		msg := fmt.Sprint("msg", i)
		done := make(chan struct{})
		go func() {
			if want, got := msg+"\n", (<-ch).Format(nil); got != want {
				t.Errorf("Unexpected message in log. got: %q, want: %q", got, want)
			}
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
	if len(got) != len(want) {
		t.Errorf("Bad results length: got %d, want %d", len(got), len(want))
	} else {
		for i := 0; i < len(want); i++ {
			if want[i]+"\n" != got[i] {
				t.Errorf("Unexpected result in log: got %q, want %q", got[i], want[i]+"\n")
			}
		}
	}

	// Shutdown.
	logger.Unsubscribe(ch)
	ch = nil
	if sz := len(logger.subscribed); sz != 0 {
		t.Errorf("want 0, got %d", sz)
	}
}

func TestFile(t *testing.T) {
	logger := New[*logMessage]("logger", 10)

	dir := t.TempDir()

	logPath := path.Join(dir, "test.log")
	logChan, err := logger.LogToFile(logPath, testLogf)
	defer logger.Unsubscribe(logChan)
	if err != nil {
		t.Errorf("error enabling file logger: %v", err)
	}

	logger.Send(&logMessage{"test 1"})
	logger.Send(&logMessage{"test 2"})

	// Allow time for propagation
	time.Sleep(10 * time.Millisecond)

	want := "test 1\ntest 2\n"
	contents, _ := os.ReadFile(logPath)
	got := string(contents)
	if want != string(got) {
		t.Errorf("streamlog file: want %q got %q", want, got)
	}

	// Rename and send another log which should go to the renamed file
	rotatedPath := path.Join(dir, "test.log.1")
	os.Rename(logPath, rotatedPath)

	logger.Send(&logMessage{"test 3"})
	time.Sleep(10 * time.Millisecond)

	want = "test 1\ntest 2\ntest 3\n"
	contents, _ = os.ReadFile(rotatedPath)
	got = string(contents)
	if want != string(got) {
		t.Errorf("streamlog file: want %q got %q", want, got)
	}

	// Send the rotate signal which should reopen the original file path
	// for new logs to go to
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR2); err != nil {
		t.Logf("failed to send streamlog rotate signal: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	logger.Send(&logMessage{"test 4"})
	time.Sleep(10 * time.Millisecond)

	want = "test 1\ntest 2\ntest 3\n"
	contents, _ = os.ReadFile(rotatedPath)
	got = string(contents)
	if want != string(got) {
		t.Errorf("streamlog file: want %q got %q", want, got)
	}

	want = "test 4\n"
	contents, _ = os.ReadFile(logPath)
	got = string(contents)
	if want != string(got) {
		t.Errorf("streamlog file: want %q got %q", want, got)
	}
}

func TestShouldEmitLog(t *testing.T) {
	origQueryLogFilterTag := queryLogFilterTag
	origQueryLogRowThreshold := queryLogRowThreshold
	defer func() {
		SetQueryLogFilterTag(origQueryLogFilterTag)
		SetQueryLogRowThreshold(origQueryLogRowThreshold)
	}()

	tests := []struct {
		sql              string
		qLogFilterTag    string
		qLogRowThreshold uint64
		rowsAffected     uint64
		rowsReturned     uint64
		ok               bool
	}{
		{
			sql:              "queryLogThreshold smaller than affected and returned",
			qLogFilterTag:    "",
			qLogRowThreshold: 2,
			rowsAffected:     7,
			rowsReturned:     7,
			ok:               true,
		},
		{
			sql:              "queryLogThreshold greater than affected and returned",
			qLogFilterTag:    "",
			qLogRowThreshold: 27,
			rowsAffected:     7,
			rowsReturned:     17,
			ok:               false,
		},
		{
			sql:              "this doesn't contains queryFilterTag: TAG",
			qLogFilterTag:    "special tag",
			qLogRowThreshold: 10,
			rowsAffected:     7,
			rowsReturned:     17,
			ok:               false,
		},
		{
			sql:              "this contains queryFilterTag: TAG",
			qLogFilterTag:    "TAG",
			qLogRowThreshold: 0,
			rowsAffected:     7,
			rowsReturned:     17,
			ok:               true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			SetQueryLogFilterTag(tt.qLogFilterTag)
			SetQueryLogRowThreshold(tt.qLogRowThreshold)

			require.Equal(t, tt.ok, ShouldEmitLog(tt.sql, tt.rowsAffected, tt.rowsReturned))
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
			val:         &mockFormatter{err: fmt.Errorf("formatter error")},
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
