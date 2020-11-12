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
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"syscall"
	"testing"
	"time"
)

type logMessage struct {
	val string
}

func (l *logMessage) Format(params url.Values) string {
	return l.val + "\n"
}

func testLogf(w io.Writer, params url.Values, m interface{}) error {
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

	go http.Serve(l, nil)

	logger := New("logger", 1)
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
	logger := New("logger", 1)

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
			if want, got := msg+"\n", (<-ch).(*logMessage).Format(nil); got != want {
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
				got = append(got, msg.(*logMessage).Format(nil))
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
	logger := New("logger", 10)

	dir, err := ioutil.TempDir("", "streamlog_file")
	if err != nil {
		t.Fatalf("error getting tempdir: %v", err)
	}

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
	contents, _ := ioutil.ReadFile(logPath)
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
	contents, _ = ioutil.ReadFile(rotatedPath)
	got = string(contents)
	if want != string(got) {
		t.Errorf("streamlog file: want %q got %q", want, got)
	}

	// Send the rotate signal which should reopen the original file path
	// for new logs to go to
	syscall.Kill(syscall.Getpid(), syscall.SIGUSR2)
	time.Sleep(10 * time.Millisecond)

	logger.Send(&logMessage{"test 4"})
	time.Sleep(10 * time.Millisecond)

	want = "test 1\ntest 2\ntest 3\n"
	contents, _ = ioutil.ReadFile(rotatedPath)
	got = string(contents)
	if want != string(got) {
		t.Errorf("streamlog file: want %q got %q", want, got)
	}

	want = "test 4\n"
	contents, _ = ioutil.ReadFile(logPath)
	got = string(contents)
	if want != string(got) {
		t.Errorf("streamlog file: want %q got %q", want, got)
	}
}
