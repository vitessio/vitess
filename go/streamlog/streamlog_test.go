// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package streamlog

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/youtube/vitess/go/sync2"
)

type logMessage struct {
	val string
}

func (l *logMessage) Format(params url.Values) string {
	return l.val + "\n"
}

func TestHTTP(t *testing.T) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	go http.Serve(l, nil)

	logger := New("logger", 1)
	logger.ServeLogs("/log", func(params url.Values, x interface{}) string { return x.(*logMessage).Format(params) })

	// This should not block
	logger.Send(&logMessage{"val1"})

	lastValue := sync2.AtomicString{}
	svm := sync2.ServiceManager{}
	svm.Go(func(_ *sync2.ServiceManager) {
		resp, err := http.Get(fmt.Sprintf("http://%s/log", addr))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		buf := make([]byte, 100)
		for svm.State() == sync2.SERVICE_RUNNING {
			n, err := resp.Body.Read(buf)
			if err != nil {
				t.Fatal(err)
			}
			lastValue.Set(string(buf[:n]))
		}
	})

	time.Sleep(100 * time.Millisecond)
	if sz := logger.size.Get(); sz != 1 {
		t.Errorf("want 1, got %d", sz)
	}
	logger.Send(&logMessage{"val2"})
	time.Sleep(100 * time.Millisecond)
	if lastValue.Get() != "val2\n" {
		t.Errorf("want val2\\n, got %q", lastValue.Get())
	}

	// This part of the test is flaky.
	// Uncomment for one-time testing.
	/*
		// This send will unblock the http client
		// which will allow it to see the stop request
		// from svm.
		go logger.Send(&logMessage{"val3"})
		svm.Stop()
		// You have to send a few times before the writer
		// returns an error.
		for i := 0; i < 10; i++ {
			time.Sleep(100 * time.Millisecond)
			logger.Send(&logMessage{"val4"})
		}
		time.Sleep(100 * time.Millisecond)
		if sz := logger.size.Get(); sz != 0 {
			t.Errorf("want 0, got %d", sz)
		}
	*/
}

func TestChannel(t *testing.T) {
	logger := New("logger", 1)

	lastValue := sync2.AtomicString{}
	svm := sync2.ServiceManager{}
	svm.Go(func(_ *sync2.ServiceManager) {
		ch := logger.Subscribe()
		defer logger.Unsubscribe(ch)
		for svm.State() == sync2.SERVICE_RUNNING {
			lastValue.Set((<-ch).(*logMessage).Format(nil))
		}
	})

	time.Sleep(10 * time.Millisecond)
	if sz := logger.size.Get(); sz != 1 {
		t.Errorf("want 1, got %d", sz)
	}
	logger.Send(&logMessage{"val2"})
	time.Sleep(10 * time.Millisecond)
	if lastValue.Get() != "val2\n" {
		t.Errorf("want val2\\n, got %q", lastValue.Get())
	}

	go logger.Send(&logMessage{"val3"})
	svm.Stop()
	time.Sleep(10 * time.Millisecond)
	if sz := logger.size.Get(); sz != 0 {
		t.Errorf("want 0, got %d", sz)
	}
}
