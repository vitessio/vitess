// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package servenv

import (
	"io/ioutil"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestThrottle(t *testing.T) {
	l, err := net.Listen("tcp", ":12345")
	if err != nil {
		t.Fatalf("could not initialize listener: %v", err)
	}
	throttled := NewThrottledListener(l, 100)
	go http.Serve(throttled, nil)

	start := time.Now()
	for i := 1; i <= 10; i++ {
		resp, err := http.Get("http://localhost:12345/debug/vars")
		if err != nil {
			t.Fatal(err)
		}
		_, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	}
	diff := time.Now().Sub(start)
	if diff < 100*time.Millisecond {
		t.Errorf("want >= 1s, got %v", diff)
	}
	l.Close()
}
