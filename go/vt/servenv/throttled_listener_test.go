// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package servenv

import (
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestThrottle(t *testing.T) {
	l, err := net.Listen("tcp", "")
	if err != nil {
		t.Fatalf("could not initialize listener: %v", err)
	}
	throttled := NewThrottledListener(l, 10, 3)
	go http.Serve(throttled, nil)

	start := time.Now()
	var wg sync.WaitGroup
	for i := 1; i <= 3; i++ {
		conn, err := net.Dial("tcp", l.Addr().String())
		if err != nil {
			t.Error(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer conn.Close()
			if _, err = conn.Write([]byte("hello\n\n")); err != nil {
				t.Error(err)
			}
			b := make([]byte, 1000)
			if _, err = conn.Read(b); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	diff := time.Now().Sub(start)
	if diff < 300*time.Millisecond {
		t.Errorf("want >= 300ms, got %v", diff)
	}
	l.Close()
}

func TestReject(t *testing.T) {
	l, err := net.Listen("tcp", "")
	if err != nil {
		t.Fatalf("could not initialize listener: %v", err)
	}
	throttled := NewThrottledListener(l, 10, 3)
	go http.Serve(throttled, nil)

	start := time.Now()
	var wg sync.WaitGroup
	for i := 1; i <= 3; i++ {
		conn, err := net.Dial("tcp", l.Addr().String())
		if err != nil {
			t.Error(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer conn.Close()
			if _, err = conn.Write([]byte("hello\n\n")); err != nil {
				t.Error(err)
			}
			b := make([]byte, 1000)
			if _, err = conn.Read(b); err != nil {
				t.Error(err)
			}
		}()
	}
	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Error(err)
	}
	if _, err = conn.Write([]byte("hello\n\n")); err != nil {
		t.Error(err)
	}
	b := make([]byte, 1000)
	if _, err = conn.Read(b); err == nil {
		t.Errorf("want error, got nil")
	}
	t.Log(err)
	wg.Wait()
	diff := time.Now().Sub(start)
	if diff < 300*time.Millisecond {
		t.Errorf("want >= 300ms, got %v", diff)
	}
	l.Close()
}
