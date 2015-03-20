// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proc

import (
	"expvar"
	"fmt"
	"net"
	"testing"
)

func TestPublished(t *testing.T) {
	l, err := Listen("")
	if err != nil {
		t.Fatal(err)
	}
	opened := make(chan struct{})
	closed := make(chan struct{})
	go func() {
		for {
			conn, err := l.Accept()
			opened <- struct{}{}
			if err != nil {
				t.Fatal(err)
			}
			go func() {
				b := make([]byte, 100)
				for {
					_, err := conn.Read(b)
					if err != nil {
						conn.Close()
						closed <- struct{}{}
						return
					}
				}
			}()
		}
	}()

	addr := l.Addr().String()
	for i := 1; i <= 3; i++ {
		conn1, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		<-opened
		if v := expvar.Get("ConnCount").String(); v != "1" {
			t.Errorf("ConnCount: %v, want 1", v)
		}
		conn1.Close()
		<-closed
		if v := expvar.Get("ConnCount").String(); v != "0" {
			t.Errorf("ConnCount: %v, want 1", v)
		}
		if v := expvar.Get("ConnAccepted").String(); v != fmt.Sprintf("%d", i) {
			t.Errorf("ConnAccepted: %v, want %d", v, i)
		}
	}
}
