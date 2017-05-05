/*
Copyright 2017 Google Inc.

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
