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

package vreplication

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestVrLog(t *testing.T) {
	r, _ := http.NewRequest("GET", "/debug/vrlog?timeout=100&limit=10", nil)
	w := NewHTTPStreamWriterMock()

	ch := vrLogStatsLogger.Subscribe("vrlogstats")
	defer vrLogStatsLogger.Unsubscribe(ch)
	go func() {
		vrlogStatsHandler(ch, w, r)
	}()
	eventType, detail := "Test", "detail 1"
	stats := NewVrLogStats(eventType)
	stats.Send(detail)
	var s string
	select {
	case ret := <-w.ch:
		b, ok := ret.([]byte)
		if ok {
			s = string(b)
		}
	case <-time.After(1 * time.Second):
		s = "Timed out"
	}

	want := fmt.Sprintf("%s Event	%s", eventType, detail)
	if !strings.Contains(s, want) {
		t.Fatalf(fmt.Sprintf("want %s, got %s", want, s))
	}
	if strings.HasSuffix(s, "\\n") {
		t.Fatalf("does not end in a newline: %s", s)
	}
	s = strings.TrimSuffix(s, "\n")
	ss := strings.Split(s, "	")
	numCols := 4
	if ss == nil || len(ss) != numCols {
		t.Fatalf("log line should have %d columns, not %d, : %s", numCols, len(ss), strings.Join(ss, "|"))
	}
	lastColValue, err := strconv.Atoi(ss[len(ss)-1])
	if err != nil {
		t.Fatalf("Duration is not an integer: %s", err)
	}
	if lastColValue == 0 {
		t.Fatalf("Duration should not be zero")
	}

	stats = &VrLogStats{}
	stats.Send("detail123")

	select {
	case ret := <-w.ch:
		b, ok := ret.([]byte)
		if ok {
			s = string(b)
		}
	case <-time.After(1 * time.Second):
		s = "Timed out"
	}
	prefix := "Error: Type not specified"
	if !strings.HasPrefix(s, prefix) {
		t.Fatalf("Incorrect Type for uninitialized stat, got %v", s)
	}
}
