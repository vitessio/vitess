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
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestVrLog(t *testing.T) {
	r, _ := http.NewRequest("GET", "/debug/vrlog?timeout=10&limit=1", nil)
	w := httptest.NewRecorder()

	ch := vrLogStatsLogger.Subscribe("vrlog")
	defer vrLogStatsLogger.Unsubscribe(ch)
	go func() {
		vrlogStatsHandler(ch, w, r)
	}()
	ctx := context.Background()
	eventType, detail := "Test", "detail 1"
	stats := NewVrLogStats(ctx, eventType)
	time.Sleep(1 * time.Millisecond)
	stats.Record(detail)
	time.Sleep(1 * time.Millisecond)
	s := w.Body.String()
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
	if lastColValue < 1<<9 {
		t.Fatalf("Waited 1 Millisecond, so duration should be greater than that: %d, %s", lastColValue, ss[len(ss)-1])
	}
	stats = &VrLogStats{}
	if stats.Record("should error out since stats is not initalized") != false {
		t.Fatalf("Uninitialized stats should not log")
	}
}
