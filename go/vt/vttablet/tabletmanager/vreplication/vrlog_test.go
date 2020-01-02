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
	stats := VrLogStats{Ctx: context.Background()}
	msg := "test msg"
	stats.Detail = msg
	stats.Send()
	time.Sleep(1 * time.Millisecond)
	s := w.Body.String()

	if !strings.Contains(s, msg) { //we use Contains since the printed log is in html and also prepends current time
		t.Fatalf(fmt.Sprintf("want %s, got %s", msg, s))
	}

}
