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

package tabletserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"

	"vitess.io/vitess/go/vt/callerid"

	"vitess.io/vitess/go/streamlog"
)

func testNotRedacted(t *testing.T, r *httptest.ResponseRecorder) {
	if strings.Contains(r.Body.String(), "redacted") {
		t.Errorf("/debug/txlogz unexpectedly redacted")
	}
}

func testRedacted(t *testing.T, r *httptest.ResponseRecorder) {
	if !strings.Contains(r.Body.String(), "redacted") {
		t.Errorf("/debug/txlogz unexpectedly not redacted")
	}
}

func testHandler(req *http.Request, t *testing.T) {
	// Test with redactions off to start
	streamlog.SetRedactDebugUIQueries(false)

	response := httptest.NewRecorder()
	tabletenv.TxLogger.Send("test msg")
	txlogzHandler(response, req)
	if !strings.Contains(response.Body.String(), "error") {
		t.Fatalf("should show an error page since transaction log format is invalid.")
	}
	txConn := &StatefulConnection{
		ConnID: 123456,
		txProps: &tx.Properties{
			EffectiveCaller: callerid.NewEffectiveCallerID("effective-caller", "component", "subcomponent"),
			ImmediateCaller: callerid.NewImmediateCallerID("immediate-caller"),
			StartTime:       time.Now(),
			Conclusion:      "unknown",
			Queries:         []string{"select * from test"},
		},
	}
	txConn.txProps.EndTime = txConn.txProps.StartTime
	response = httptest.NewRecorder()
	tabletenv.TxLogger.Send(txConn)
	txlogzHandler(response, req)
	testNotRedacted(t, response)
	txConn.txProps.EndTime = txConn.txProps.StartTime.Add(time.Duration(2) * time.Second)
	response = httptest.NewRecorder()
	tabletenv.TxLogger.Send(txConn)
	txlogzHandler(response, req)
	testNotRedacted(t, response)
	txConn.txProps.EndTime = txConn.txProps.StartTime.Add(time.Duration(500) * time.Millisecond)
	response = httptest.NewRecorder()
	tabletenv.TxLogger.Send(txConn)
	txlogzHandler(response, req)
	testNotRedacted(t, response)

	// Test with redactions on
	streamlog.SetRedactDebugUIQueries(true)
	txlogzHandler(response, req)
	testRedacted(t, response)

	// Reset to default redaction state
	streamlog.SetRedactDebugUIQueries(false)
}

func TestTxlogzHandler(t *testing.T) {
	req, _ := http.NewRequest("GET", "/txlogz?timeout=0&limit=10", nil)
	testHandler(req, t)
}

func TestTxlogzHandlerWithNegativeTimeout(t *testing.T) {
	req, _ := http.NewRequest("GET", "/txlogz?timeout=-1&limit=10", nil)
	testHandler(req, t)
}

func TestTxlogzHandlerWithLargeLimit(t *testing.T) {
	req, _ := http.NewRequest("GET", "/txlogz?timeout=0&limit=10000000", nil)
	testHandler(req, t)
}
