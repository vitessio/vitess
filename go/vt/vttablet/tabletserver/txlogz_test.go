// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func testHandler(req *http.Request, t *testing.T) {
	response := httptest.NewRecorder()
	tabletenv.TxLogger.Send("test msg")
	txlogzHandler(response, req)
	if !strings.Contains(response.Body.String(), "error") {
		t.Fatalf("should show an error page since transaction log format is invalid.")
	}
	txConn := &TxConnection{
		TransactionID:     123456,
		StartTime:         time.Now(),
		Queries:           []string{"select * from test"},
		Conclusion:        "unknown",
		LogToFile:         sync2.AtomicInt32{},
		EffectiveCallerID: callerid.NewEffectiveCallerID("effective-caller", "component", "subcomponent"),
		ImmediateCallerID: callerid.NewImmediateCallerID("immediate-caller"),
	}
	txConn.EndTime = txConn.StartTime
	response = httptest.NewRecorder()
	tabletenv.TxLogger.Send(txConn)
	txlogzHandler(response, req)
	txConn.EndTime = txConn.StartTime.Add(time.Duration(2) * time.Second)
	response = httptest.NewRecorder()
	tabletenv.TxLogger.Send(txConn)
	txlogzHandler(response, req)
	txConn.EndTime = txConn.StartTime.Add(time.Duration(500) * time.Millisecond)
	response = httptest.NewRecorder()
	tabletenv.TxLogger.Send(txConn)
	txlogzHandler(response, req)

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
