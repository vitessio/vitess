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
	"testing"

	"golang.org/x/net/context"
)

func TestLiveQueryzHandlerJSON(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/oltpqueryz/?format=json", nil)

	queryList := NewQueryList()
	queryList.Add(NewQueryDetail(context.Background(), &testConn{id: 1}))
	queryList.Add(NewQueryDetail(context.Background(), &testConn{id: 2}))

	livequeryzHandler(queryList, resp, req)
}

func TestLiveQueryzHandlerHTTP(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/oltpqueryz/", nil)

	queryList := NewQueryList()
	queryList.Add(NewQueryDetail(context.Background(), &testConn{id: 1}))
	queryList.Add(NewQueryDetail(context.Background(), &testConn{id: 2}))

	livequeryzHandler(queryList, resp, req)
}

func TestLiveQueryzHandlerHTTPFailedInvalidForm(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/oltpqueryz/", nil)

	livequeryzHandler(NewQueryList(), resp, req)
	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("http call should fail and return code: %d, but got: %d",
			http.StatusInternalServerError, resp.Code)
	}
}

func TestLiveQueryzHandlerTerminateConn(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/oltpqueryz//terminate?connID=1", nil)

	queryList := NewQueryList()
	testConn := &testConn{id: 1}
	queryList.Add(NewQueryDetail(context.Background(), testConn))
	if testConn.IsKilled() {
		t.Fatalf("conn should still be alive")
	}
	livequeryzTerminateHandler(queryList, resp, req)
	if !testConn.IsKilled() {
		t.Fatalf("conn should be killed")
	}
}

func TestLiveQueryzHandlerTerminateFailedInvalidConnID(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/oltpqueryz//terminate?connID=invalid", nil)

	livequeryzTerminateHandler(NewQueryList(), resp, req)
	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("http call should fail and return code: %d, but got: %d",
			http.StatusInternalServerError, resp.Code)
	}
}

func TestLiveQueryzHandlerTerminateFailedKnownConnID(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/oltpqueryz//terminate?connID=10", nil)

	livequeryzTerminateHandler(NewQueryList(), resp, req)
	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("http call should fail and return code: %d, but got: %d",
			http.StatusInternalServerError, resp.Code)
	}
}

func TestLiveQueryzHandlerTerminateFailedInvalidForm(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/oltpqueryz//terminate?inva+lid=2", nil)

	livequeryzTerminateHandler(NewQueryList(), resp, req)
	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("http call should fail and return code: %d, but got: %d",
			http.StatusInternalServerError, resp.Code)
	}
}
