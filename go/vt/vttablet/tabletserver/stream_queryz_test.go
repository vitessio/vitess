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

package tabletserver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/net/context"
)

func TestStreamQueryzHandlerJSON(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/streamqueryz?format=json", nil)

	queryList := NewQueryList()
	queryList.Add(NewQueryDetail(context.Background(), &testConn{id: 1}))
	queryList.Add(NewQueryDetail(context.Background(), &testConn{id: 2}))

	streamQueryzHandler(queryList, resp, req)
}

func TestStreamQueryzHandlerHTTP(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/streamqueryz", nil)

	queryList := NewQueryList()
	queryList.Add(NewQueryDetail(context.Background(), &testConn{id: 1}))
	queryList.Add(NewQueryDetail(context.Background(), &testConn{id: 2}))

	streamQueryzHandler(queryList, resp, req)
}

func TestStreamQueryzHandlerHTTPFailedInvalidForm(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/streamqueryz", nil)

	streamQueryzHandler(NewQueryList(), resp, req)
	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("http call should fail and return code: %d, but got: %d",
			http.StatusInternalServerError, resp.Code)
	}
}

func TestStreamQueryzHandlerTerminateConn(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/streamqueryz/terminate?connID=1", nil)

	queryList := NewQueryList()
	testConn := &testConn{id: 1}
	queryList.Add(NewQueryDetail(context.Background(), testConn))
	if testConn.IsKilled() {
		t.Fatalf("conn should still be alive")
	}
	streamQueryzTerminateHandler(queryList, resp, req)
	if !testConn.IsKilled() {
		t.Fatalf("conn should be killed")
	}
}

func TestStreamQueryzHandlerTerminateFailedInvalidConnID(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/streamqueryz/terminate?connID=invalid", nil)

	streamQueryzTerminateHandler(NewQueryList(), resp, req)
	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("http call should fail and return code: %d, but got: %d",
			http.StatusInternalServerError, resp.Code)
	}
}

func TestStreamQueryzHandlerTerminateFailedKnownConnID(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/streamqueryz/terminate?connID=10", nil)

	streamQueryzTerminateHandler(NewQueryList(), resp, req)
	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("http call should fail and return code: %d, but got: %d",
			http.StatusInternalServerError, resp.Code)
	}
}

func TestStreamQueryzHandlerTerminateFailedInvalidForm(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/streamqueryz/terminate?inva+lid=2", nil)

	streamQueryzTerminateHandler(NewQueryList(), resp, req)
	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("http call should fail and return code: %d, but got: %d",
			http.StatusInternalServerError, resp.Code)
	}
}
