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

package throttler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestThrottlerzHandler_MissingSlash(t *testing.T) {
	request, _ := http.NewRequest("GET", "/throttlerz", nil)
	response := httptest.NewRecorder()
	m := newManager()

	throttlerzHandler(response, request, m)

	got := response.Body.String()
	require.Contains(t, got, "invalid /throttlerz path", "/throttlerz without the slash does not work (the Go HTTP server does automatically redirect in practice though)")
}

func TestThrottlerzHandler_List(t *testing.T) {
	f := &managerTestFixture{}
	if err := f.setUp(); err != nil {
		t.Fatal(err)
	}
	defer f.tearDown()

	request, _ := http.NewRequest("GET", "/throttlerz/", nil)
	response := httptest.NewRecorder()

	throttlerzHandler(response, request, f.m)

	got := response.Body.String()
	require.Contains(t, got, `<a href="/throttlerz/t1">t1</a>`, "list does not include 't1'")
	require.Contains(t, got, `<a href="/throttlerz/t2">t2</a>`, "list does not include 't2'")
}

func TestThrottlerzHandler_Details(t *testing.T) {
	f := &managerTestFixture{}
	if err := f.setUp(); err != nil {
		t.Fatal(err)
	}
	defer f.tearDown()

	request, _ := http.NewRequest("GET", "/throttlerz/t1", nil)
	response := httptest.NewRecorder()

	throttlerzHandler(response, request, f.m)

	got := response.Body.String()
	require.Contains(t, got, `<title>Details for Throttler 't1'</title>`, "details for 't1' not shown")
}
