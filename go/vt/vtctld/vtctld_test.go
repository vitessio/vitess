/*
Copyright 2022 The Vitess Authors.

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

package vtctld

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWebApp(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, appPrefix, nil)
	w := httptest.NewRecorder()

	handler := staticContentHandler(true)
	handler.ServeHTTP(w, req)
	res := w.Result()

	assert.Equal(t, http.StatusOK, res.StatusCode)

	defer res.Body.Close()

	data, err := io.ReadAll(res.Body)
	fmt.Printf("body: %s\n", string(data))

	assert.NoError(t, err)
	assert.Contains(t, string(data), "<!doctype html>")
}

func TestWebAppDisabled(t *testing.T) {
	flag.Set("enable_vtctld_ui", "false")
	defer flag.Set("enable_vtctld_ui", "true")

	req := httptest.NewRequest(http.MethodGet, appPrefix, nil)
	w := httptest.NewRecorder()

	handler := staticContentHandler(false)
	handler.ServeHTTP(w, req)
	res := w.Result()

	assert.Equal(t, http.StatusNotFound, res.StatusCode)

	defer res.Body.Close()

	data, err := io.ReadAll(res.Body)
	assert.NoError(t, err)
	assert.Equal(t, "404 page not found\n", string(data))
}
