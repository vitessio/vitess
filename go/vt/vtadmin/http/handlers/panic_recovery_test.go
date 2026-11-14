/*
Copyright 2023 The Vitess Authors.

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

package handlers

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
)

func TestPanicRecoveryHandler(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)

	m := mux.NewRouter()
	m.HandleFunc("/panic", func(w http.ResponseWriter, r *http.Request) { panic("test") }).Name("API.Panic")
	m.HandleFunc("/unnamed", func(w http.ResponseWriter, r *http.Request) { panic("unnamed") })
	m.HandleFunc("/nopanic", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})

	m.Use(PanicRecoveryHandler)
	serv := httptest.NewServer(m)
	defer serv.Close()

	cases := []struct {
		route   string
		code    int
		message string
	}{
		{
			route:   "/panic",
			code:    http.StatusInternalServerError,
			message: "test\n",
		},
		{
			route:   "/unnamed",
			code:    http.StatusInternalServerError,
			message: "unnamed\n",
		},
		{
			route:   "/nopanic",
			code:    http.StatusOK,
			message: "ok\n",
		},
	}

	for _, tcase := range cases {
		t.Run(tcase.route, func(t *testing.T) {
			rec := httptest.ResponseRecorder{
				Body: bytes.NewBuffer(nil),
			}

			m.ServeHTTP(&rec, httptest.NewRequest("GET", tcase.route, nil))
			assert.Equal(t, tcase.code, rec.Code)
			assert.Equal(t, tcase.message, rec.Body.String())
		})
	}

	var records []map[string]any
	for _, record := range logs.Records(t) {
		if record["msg"] == "uncaught panic" {
			records = append(records, record)
		}
	}
	require.Len(t, records, 2)
	assert.Equal(t, "ERROR", records[0]["level"])
	assert.Equal(t, "API.Panic", records[0]["route"])
	assert.Equal(t, "test", records[0]["panic"])
	assert.Equal(t, "ERROR", records[1]["level"])
	assert.NotContains(t, records[1], "route")
	assert.Equal(t, "unnamed", records[1]["panic"])
}
