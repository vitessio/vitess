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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

func TestPanicRecoveryHandler(t *testing.T) {
	m := mux.NewRouter()
	m.HandleFunc("/panic", func(w http.ResponseWriter, r *http.Request) { panic("test") })
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
			route:   "/nopanic",
			code:    http.StatusOK,
			message: "ok\n",
		},
	}

	for _, tcase := range cases {
		tcase := tcase
		t.Run(tcase.route, func(t *testing.T) {
			rec := httptest.ResponseRecorder{
				Body: bytes.NewBuffer(nil),
			}

			m.ServeHTTP(&rec, httptest.NewRequest("GET", tcase.route, nil))
			assert.Equal(t, tcase.code, rec.Code)
			assert.Equal(t, tcase.message, rec.Body.String())
		})
	}
}
