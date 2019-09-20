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
package main

import (
	"fmt"
	"net/http"

	"vitess.io/vitess/go/vt/servenv"
)

// This file registers a /healthz URL that reports the health of the agent.

var okMessage = []byte("ok\n")

func init() {
	servenv.OnRun(func() {
		http.Handle("/healthz", http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			if _, err := agent.Healthy(); err != nil {
				http.Error(rw, fmt.Sprintf("500 internal server error: agent not healthy: %v", err), http.StatusInternalServerError)
				return
			}

			rw.Header().Set("Content-Length", fmt.Sprintf("%v", len(okMessage)))
			rw.WriteHeader(http.StatusOK)
			rw.Write(okMessage)
		}))
	})
}
