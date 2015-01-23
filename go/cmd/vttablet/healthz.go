// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

import (
	"fmt"
	"net/http"

	"github.com/youtube/vitess/go/vt/servenv"
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
