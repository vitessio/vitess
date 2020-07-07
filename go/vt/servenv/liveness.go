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

package servenv

import (
	"net/http"
)

// This file registers a handler that immediately responds with HTTP 200 OK.
// This can be used as a liveness check to make sure the process can pass a
// minimum bar of functionality: accept a connection and reply unconditionally.
// The handler is as simple as possible to avoid false negatives for liveness.
// If the process is actually making progress, albeit slowly, failing the
// liveness check and restarting the process will likely only make it fall
// further behind on its backlog.

func init() {
	http.HandleFunc("/debug/liveness", func(rw http.ResponseWriter, r *http.Request) {
		// Do nothing. Return success immediately.
	})
}
