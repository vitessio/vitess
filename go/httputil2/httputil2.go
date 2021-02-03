/*
Copyright 2020 The Vitess Authors.

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
package httputil2

import (
	"net/http"
	"net/http/pprof"
)

func RegisterPprof(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof", pprof.Index)
	mux.Handle("/debug/allocs", pprof.Handler("allocs"))
	mux.Handle("/debug/block", pprof.Handler("block"))
	mux.Handle("/debug/cmdline", pprof.Handler("cmdline"))
	mux.Handle("/debug/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/heap", pprof.Handler("heap"))
	mux.Handle("/debug/mutex", pprof.Handler("mutex"))
	mux.HandleFunc("/debug/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.Handle("/debug/threadcreate", pprof.Handler("threadcreate"))
	mux.Handle("/debug/trace", pprof.Handler("trace"))
}
