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

package servenv

import (
	"errors"
	"net"
	"net/http"
	"net/http/pprof"

	"vitess.io/vitess/go/vt/servenv/internal/mux"
)

// HTTPHandle registers the given handler for the internal servenv mux.
func HTTPHandle(pattern string, handler http.Handler) {
	mux.Mux.Handle(pattern, handler)
}

// HTTPHandleFunc registers the given handler func for the internal servenv mux.
func HTTPHandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	mux.Mux.HandleFunc(pattern, handler)
}

// HTTPServe starts the HTTP server for the internal servenv mux on the listener.
func HTTPServe(l net.Listener) error {
	err := http.Serve(l, mux.Mux)
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

// HTTPRegisterProfile registers the default pprof HTTP endpoints with the internal servenv mux.
func HTTPRegisterProfile() {
	HTTPHandleFunc("/debug/pprof/", pprof.Index)
	HTTPHandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	HTTPHandleFunc("/debug/pprof/profile", pprof.Profile)
	HTTPHandleFunc("/debug/pprof/symbol", pprof.Symbol)
	HTTPHandleFunc("/debug/pprof/trace", pprof.Trace)
}
