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
	"fmt"
	"net/http"

	"github.com/gorilla/mux"

	"vitess.io/vitess/go/vt/log"
)

// PanicRecoveryHandler is a mux.MiddlewareFunc which recovers from any uncaught
// `panic`s further down the middleware chain.
//
// If it recovers from a panic, it returns a 500 to the caller, and logs the
// route name along with the panicking error.
// name, as set by (mux.*Route).Name(), embeds it in the request context, and invokes
// the next middleware in the chain.
func PanicRecoveryHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := mux.CurrentRoute(r).GetName()
		defer func() {
			if err := recover(); err != nil {
				log.Errorf("uncaught panic in %s: %s", name, err)
				http.Error(w, fmt.Sprintf("%v", err), http.StatusInternalServerError)
			}
		}()

		next.ServeHTTP(w, r)
	})
}
