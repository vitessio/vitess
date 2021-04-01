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

package http

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"

	"vitess.io/vitess/go/vt/vtadmin/errors"
)

// Request wraps an *http.Request to provide some convenience functions for
// accessing request data.
type Request struct{ *http.Request }

// Vars returns the route variables in a request, if any, as defined by
// gorilla/mux.
func (r Request) Vars() map[string]string {
	return mux.Vars(r.Request)
}

// ParseQueryParamAsBool attempts to parse the query parameter of the given name
// into a boolean value. If the parameter is not set, the provided default value
// is returned.
func (r Request) ParseQueryParamAsBool(name string, defaultVal bool) (bool, error) {
	if param := r.URL.Query().Get(name); param != "" {
		val, err := strconv.ParseBool(param)
		if err != nil {
			return defaultVal, &errors.BadRequest{
				Err:        err,
				ErrDetails: fmt.Sprintf("could not parse query parameter %s (= %v) into bool value", name, param),
			}
		}

		return val, nil
	}

	return defaultVal, nil
}
