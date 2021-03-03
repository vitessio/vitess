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
	"encoding/json"
	"fmt"
	"net/http"

	"vitess.io/vitess/go/vt/vtadmin/errors"
)

// JSONResponse represents a generic response object.
type JSONResponse struct {
	Result     interface{} `json:"result,omitempty"`
	Error      *errorBody  `json:"error,omitempty"`
	Ok         bool        `json:"ok"`
	httpStatus int
}

type errorBody struct {
	Message string      `json:"message"`
	Code    string      `json:"code"`
	Details interface{} `json:"details,omitempty"`
}

// NewJSONResponse returns a JSONResponse for the given result and error. If err
// is non-nil, and implements errors.TypedError, the HTTP status code and
// message are provided by the error. If not, the code and message fallback to
// 500 unknown.
func NewJSONResponse(value interface{}, err error) *JSONResponse {
	if err != nil {
		switch e := err.(type) {
		case errors.TypedError:
			return typedErrorJSONResponse(e)
		default:
			return typedErrorJSONResponse(&errors.Unknown{Err: e})
		}
	}

	return &JSONResponse{
		Result:     value,
		Error:      nil,
		Ok:         true,
		httpStatus: 200,
	}
}

// WithHTTPStatus forces a response to be used for the JSONResponse.
func (r *JSONResponse) WithHTTPStatus(code int) *JSONResponse {
	r.httpStatus = code
	return r
}

func typedErrorJSONResponse(v errors.TypedError) *JSONResponse {
	return &JSONResponse{
		Error: &errorBody{
			Message: v.Error(),
			Details: v.Details(),
			Code:    v.Code(),
		},
		Ok:         false,
		httpStatus: v.HTTPStatus(),
	}
}

// Write marshals a JSONResponse into the http response.
func (r *JSONResponse) Write(w http.ResponseWriter) {
	b, err := json.Marshal(r)
	if err != nil {
		w.WriteHeader(500)
		// A bit clunky but if we already failed to marshal JSON, let's do it by hand.
		msgFmt := `{"error": {"code": "unknown_error", "message": %q}}`
		fmt.Fprintf(w, msgFmt, err.Error())

		return
	}

	if r.httpStatus != 200 {
		w.WriteHeader(r.httpStatus)
	}

	fmt.Fprintf(w, "%s", b)
}
