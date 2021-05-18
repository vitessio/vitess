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

package errors

import (
	"fmt"
	"net/http"
	"strings"
)

// TypedError defines the behavior needed to strongly-type an error into an
// http response.
type TypedError interface {
	Error() string
	Code() string
	Details() interface{}
	HTTPStatus() int
}

// BadRequest is returned when some request parameter is invalid.
type BadRequest struct {
	Err        error
	ErrDetails interface{}
}

func (e *BadRequest) Error() string        { return e.Err.Error() }
func (e *BadRequest) Code() string         { return "bad request" }
func (e *BadRequest) Details() interface{} { return e.ErrDetails }
func (e *BadRequest) HTTPStatus() int      { return http.StatusBadRequest }

// Unknown is the generic error, used when a more specific error is either
// unspecified or inappropriate.
type Unknown struct {
	Err        error
	ErrDetails interface{}
}

func (e *Unknown) Error() string        { return e.Err.Error() }
func (e *Unknown) Code() string         { return "unknown" }
func (e *Unknown) Details() interface{} { return e.ErrDetails }
func (e *Unknown) HTTPStatus() int      { return http.StatusInternalServerError }

// ErrInvalidCluster is returned when a cluster parameter, either in a route or
// as a query param, is invalid.
type ErrInvalidCluster struct {
	Err error
}

func (e *ErrInvalidCluster) Error() string        { return e.Err.Error() }
func (e *ErrInvalidCluster) Code() string         { return "invalid cluster" }
func (e *ErrInvalidCluster) Details() interface{} { return nil }
func (e *ErrInvalidCluster) HTTPStatus() int      { return http.StatusBadRequest }

// MissingParams is returned when an HTTP handler requires parameters that were
// not provided.
type MissingParams struct {
	Params []string
}

func (e *MissingParams) Error() string {
	return fmt.Sprintf("missing required params: %s", strings.Join(e.Params, ", "))
}

func (e *MissingParams) Code() string         { return "missing params" }
func (e *MissingParams) Details() interface{} { return nil }
func (e *MissingParams) HTTPStatus() int      { return http.StatusBadRequest }

// NoSuchSchema is returned when a schema definition cannot be found for a given
// set of filter criteria. Both GetSchema and FindSchema can return this error.
type NoSuchSchema struct {
	Clusters []string
	Table    string
}

func (e *NoSuchSchema) Error() string {
	return fmt.Sprintf("%s: no schemas found with table named %s", e.Code(), e.Table)
}

func (e *NoSuchSchema) Details() interface{} {
	details := map[string]interface{}{
		"table": e.Table,
	}

	if e.Clusters != nil {
		details["clusters"] = e.Clusters
	}

	return details
}

func (e *NoSuchSchema) Code() string    { return "no such schema" }
func (e *NoSuchSchema) HTTPStatus() int { return http.StatusNotFound }
