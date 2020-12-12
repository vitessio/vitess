package errors

import (
	"fmt"
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

// Unknown is the generic error, used when a more specific error is either
// unspecified or inappropriate.
type Unknown struct {
	Err        error
	ErrDetails interface{}
}

func (e *Unknown) Error() string        { return e.Err.Error() }
func (e *Unknown) Code() string         { return "unknown" }
func (e *Unknown) Details() interface{} { return e.ErrDetails }
func (e *Unknown) HTTPStatus() int      { return 500 }

// ErrInvalidCluster is returned when a cluster parameter, either in a route or
// as a query param, is invalid.
type ErrInvalidCluster struct {
	Err error
}

func (e *ErrInvalidCluster) Error() string        { return e.Err.Error() }
func (e *ErrInvalidCluster) Code() string         { return "invalid cluster" }
func (e *ErrInvalidCluster) Details() interface{} { return nil }
func (e *ErrInvalidCluster) HTTPStatus() int      { return 400 }

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
func (e *MissingParams) HTTPStatus() int      { return 400 }
