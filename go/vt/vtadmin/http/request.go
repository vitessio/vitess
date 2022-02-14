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

// ParseQueryParamAsUint32 attempts to parse the query parameter of the given
// name into a uint32 value. If the parameter is not set, the provided default
// value is returned.
func (r Request) ParseQueryParamAsUint32(name string, defaultVal uint32) (uint32, error) {
	if param := r.URL.Query().Get(name); param != "" {
		val, err := strconv.ParseUint(param, 10, 32)
		if err != nil {
			return defaultVal, &errors.BadRequest{
				Err:        err,
				ErrDetails: fmt.Sprintf("could not parse query parameter %s (= %v) into uint32 value", name, param),
			}
		}

		return uint32(val), nil
	}

	return defaultVal, nil
}
