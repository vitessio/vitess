package http

import (
	"net/http"

	"github.com/gorilla/mux"
)

// Request wraps an *http.Request to provide some convenience functions for
// accessing request data.
type Request struct{ *http.Request }

// Vars returns the route variables in a request, if any, as defined by
// gorilla/mux.
func (r Request) Vars() map[string]string {
	return mux.Vars(r.Request)
}
