package handlers

import (
	"net/http"

	"vitess.io/vitess/go/vt/vtadmin/rbac"
)

// NewAuthenticationHandler returns an http middleware that invokes the given
// authenticator and calls the next handler in the middleware chain, with the
// authenticated actor added to the request context.
//
// If the authenticator returns an error, then the overall http request is
// failed with an Unauthorized code.
func NewAuthenticationHandler(authenticator rbac.Authenticator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			actor, err := authenticator.AuthenticateHTTP(r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}

			next.ServeHTTP(w, r.WithContext(rbac.NewContext(r.Context(), actor)))
		})
	}
}
