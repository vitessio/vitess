package acl

import (
	"errors"
	"net/http"
)

var errDenyAll = errors.New("not allowed: deny-all security_policy enforced")

// denyAllPolicy rejects all access.
type denyAllPolicy struct{}

// CheckAccessActor disallows all actor access.
func (denyAllPolicy) CheckAccessActor(actor, role string) error {
	return errDenyAll
}

// CheckAccessHTTP disallows all HTTP access.
func (denyAllPolicy) CheckAccessHTTP(req *http.Request, role string) error {
	return errDenyAll
}

func init() {
	RegisterPolicy("deny-all", denyAllPolicy{})
}
