package acl

import (
	"errors"
	"net/http"
)

var errReadOnly = errors.New("not allowed: read-only security_policy enforced")

// readOnlyPolicy allows DEBUGGING and MONITORING roles for everyone,
// while denying any other roles (e.g. ADMIN) for everyone.
type readOnlyPolicy struct{}

// CheckAccessActor disallows all actor access.
func (readOnlyPolicy) CheckAccessActor(actor, role string) error {
	switch role {
	case DEBUGGING, MONITORING:
		return nil
	default:
		return errReadOnly
	}
}

// CheckAccessHTTP disallows all HTTP access.
func (readOnlyPolicy) CheckAccessHTTP(req *http.Request, role string) error {
	switch role {
	case DEBUGGING, MONITORING:
		return nil
	default:
		return errReadOnly
	}
}

func init() {
	RegisterPolicy("read-only", readOnlyPolicy{})
}
