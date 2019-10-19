/*
Copyright 2017 Google Inc.

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
