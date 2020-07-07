/*
Copyright 2019 The Vitess Authors.

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
