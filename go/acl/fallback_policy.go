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

var fallbackError = errors.New("not allowed: fallback policy")

// FallbackPolicy is the policy that's used if the
// requested policy cannot be found. It rejects all
// access.
type FallbackPolicy struct{}

// CheckAccessActor disallows all actor access.
func (fp FallbackPolicy) CheckAccessActor(actor, role string) error {
	return fallbackError
}

// CheckAccessHTTP disallows all HTTP access.
func (fp FallbackPolicy) CheckAccessHTTP(req *http.Request, role string) error {
	return fallbackError
}
