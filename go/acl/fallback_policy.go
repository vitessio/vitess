// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
