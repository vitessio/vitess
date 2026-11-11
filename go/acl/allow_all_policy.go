/*
Copyright 2026 The Vitess Authors.

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

var errMonitoringOnly = errors.New("not allowed: no security-policy configured, only the monitoring role is allowed")

// allowAllPolicy allows every role for everyone.
type allowAllPolicy struct{}

// CheckAccessHTTP allows all HTTP access.
func (allowAllPolicy) CheckAccessHTTP(req *http.Request, role string) error {
	return nil
}

// monitoringOnlyPolicy is used when no security-policy is configured.
type monitoringOnlyPolicy struct{}

// CheckAccessHTTP allows only the MONITORING role.
func (monitoringOnlyPolicy) CheckAccessHTTP(req *http.Request, role string) error {
	if role == MONITORING {
		return nil
	}
	return errMonitoringOnly
}

func init() {
	RegisterPolicy("allow-all", allowAllPolicy{})
}
