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

// Package acltest provides a security policy for tests whose behavior a test
// can switch at runtime. Importing it selects that policy for the whole test
// binary through the regular --security-policy flag, so the acl package keeps
// its "policy cannot be changed on-the-fly" contract for production binaries.
// The policy allows everything until a test calls SetMode.
//
// Only import this package from tests.
package acltest

import (
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/acl"
)

// PolicyName is the name the test policy is registered under.
const PolicyName = "acltest"

// Mode selects how the test policy answers access checks.
type Mode int32

const (
	// AllowAll allows every role, like running without a security policy.
	AllowAll Mode = iota
	// DenyAll denies every role, like the built-in deny-all policy.
	DenyAll
	// ReadOnly allows DEBUGGING and MONITORING and denies every other role,
	// like the built-in read-only policy.
	ReadOnly
)

// String returns the name of the built-in policy the mode mimics.
func (m Mode) String() string {
	switch m {
	case DenyAll:
		return "deny-all"
	case ReadOnly:
		return "read-only"
	default:
		return "allow-all"
	}
}

var (
	errDenyAll  = errors.New("not allowed: acltest deny-all mode")
	errReadOnly = errors.New("not allowed: acltest read-only mode")

	mode atomic.Int32
)

type policy struct{}

// CheckAccessHTTP answers according to the mode the test selected.
func (policy) CheckAccessHTTP(_ *http.Request, role string) error {
	switch Mode(mode.Load()) {
	case DenyAll:
		return errDenyAll
	case ReadOnly:
		switch role {
		case acl.DEBUGGING, acl.MONITORING:
			return nil
		default:
			return errReadOnly
		}
	default:
		return nil
	}
}

func init() {
	acl.RegisterPolicy(PolicyName, policy{})

	fs := pflag.NewFlagSet("acltest", pflag.PanicOnError)
	acl.RegisterFlags(fs)
	if err := fs.Set("security-policy", PolicyName); err != nil {
		panic(err)
	}
}

// SetMode switches the test policy to m and restores the previous mode when
// the test finishes.
func SetMode(t testing.TB, m Mode) {
	t.Helper()
	prev := mode.Swap(int32(m))
	t.Cleanup(func() { mode.Store(prev) })
}
