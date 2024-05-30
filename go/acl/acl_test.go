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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

type TestPolicy struct{}

func (tp TestPolicy) CheckAccessActor(actor, role string) error {
	if role == ADMIN {
		return errors.New("not allowed")
	}
	return nil
}

func (tp TestPolicy) CheckAccessHTTP(req *http.Request, role string) error {
	if role == ADMIN {
		return errors.New("not allowed")
	}
	return nil
}

func init() {
	RegisterPolicy("test", TestPolicy{})

	// Run the `once` so it doesn't run during testing,
	// since we need to override the currentPolicy.
	once.Do(savePolicy)
}

func TestSimplePolicy(t *testing.T) {
	currentPolicy = policies["test"]
	err := CheckAccessActor("", ADMIN)
	want := "not allowed"
	assert.Equalf(t, err.Error(), want, "got %v, want %s", err, want)

	err = CheckAccessActor("", DEBUGGING)
	assert.Equalf(t, err, nil, "got %v, want no error", err)

	err = CheckAccessActor("", MONITORING)
	assert.Equalf(t, err, nil, "got %v, want no error", err)

	err = CheckAccessHTTP(nil, ADMIN)
	assert.Equalf(t, err.Error(), want, "got %v, want %s", err, want)

	err = CheckAccessHTTP(nil, DEBUGGING)
	assert.Equalf(t, err, nil, "got %v, want no error", err)

	err = CheckAccessHTTP(nil, MONITORING)
	assert.Equalf(t, err, nil, "got %v, want no error", err)
}

func TestEmptyPolicy(t *testing.T) {
	currentPolicy = nil
	err := CheckAccessActor("", ADMIN)
	assert.Equalf(t, err, nil, "got %v, want no error", err)

	err = CheckAccessActor("", DEBUGGING)
	assert.Equalf(t, err, nil, "got %v, want no error", err)

	err = CheckAccessActor("", MONITORING)
	assert.Equalf(t, err, nil, "got %v, want no error", err)

	err = CheckAccessHTTP(nil, ADMIN)
	assert.Equalf(t, err, nil, "got %v, want no error", err)

	err = CheckAccessHTTP(nil, DEBUGGING)
	assert.Equalf(t, err, nil, "got %v, want no error", err)

	err = CheckAccessHTTP(nil, MONITORING)
	assert.Equalf(t, err, nil, "got %v, want no error", err)
}

func TestValidSecurityPolicy(t *testing.T) {
	securityPolicy = "test"
	savePolicy()

	assert.Equalf(t, TestPolicy{}, currentPolicy, "got %v, expected %v", currentPolicy, TestPolicy{})
}

func TestInvalidSecurityPolicy(t *testing.T) {
	securityPolicy = "invalidSecurityPolicy"
	savePolicy()

	assert.Equalf(t, denyAllPolicy{}, currentPolicy, "got %v, expected %v", currentPolicy, denyAllPolicy{})
}

func TestSendError(t *testing.T) {
	testW := httptest.NewRecorder()

	testErr := errors.New("Testing error message")
	SendError(testW, testErr)

	// Check the status code
	assert.Equalf(t, testW.Code, http.StatusForbidden, "got %v; want %v", testW.Code, http.StatusForbidden)

	// Check the writer body
	want := fmt.Sprintf("Access denied: %v\n", testErr)
	got := testW.Body.String()
	assert.Equalf(t, got, want, "got %v; want %v", got, want)
}

func TestRegisterFlags(t *testing.T) {
	testFs := pflag.NewFlagSet("test", pflag.ExitOnError)
	securityPolicy = "test"

	RegisterFlags(testFs)

	securityPolicyFlag := testFs.Lookup("security_policy")
	assert.NotNil(t, securityPolicyFlag, "no security_policy flag is registered")

	// Check the default value of the flag
	want := "test"
	got := securityPolicyFlag.DefValue
	assert.Equalf(t, got, want, "got %v; want %v", got, want)
}

func TestAlreadyRegisteredPolicy(t *testing.T) {
	if os.Getenv("TEST_ACL") == "1" {
		RegisterPolicy("test", nil)
		return
	}

	// Run subprocess to test os.Exit which is called by log.fatalf
	// os.Exit should be called if we try to re-register a policy
	cmd := exec.Command(os.Args[0], "-test.run=TestAlreadyRegisteredPolicy")
	cmd.Env = append(os.Environ(), "TEST_ACL=1")
	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}

	t.Errorf("process ran with err %v, want exit status 1", err)
}
