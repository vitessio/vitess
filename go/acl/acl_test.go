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
	"testing"
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
}

func TestSimplePolicy(t *testing.T) {
	currentPolicy = policies["test"]
	err := CheckAccessActor("", ADMIN)
	want := "not allowed"
	if err == nil || err.Error() != want {
		t.Errorf("got %v, want %s", err, want)
	}
	err = CheckAccessActor("", DEBUGGING)
	if err != nil {
		t.Errorf("got %v, want no error", err)
	}

	err = CheckAccessHTTP(nil, ADMIN)
	if err == nil || err.Error() != want {
		t.Errorf("got %v, want %s", err, want)
	}
	err = CheckAccessHTTP(nil, DEBUGGING)
	if err != nil {
		t.Errorf("got %v, want no error", err)
	}
}

func TestEmptyPolicy(t *testing.T) {
	currentPolicy = nil
	err := CheckAccessActor("", ADMIN)
	if err != nil {
		t.Errorf("got %v, want no error", err)
	}
	err = CheckAccessActor("", DEBUGGING)
	if err != nil {
		t.Errorf("got %v, want no error", err)
	}

	err = CheckAccessHTTP(nil, ADMIN)
	if err != nil {
		t.Errorf("got %v, want no error", err)
	}
	err = CheckAccessHTTP(nil, DEBUGGING)
	if err != nil {
		t.Errorf("got %v, want no error", err)
	}
}
