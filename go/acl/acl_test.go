// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
