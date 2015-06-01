// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgateconn

import (
	"testing"
	"time"

	"golang.org/x/net/context"
)

func TestRegisterDialer(t *testing.T) {
	dialerFunc := func(context.Context, string, time.Duration) (Impl, error) {
		return nil, nil
	}
	RegisterDialer("test1", dialerFunc)
	RegisterDialer("test1", dialerFunc)
}

func TestGetDialerWithProtocol(t *testing.T) {
	protocol := "test2"
	c, err := DialProtocol(context.Background(), protocol, "", 0)
	if err == nil || err.Error() != "no dialer registered for VTGate protocol "+protocol {
		t.Fatalf("protocol: %s is not registered, should return error: %v", protocol, err)
	}
	RegisterDialer(protocol, func(context.Context, string, time.Duration) (Impl, error) {
		return nil, nil
	})
	c, err = DialProtocol(context.Background(), protocol, "", 0)
	if err != nil || c == nil {
		t.Fatalf("dialerFunc has been registered, should not get nil: %v %v", err, c)
	}
}

func TestServerError(t *testing.T) {
	serverError := &ServerError{Code: 12, Err: "error"}
	if serverError.Error() == "" {
		t.Fatalf("server error is not empty, should not return empty error")
	}
}

func TestOperationalError(t *testing.T) {
	if OperationalError("error").Error() == "" {
		t.Fatal("operational error is not mepty, should not return empty error")
	}
}
