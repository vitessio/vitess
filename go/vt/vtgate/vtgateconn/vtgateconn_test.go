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

package vtgateconn

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegisterDialer(t *testing.T) {
	dialerFunc := func(context.Context, string) (Impl, error) {
		return nil, nil
	}
	RegisterDialer("test1", dialerFunc)
	RegisterDialer("test1", dialerFunc)
}

func TestGetDialerWithProtocol(t *testing.T) {
	protocol := "test2"
	_, err := DialProtocol(context.Background(), protocol, "")
	if err == nil || err.Error() != "no dialer registered for VTGate protocol "+protocol {
		t.Fatalf("protocol: %s is not registered, should return error: %v", protocol, err)
	}
	RegisterDialer(protocol, func(context.Context, string) (Impl, error) {
		return nil, nil
	})
	c, err := DialProtocol(context.Background(), protocol, "")
	if err != nil || c == nil {
		t.Fatalf("dialerFunc has been registered, should not get nil: %v %v", err, c)
	}
}

func TestDeregisterDialer(t *testing.T) {
	const protocol = "test3"

	RegisterDialer(protocol, func(context.Context, string) (Impl, error) {
		return nil, nil
	})

	DeregisterDialer(protocol)

	_, err := DialProtocol(context.Background(), protocol, "")
	if err == nil || err.Error() != "no dialer registered for VTGate protocol "+protocol {
		t.Fatalf("protocol: %s is not registered, should return error: %v", protocol, err)
	}
}

func TestDialCustom(t *testing.T) {
	const protocol = "test4"
	var dialer string

	defaultDialerFunc := func(context.Context, string) (Impl, error) {
		dialer = "default"
		return nil, nil
	}

	customDialerFunc := func(context.Context, string) (Impl, error) {
		dialer = "custom"
		return nil, nil
	}

	customDialerFunc2 := func(context.Context, string) (Impl, error) {
		dialer = "custom2"
		return nil, nil
	}

	RegisterDialer(protocol, defaultDialerFunc)

	_, err := DialProtocol(context.Background(), protocol, "")
	require.NoError(t, err)
	require.Equal(t, "default", dialer)

	_, err = DialCustom(context.Background(), customDialerFunc, protocol)
	require.NoError(t, err)
	require.Equal(t, "custom", dialer)

	_, err = DialCustom(context.Background(), customDialerFunc2, protocol)
	require.NoError(t, err)
	require.Equal(t, "custom2", dialer)
}
