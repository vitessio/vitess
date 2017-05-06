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
