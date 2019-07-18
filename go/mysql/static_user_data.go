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

package mysql

import (
	"net"
	"testing"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// StaticUserData holds the username and groups
type StaticUserData struct {
	Username string
	Groups   []string
}

// Get returns the wrapped username and groups
func (sud *StaticUserData) Get() *querypb.VTGateCallerID {
	return &querypb.VTGateCallerID{Username: sud.Username, Groups: sud.Groups}
}

func GetHostPort(t *testing.T, a net.Addr) (string, int) {
	// For the host name, we resolve 'localhost' into an address.
	// This works around a few travis issues where IPv6 is not 100% enabled.
	hosts, err := net.LookupHost("localhost")
	if err != nil {
		t.Fatalf("LookupHost(localhost) failed: %v", err)
	}
	host := hosts[0]
	port := a.(*net.TCPAddr).Port
	t.Logf("listening on address '%v' port %v", host, port)
	return host, port
}
