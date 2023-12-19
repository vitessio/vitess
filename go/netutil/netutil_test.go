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

package netutil

import (
	"testing"
)

func TestSplitHostPort(t *testing.T) {
	type addr struct {
		host string
		port int
	}
	table := map[string]addr{
		"host-name:132":  {host: "host-name", port: 132},
		"hostname:65535": {host: "hostname", port: 65535},
		"[::1]:321":      {host: "::1", port: 321},
		"::1:432":        {host: "::1", port: 432},
	}
	for input, want := range table {
		gotHost, gotPort, err := SplitHostPort(input)
		if err != nil {
			t.Errorf("SplitHostPort error: %v", err)
		}
		if gotHost != want.host || gotPort != want.port {
			t.Errorf("SplitHostPort(%#v) = (%v, %v), want (%v, %v)", input, gotHost, gotPort, want.host, want.port)
		}
	}
}

func TestSplitHostPortFail(t *testing.T) {
	// These cases should all fail to parse.
	inputs := []string{
		"host-name",
		"host-name:123abc",
	}
	for _, input := range inputs {
		_, _, err := SplitHostPort(input)
		if err == nil {
			t.Errorf("expected error from SplitHostPort(%q), but got none", input)
		}
	}
}

func TestJoinHostPort(t *testing.T) {
	type addr struct {
		host string
		port int32
	}
	table := map[string]addr{
		"host-name:132": {host: "host-name", port: 132},
		"[::1]:321":     {host: "::1", port: 321},
	}
	for want, input := range table {
		if got := JoinHostPort(input.host, input.port); got != want {
			t.Errorf("SplitHostPort(%v, %v) = %#v, want %#v", input.host, input.port, got, want)
		}
	}
}

func TestNormalizeIP(t *testing.T) {
	table := map[string]string{
		"1.2.3.4":   "1.2.3.4",
		"127.0.0.1": "127.0.0.1",
		"127.0.1.1": "127.0.0.1",
		// IPv6 must be mapped to IPv4.
		"::1": "127.0.0.1",
		// An unparseable IP should be returned as is.
		"127.": "127.",
	}
	for input, want := range table {
		if got := NormalizeIP(input); got != want {
			t.Errorf("NormalizeIP(%#v) = %#v, want %#v", input, got, want)
		}
	}
}
