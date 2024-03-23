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
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
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
		assert.NoError(t, err)
		assert.Equal(t, want.host, gotHost)
		assert.Equal(t, want.port, gotPort)
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
		assert.Error(t, err)
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
		assert.Equal(t, want, JoinHostPort(input.host, input.port))
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
		assert.Equal(t, want, NormalizeIP(input))
	}
}

func TestDNSTracker(t *testing.T) {
	refresh := DNSTracker("localhost")
	_, err := refresh()
	assert.NoError(t, err)

	refresh = DNSTracker("")
	val, err := refresh()
	assert.NoError(t, err)
	assert.False(t, val, "DNS name resolution should not have changed")
}

func TestAddrEqual(t *testing.T) {
	addr1 := net.ParseIP("1.2.3.4")
	addr2 := net.ParseIP("127.0.0.1")

	addrSet1 := []net.IP{addr1, addr2}
	addrSet2 := []net.IP{addr1}
	addrSet3 := []net.IP{addr2}
	ok := addrEqual(addrSet1, addrSet2)
	assert.False(t, ok, "addresses %q and %q should not be equal", addrSet1, addrSet2)

	ok = addrEqual(addrSet3, addrSet2)
	assert.False(t, ok, "addresses %q and %q should not be equal", addrSet3, addrSet2)

	ok = addrEqual(addrSet1, addrSet1)
	assert.True(t, ok, "addresses %q and %q should be equal", addrSet1, addrSet1)
}
