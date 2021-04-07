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
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"testing"
)

func checkDistribution(t *testing.T, data []*net.SRV, margin float64) {
	sum := 0
	for _, srv := range data {
		sum += int(srv.Weight)
	}

	results := make(map[string]int)

	count := 1000
	for j := 0; j < count; j++ {
		d := make([]*net.SRV, len(data))
		copy(d, data)
		byPriorityWeight(d).shuffleByWeight()
		key := d[0].Target
		results[key] = results[key] + 1
	}

	actual := results[data[0].Target]
	expected := float64(count) * float64(data[0].Weight) / float64(sum)
	diff := float64(actual) - expected
	t.Logf("actual: %v diff: %v e: %v m: %v", actual, diff, expected, margin)
	if diff < 0 {
		diff = -diff
	}
	if diff > (expected * margin) {
		t.Errorf("missed target weight: expected %v, %v", expected, actual)
	}
}

func testUniformity(t *testing.T, size int, margin float64) {
	rand.Seed(1)
	data := make([]*net.SRV, size)
	for i := 0; i < size; i++ {
		data[i] = &net.SRV{Target: fmt.Sprintf("%c", 'a'+i), Weight: 1}
	}
	checkDistribution(t, data, margin)
}

func TestUniformity(t *testing.T) {
	testUniformity(t, 2, 0.05)
	testUniformity(t, 3, 0.10)
	testUniformity(t, 10, 0.20)
	testWeighting(t, 0.05)
}

func testWeighting(t *testing.T, margin float64) {
	rand.Seed(1)
	data := []*net.SRV{
		{Target: "a", Weight: 60},
		{Target: "b", Weight: 30},
		{Target: "c", Weight: 10},
	}
	checkDistribution(t, data, margin)
}

func TestWeighting(t *testing.T) {
	testWeighting(t, 0.05)
}

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

func TestResolveIPv4Addrs(t *testing.T) {
	cases := []struct {
		address       string
		expected      []string
		expectedError bool
	}{
		{
			address:  "localhost:3306",
			expected: []string{"127.0.0.1:3306"},
		},
		{
			address:       "127.0.0.256:3306",
			expectedError: true,
		},
		{
			address:       "localhost",
			expectedError: true,
		},
		{
			address:       "InvalidHost:3306",
			expectedError: true,
		},
	}

	for _, c := range cases {
		t.Run(c.address, func(t *testing.T) {
			got, err := ResolveIPv4Addrs(c.address)
			if (err != nil) != c.expectedError {
				t.Errorf("expected error but got: %v", err)
			}
			if !reflect.DeepEqual(got, c.expected) {
				t.Errorf("expected: %v, got: %v", c.expected, got)
			}
		})
	}
}
