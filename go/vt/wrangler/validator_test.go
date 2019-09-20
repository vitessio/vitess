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

package wrangler

import (
	"testing"
)

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
		if got := normalizeIP(input); got != want {
			t.Errorf("normalizeIP(%#v) = %#v, want %#v", input, got, want)
		}
	}
}
