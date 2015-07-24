// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
