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

package sqlescape

import (
	"testing"
)

func TestEscapeID(t *testing.T) {
	testcases := []struct {
		in, out string
	}{{
		in:  "aa",
		out: "`aa`",
	}, {
		in:  "a`a",
		out: "`a``a`",
	}, {
		in:  "a``a",
		out: "`a````a`",
	}}
	for _, tc := range testcases {
		t.Run(tc.in, func(t *testing.T) {
			out := EscapeID(tc.in)
			if out != tc.out {
				t.Errorf("EscapeID(%s): %s, want %s", tc.in, out, tc.out)
			}
		})
	}
}

func TestUnescapeID(t *testing.T) {
	testcases := []struct {
		in, out string
	}{{
		in:  "`aa`",
		out: "aa",
	}, {
		in:  "`a``a`",
		out: "a`a",
	}, {
		in:  "`a````a`",
		out: "a``a",
	}, {
		in:  "aa",
		out: "aa",
	}, {
		in:  "",
		out: "",
	}, {
		in:  "`aa",
		out: "`aa",
	}, {
		in:  "`a```a`",
		out: "a``a",
	}, {
		in:  "`a",
		out: "`a",
	}}
	for _, tc := range testcases {
		t.Run(tc.in, func(t *testing.T) {
			out := UnescapeID(tc.in)
			if out != tc.out {
				t.Errorf("UnescapeID(%s): %s, want %s", tc.in, out, tc.out)
			}
		})
	}
}
