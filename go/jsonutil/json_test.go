/*
Copyright 2020 The Vitess Authors.

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

package jsonutil

import (
	"testing"
)

func TestMarshalNoEscape(t *testing.T) {
	cases := []struct {
		name     string
		v        any
		expected string
	}{
		{
			name: "normal",
			v: struct {
				Usr string
				Pwd string
			}{
				Usr: "vitess",
				Pwd: "vitess",
			},
			expected: "{\"Usr\":\"vitess\",\"Pwd\":\"vitess\"}",
		},
		{
			name: "not exported",
			v: struct {
				usr string
				pwd string
			}{
				usr: "vitess",
				pwd: "vitess",
			},
			expected: "{}",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			json, _ := MarshalNoEscape(c.v)
			sjson := string(json[:len(json)-1])
			if sjson != c.expected {
				t.Errorf("expected: %v, got: %v", c.expected, sjson)
			}
		})
	}
}

func TestMarshalIndentNoEscape(t *testing.T) {
	cases := []struct {
		name     string
		v        any
		prefix   string
		ident    string
		expected string
	}{
		{
			name: "normal",
			v: struct {
				Usr string
				Pwd string
			}{
				Usr: "vitess",
				Pwd: "vitess",
			},
			prefix:   "test",
			ident:    "\t",
			expected: "{\ntest\t\"Usr\": \"vitess\",\ntest\t\"Pwd\": \"vitess\"\ntest}",
		},
		{
			name: "not exported",
			v: struct {
				usr string
				pwd string
			}{
				usr: "vitess",
				pwd: "vitess",
			},
			expected: "{}",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			json, _ := MarshalIndentNoEscape(c.v, c.prefix, c.ident)
			sjson := string(json[:len(json)-1])
			if sjson != c.expected {
				t.Errorf("expected: %v, got: %v", c.expected, sjson)
			}
		})
	}
}
