/*
Copyright 2018 The Vitess Authors.

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

package stats

import (
	"testing"
)

func TestToKebabCase(t *testing.T) {
	var kebabCaseTest = []struct{ input, output string }{
		{"Camel", "camel"},
		{"Camel", "camel"},
		{"CamelCase", "camel-case"},
		{"CamelCaseAgain", "camel-case-again"},
		{"CCamel", "c-camel"},
		{"CCCamel", "cc-camel"},
		{"CAMEL_CASE", "camel-case"},
		{"camel-case", "camel-case"},
		{"0", "0"},
		{"0.0", "0_0"},
		{"JSON", "json"},
	}

	for _, tt := range kebabCaseTest {
		if got, want := toKebabCase(tt.input), tt.output; got != want {
			t.Errorf("want '%s', got '%s'", want, got)
		}
	}
}

func TestMemoize(t *testing.T) {
	key := "Test"
	if memoizer.memo[key] != "" {
		t.Errorf("want '', got '%s'", memoizer.memo[key])
	}
	toKebabCase(key)
	if memoizer.memo[key] != "test" {
		t.Errorf("want 'test', got '%s'", memoizer.memo[key])
	}
}
