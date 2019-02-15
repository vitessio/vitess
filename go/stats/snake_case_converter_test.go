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

func TestToSnakeCase(t *testing.T) {
	var snakeCaseTest = []struct{ input, output string }{
		{"Camel", "camel"},
		{"Camel", "camel"},
		{"CamelCase", "camel_case"},
		{"CamelCaseAgain", "camel_case_again"},
		{"CCamel", "c_camel"},
		{"CCCamel", "cc_camel"},
		{"CAMEL_CASE", "camel_case"},
		{"camel-case", "camel_case"},
		{"0", "0"},
		{"0.0", "0_0"},
		{"JSON", "json"},
	}

	for _, tt := range snakeCaseTest {
		if got, want := toSnakeCase(tt.input), tt.output; got != want {
			t.Errorf("want '%s', got '%s'", want, got)
		}
	}
}

func TestSnakeMemoize(t *testing.T) {
	key := "Test"
	if snakeMemoizer.memo[key] != "" {
		t.Errorf("want '', got '%s'", snakeMemoizer.memo[key])
	}
	toSnakeCase(key)
	if snakeMemoizer.memo[key] != "test" {
		t.Errorf("want 'test', got '%s'", snakeMemoizer.memo[key])
	}
}
