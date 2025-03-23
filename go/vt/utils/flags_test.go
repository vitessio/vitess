/*
Copyright 2025 The Vitess Authors.

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

package utils

import (
	"fmt"
	"testing"

	"github.com/spf13/pflag"
)

func TestFlagVariants(t *testing.T) {
	tests := []struct {
		input               string
		expectedUnderscored string
		expectedDashed      string
	}{
		{"a-b", "a_b", "a-b"},
		{"a_b", "a_b", "a-b"},
		{"a-b_c", "a_b_c", "a-b-c"},
		{"example", "example", "example"},
	}

	for _, tc := range tests {
		underscored, dashed := flagVariants(tc.input)
		if underscored != tc.expectedUnderscored {
			t.Errorf("For input %q, expected underscored %q, got %q", tc.input, tc.expectedUnderscored, underscored)
		}
		if dashed != tc.expectedDashed {
			t.Errorf("For input %q, expected dashed %q, got %q", tc.input, tc.expectedDashed, dashed)
		}
	}
}

// testFlagVar is a generic helper to test both flag setters for various data types.
func testFlagVar[T any](t *testing.T, name string, def T, usage string, setter func(fs *pflag.FlagSet, p *T, name string, def T, usage string)) {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	var value T
	setter(fs, &value, name, def, usage)
	underscored, dashed := flagVariants(name)

	// Verify the primary (dashed) flag.
	fDashed := fs.Lookup(dashed)
	if fDashed == nil {
		t.Fatalf("Expected flag %q to be registered", dashed)
	} else {
		if fDashed.Usage != usage {
			t.Errorf("Expected usage %q for flag %q, got %q", usage, dashed, fDashed.Usage)
		}
		if fDashed.Hidden {
			t.Errorf("Flag %q should not be hidden", dashed)
		}
	}

	// Verify the (deprecated )alias (underscored) flag.
	fUnderscored := fs.Lookup(underscored)
	if fUnderscored == nil {
		t.Fatalf("Expected flag %q to be registered", underscored)
	} else {
		if fUnderscored.Usage != "" {
			t.Errorf("Expected empty usage for flag %q, got %q", underscored, fUnderscored.Usage)
		}
		if !fUnderscored.Hidden {
			t.Errorf("Flag %q should be hidden", underscored)
		}
		expectedDep := fmt.Sprintf("use %s instead", dashed)
		if fUnderscored.Deprecated != expectedDep {
			t.Errorf("Expected deprecated message %q for flag %q, got %q", expectedDep, underscored, fUnderscored.Deprecated)
		}
	}
}

func testFlagVars[T any](t *testing.T, name string, def T, usage string, setter func(fs *pflag.FlagSet, p *T, name string, def T, usage string)) {
	underscored, dashed := flagVariants(name)
	testFlagVar(t, dashed, def, usage, setter)
	testFlagVar(t, underscored, def, "", setter)
}

func TestSetFlagIntVar(t *testing.T) {
	testFlagVars(t, "int-flag", 42, "an integer flag", SetFlagIntVar)
}

func TestSetFlagBoolVar(t *testing.T) {
	testFlagVars(t, "bool-flag", true, "a boolean flag", SetFlagBoolVar)
}

func TestSetFlagVariantsForTests(t *testing.T) {
	m := make(map[string]string)
	key := "test-flag"
	value := "some-value"

	SetFlagVariantsForTests(m, key, value)

	underscored, dashed := flagVariants(key)
	if m[underscored] != value && m[dashed] != value {
		t.Errorf("Expected either m[%q] or m[%q] to be %q, but got neither", underscored, dashed, value)
	}

	if m[underscored] == value && m[dashed] == value {
		t.Errorf("Expected only one variant to be set, but both were set")
	}
}

// TestGetFlagVariantForTests checks that GetFlagVariantForTests returns either the underscored or dashed variant.
func TestGetFlagVariantForTests(t *testing.T) {
	tests := []struct {
		input string
	}{
		{"a-b"},     // expects either "a_b" or "a-b"
		{"--a_b"},   // expects either "--a_b" or "--a-b"
		{"example"}, // expects "example"
	}

	for _, tc := range tests {
		underscored, dashed := flagVariants(tc.input)
		result := GetFlagVariantForTests(tc.input)
		if result != underscored && result != dashed {
			t.Errorf(
				"Expected either %q or %q for input %q, got %q",
				underscored, dashed, tc.input, result,
			)
		}
	}
}
