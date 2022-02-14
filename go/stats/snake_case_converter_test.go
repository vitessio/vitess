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
