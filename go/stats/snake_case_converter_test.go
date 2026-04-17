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

package stats

import (
	"fmt"
	"sync"
	"testing"
)

func TestToSnakeCase(t *testing.T) {
	snakeCaseTest := []struct{ input, output string }{
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
		if got, want := GetSnakeName(tt.input), tt.output; got != want {
			t.Errorf("want '%s', got '%s'", want, got)
		}
	}
}

func TestSnakeMemoize(t *testing.T) {
	key := "TestMemoize"
	if _, ok := snakeMemoizer.Load(key); ok {
		t.Errorf("expected key %q to not be memoized yet", key)
	}
	toSnakeCase(key)
	if val, ok := snakeMemoizer.Load(key); !ok || val.(string) != "test_memoize" {
		t.Errorf("want 'test_memoize', got '%v'", val)
	}
}

// TestSnakeCaseConcurrent tests that the sync.Map-based memoization
// works correctly under concurrent access from multiple goroutines.
func TestSnakeCaseConcurrent(t *testing.T) {
	const numGoroutines = 100
	const numIterations = 1000

	inputs := []string{
		"CamelCase", "AnotherCamelCase", "YetAnotherOne",
		"HTTPServer", "JSONParser", "XMLReader",
	}
	expected := map[string]string{
		"CamelCase":        "camel_case",
		"AnotherCamelCase": "another_camel_case",
		"YetAnotherOne":    "yet_another_one",
		"HTTPServer":       "http_server",
		"JSONParser":       "json_parser",
		"XMLReader":        "xml_reader",
	}

	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines)

	for i := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for range numIterations {
				for _, input := range inputs {
					result := GetSnakeName(input)
					if result != expected[input] {
						errChan <- fmt.Errorf("goroutine %d: GetSnakeName(%q) = %q, want %q",
							goroutineID, input, result, expected[input])
						return
					}
				}
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Error(err)
	}
}

// BenchmarkSnakeCaseCachedRead benchmarks the read path when values are
// already cached, which is the common case in production.
func BenchmarkSnakeCaseCachedRead(b *testing.B) {
	// Pre-populate the cache
	inputs := []string{
		"CamelCase", "AnotherCamelCase", "YetAnotherOne",
		"HTTPServer", "JSONParser", "XMLReader",
	}
	for _, input := range inputs {
		GetSnakeName(input)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			GetSnakeName(inputs[i%len(inputs)])
			i++
		}
	})
}
