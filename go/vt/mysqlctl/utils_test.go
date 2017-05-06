/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysqlctl

import (
	"errors"
	"testing"
)

func TestConcurrentMap(t *testing.T) {
	work := make([]int, 10)
	result := make([]int, 10)
	for i := 0; i < 10; i++ {
		work[i] = i
	}
	mapFunc := func(i int) error {
		result[i] = work[i]
		return nil
	}
	if err := ConcurrentMap(2, 10, mapFunc); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	for i := 0; i < 10; i++ {
		if got, expected := result[i], work[i]; got != expected {
			t.Errorf("Wrong values in result: got %v, expected %v", got, expected)
		}
	}
	fooErr := errors.New("foo")
	if err := ConcurrentMap(2, 10, func(i int) error { return fooErr }); err != fooErr {
		t.Errorf("Didn't get expected error: %v", err)
	}
}
