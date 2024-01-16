/*
Copyright 2023 The Vitess Authors.

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

package maps2

import (
	"reflect"
	"sort"
	"testing"
)

func TestMaps(t *testing.T) {
	tests := []struct {
		name   string
		m      map[int]string
		keys   []int
		values []string
	}{
		{
			name:   "EmptyMap",
			m:      map[int]string{},
			keys:   []int{},
			values: []string{},
		},
		{
			name:   "NonEmptyMap",
			m:      map[int]string{1: "one", 2: "two", 3: "three"},
			keys:   []int{1, 2, 3},
			values: []string{"one", "two", "three"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKeys := Keys(tt.m)
			// Sort the keys so that we can compare them
			sort.Ints(gotKeys)
			sort.Ints(tt.keys)
			if !reflect.DeepEqual(gotKeys, tt.keys) {
				t.Errorf("Keys() = %v, want %v", gotKeys, tt.keys)
			}

			gotValues := Values(tt.m)
			// Sort the values so that we can compare them
			sort.Strings(gotValues)
			sort.Strings(tt.values)
			if !reflect.DeepEqual(gotValues, tt.values) {
				t.Errorf("Values() = %v, want %v", gotValues, tt.values)
			}
		})
	}
}
