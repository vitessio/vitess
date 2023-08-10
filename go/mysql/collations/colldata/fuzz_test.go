//go:build go1.18

/*
Copyright 2021 The Vitess Authors.

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

// The fuzzing tests for collations use the new Fuzz implementation in Go 1.18+

package colldata

import (
	"testing"
)

func FuzzUCACollate(f *testing.F) {
	for _, left := range AllTestStrings {
		for _, right := range AllTestStrings {
			f.Add([]byte(left.Content), []byte(right.Content))
		}
	}

	coll := testcollation(f, "utf8mb4_0900_ai_ci")

	f.Fuzz(func(t *testing.T, left, right []byte) {
		_ = coll.Collate(left, right, false)
	})
}

func FuzzUCAWeightStrings(f *testing.F) {
	for _, input := range AllTestStrings {
		f.Add([]byte(input.Content))
	}

	coll := testcollation(f, "utf8mb4_0900_ai_ci")

	f.Fuzz(func(t *testing.T, input []byte) {
		_ = coll.WeightString(nil, input, 0)
	})
}
