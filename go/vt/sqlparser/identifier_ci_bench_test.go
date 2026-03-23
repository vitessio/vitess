/*
Copyright 2026 The Vitess Authors.

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

package sqlparser

import "testing"

func BenchmarkIdentifierCI_EqualString(b *testing.B) {
	benchmarks := []struct {
		name  string
		ident string
		cmp   string
		match bool
	}{
		{"short/match", "id", "ID", true},
		{"short/nomatch", "id", "name", false},
		{"medium/match", "column_name", "COLUMN_NAME", true},
		{"medium/nomatch", "column_name", "table_name", false},
		{"long/match", "some_really_long_column_name_here", "SOME_REALLY_LONG_COLUMN_NAME_HERE", true},
		{"long/nomatch", "some_really_long_column_name_here", "another_very_long_column_name_too", false},
	}

	for _, bm := range benchmarks {
		node := NewIdentifierCI(bm.ident)
		b.Run(bm.name, func(b *testing.B) {
			for b.Loop() {
				_ = node.EqualString(bm.cmp)
			}
		})
	}
}
