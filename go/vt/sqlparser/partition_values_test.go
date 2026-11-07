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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsParenthesizedTwice(t *testing.T) {
	testcases := []struct {
		text  string
		twice bool
	}{
		{"((1, 2))", true},
		{"( ( 1, 2 ) )", true},
		{"(((1, 2)))", true},
		{"(1, 2)", false},
		{"((1) + 1, 2)", false},
		{"((1), (2))", false},
		{"('((', 1)", false},
		{"(1)", false},
	}
	for _, tc := range testcases {
		t.Run(tc.text, func(t *testing.T) {
			assert.Equal(t, tc.twice, isParenthesizedTwice(tc.text))
		})
	}
}

// TestTakePartitionValuesMissing tests that values that did not record where
// they are fail closed.
func TestTakePartitionValuesMissing(t *testing.T) {
	tkn := &Tokenizer{buf: "(1)"}
	assert.Equal(t, partitionColumnListError, tkn.checkPartitionValues(ValTuple{NewIntLiteral("1")}, true))
}
