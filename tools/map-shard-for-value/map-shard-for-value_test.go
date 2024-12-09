/*
Copyright 2024 The Vitess Authors.

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

package main

import (
	"bufio"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcess(t *testing.T) {
	type testCase struct {
		name       string
		shardsCSV  string
		vindexType string
		values     []int
		valueType  string
		expected   []output
	}
	testCases := []testCase{
		{
			name:       "hash,2 shards",
			shardsCSV:  "-80,80-",
			vindexType: "hash",
			values:     []int{1, 99},
			valueType:  "int",
			expected: []output{
				{
					Value:      "1",
					KeyspaceID: "166b40b44aba4bd6",
					Shard:      "-80",
				},
				{
					Value:      "99",
					KeyspaceID: "2c40ad56f4593c47",
					Shard:      "-80",
				},
			},
		},
		{
			name:       "xxhash,4 shards",
			shardsCSV:  "-40,40-80,80-c0,c0-",
			vindexType: "xxhash",
			values:     []int{1, 99},
			valueType:  "int",
			expected: []output{
				{
					Value:      "1",
					KeyspaceID: "d46405367612b4b7",
					Shard:      "c0-",
				},
				{
					Value:      "99",
					KeyspaceID: "200533312244abca",
					Shard:      "-40",
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var input strings.Builder
			for _, num := range tc.values {
				fmt.Fprintf(&input, "%d\n", num)
			}
			reader := strings.NewReader(input.String())
			scanner := bufio.NewScanner(reader)
			got, err := processValues(scanner, &tc.shardsCSV, tc.vindexType, tc.valueType)
			require.NoError(t, err)
			require.EqualValues(t, tc.expected, got)
		})
	}
}
