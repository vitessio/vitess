/*
Copyright 2022 The Vitess Authors.

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

package sqltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInfoParsing(t *testing.T) {
	testCases := []struct {
		info   string
		expect uint64
	}{
		// valid
		{"\000{\"rows_read\":42}", 42},
		{"\000{\"foo\":true,\"rows_read\":42,\"bar\":[1,2,3,4,\"hello\"]}", 42},

		// blank
		{"", 0},

		// splitter errors
		{"{\"rows_read\":42}", 0},
		{"\000\000{\"rows_read\":42}", 0},

		// json errors
		{"\000{\"rows_read\":42", 0},      // missing }
		{"\000{\"rows_read\"=42}", 0},     // = instead of :
		{"\000{rows_read:42}", 0},         // missing "
		{"\000{\"rows_read\":\"42\"}", 0}, // string instead of int
		{"\000{\"roes_red\":42}", 0},      // wrong key name
	}
	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			j := parseOKInfoJSON(tc.info)
			assert.Equal(t, tc.expect, j.RowsRead)
		})
	}
}
