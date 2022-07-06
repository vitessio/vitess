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

package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmptyErrorAndComments(t *testing.T) {
	testcases := []struct {
		input  string
		output string
		err    error
	}{
		{
			input:  "select 1",
			output: "select 1 from dual",
		}, {
			input: "",
			err:   ErrEmpty,
		}, {
			input: ";",
			err:   ErrEmpty,
		}, {
			input:  "-- sdf",
			output: "-- sdf",
		}, {
			input:  "/* sdf */",
			output: "/* sdf */",
		}, {
			input:  "# sdf",
			output: "# sdf",
		}, {
			input:  "/* sdf */ select 1",
			output: "select 1 from dual",
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.input, func(t *testing.T) {
			res, err := Parse(testcase.input)
			if testcase.err != nil {
				require.Equal(t, testcase.err, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, testcase.output, String(res))
			}
		})

		t.Run(testcase.input+"-Strict DDL", func(t *testing.T) {
			res, err := ParseStrictDDL(testcase.input)
			if testcase.err != nil {
				require.Equal(t, testcase.err, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, testcase.output, String(res))
			}
		})
	}
}
