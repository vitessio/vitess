/*
Copyright 2020 The Vitess Authors.

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

package vreplication

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilterOptions(t *testing.T) {
	type TestCase struct {
		currentOptions  string
		optionsToRemove []string
		optionsToAdd    []string
		newOptions      string
	}
	tcases := []*TestCase{
		{
			"TRADITIONAL,NO_ZERO_IN_DATE,NO_ZERO_DATE,SOME_OPTION",
			[]string{"TRADITIONAL", "NO_ZERO_DATE", "NO_ZERO_IN_DATE"},
			[]string{"ALLOW_INVALID_DATES"},
			"SOME_OPTION,ALLOW_INVALID_DATES",
		},
		{
			"STRICT_TRANS_TABLE,NO_ZERO_IN_DATE,NO_ZERO_DATE",
			[]string{"TRADITIONAL", "NO_ZERO_DATE", "NO_ZERO_IN_DATE"},
			[]string{"ALLOW_INVALID_DATES"},
			"STRICT_TRANS_TABLE,ALLOW_INVALID_DATES",
		},
	}
	for _, tcase := range tcases {
		require.Equal(t, tcase.newOptions, filterOptions(tcase.currentOptions, tcase.optionsToRemove, tcase.optionsToAdd))
	}
}
