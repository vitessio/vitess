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

package textutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitDelimitedList(t *testing.T) {
	defaultList := []string{"one", "two", "three"}
	tt := []struct {
		s    string
		list []string
	}{
		{s: "one,two,three"},
		{s: "one, two, three"},
		{s: "one,two; three  "},
		{s: "one two three"},
		{s: "one,,,two,three"},
		{s: " one, ,two,  three "},
	}

	for _, tc := range tt {
		if tc.list == nil {
			tc.list = defaultList
		}
		list := SplitDelimitedList(tc.s)
		assert.Equal(t, tc.list, list)
	}
}
