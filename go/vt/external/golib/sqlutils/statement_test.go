/*
   Copyright 2017 GitHub Inc.

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

/*
	This file has been copied over from VTOrc package
*/

package sqlutils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsCreateIndex(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"create index my_index on my_table(column);", true},
		{"create unique index my_index on my_table(column);", true},
		{"create index my_index on my_table(column) where condition;", true},
		{"create unique index my_index on my_table(column) where condition;", true},
		{"create table my_table(column);", false},
		{"drop index my_index on my_table;", false},
		{"alter table my_table add index my_index (column);", false},
		{"", false},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			result := IsCreateIndex(test.input)
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestIsDropIndex(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"drop index my_index on my_table;", true},
		{"drop index if exists my_index on my_table;", true},
		{"drop table my_table;", false},
		{"create index my_index on my_table(column);", false},
		{"alter table my_table add index my_index (column);", false},
		{"", false},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			result := IsDropIndex(test.input)
			assert.Equal(t, test.expected, result)
		})
	}
}
