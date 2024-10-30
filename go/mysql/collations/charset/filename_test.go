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

package charset

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTablenameToFilename(t *testing.T) {
	testCases := []struct {
		tablename string
		filename  string
	}{
		{
			tablename: "my_table123",
			filename:  "my_table123",
		},
		{
			tablename: "my-table",
			filename:  "my@002dtable",
		},
		{
			tablename: "my$table",
			filename:  "my@0024table",
		},
		{
			tablename: "myát",
			filename:  "my@0ht",
		},
		{
			tablename: "myÃt",
			filename:  "my@0jt",
		},
		{
			tablename: "myאt",
			filename:  "my@05d0t",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.tablename, func(t *testing.T) {
			filename := TablenameToFilename(tc.tablename)
			assert.Equal(t, tc.filename, filename, "original bytes: %x", []byte(tc.tablename))
		})
	}
}
