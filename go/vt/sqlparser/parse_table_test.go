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

package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTable(t *testing.T) {
	testcases := []struct {
		input    string
		keyspace string
		table    string
		err      bool
	}{{
		input:    "t",
		keyspace: "",
		table:    "t",
	}, {
		input:    "k.t",
		keyspace: "k",
		table:    "t",
	}, {
		input:    "`k.k`.`t``.t`",
		keyspace: "k.k",
		table:    "t`.t",
	}, {
		input: ".",
		err:   true,
	}, {
		input: "t,",
		err:   true,
	}, {
		input: "t,",
		err:   true,
	}, {
		input: "t..",
		err:   true,
	}, {
		input: "k.t.",
		err:   true,
	}}
	for _, tcase := range testcases {
		keyspace, table, err := ParseTable(tcase.input)
		assert.Equal(t, tcase.keyspace, keyspace)
		assert.Equal(t, tcase.table, table)
		if tcase.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}
