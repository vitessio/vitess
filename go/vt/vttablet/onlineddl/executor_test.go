/*
Copyright 2021 The Vitess Authors.

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
Functionality of this Executor is tested in go/test/endtoend/onlineddl/...
*/

package onlineddl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestValidateAndEditCreateTableStatement(t *testing.T) {
	e := Executor{}
	tt := []struct {
		name        string
		query       string
		expectError bool
	}{
		{
			name: "table with FK",
			query: `
            create table onlineddl_test (
                id int auto_increment,
                i int not null,
                parent_id int not null,
                primary key(id),
                constraint test_fk foreign key (parent_id) references onlineddl_test_parent (id) on delete no action
              ) auto_increment=1;
            `,
			expectError: true,
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := sqlparser.ParseStrictDDL(tc.query)
			require.NoError(t, err)
			alterTable, ok := stmt.(*sqlparser.CreateTable)
			require.True(t, ok)

			err = e.validateAndEditCreateTableStatement(context.Background(), alterTable)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
