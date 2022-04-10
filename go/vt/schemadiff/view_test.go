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

package schemadiff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestCreateViewDiff(t *testing.T) {
	tt := []struct {
		name    string
		from    string
		to      string
		diff    string
		isError bool
	}{
		{
			name: "identical",
			from: "create view v1 as select a, b, c from t",
			to:   "create view v1 as select a, b, c from t",
		},
		{
			name: "identical, case change",
			from: "create view v1 as SELECT a, b, c from t",
			to:   "create view v1 as select a, b, c from t",
		},
		{
			name: "identical, case change on target",
			from: "create view v1 as select a, b, c from t",
			to:   "create view v1 as SELECT a, b, c from t",
		},
		{
			name: "identical, case and qualifiers",
			from: "create view v1 as select `a`, `b`, c from t",
			to:   "create view v1 as SELECT a, b, `c` from t",
		},
		{
			name: "identical, column list, qualified",
			from: "create view v1 (col1, `col2`, `col3`) as select `a`, `b`, c from t",
			to:   "create view v1 (`col1`, col2, col3) as select a, b, `c` from t",
		},
		{
			name: "change of column list, qualifiers",
			from: "create view v1 (col1, `col2`, `col3`) as select `a`, `b`, c from t",
			to:   "create view v1 (`col1`, col2, colother) as select a, b, `c` from t",
			diff: "alter view v1(col1, col2, colother) as select a, b, c from t",
		},
		{
			name: "change of column list, must have qualifiers",
			from: "create view v1 (col1, `col2`, `col3`) as select `a`, `b`, c from t",
			to:   "create view v1 (`col1.with.dot`, `col2`, colother) as select a, b, `c` from t",
			diff: "alter view v1(`col1.with.dot`, col2, colother) as select a, b, c from t",
		},
		{
			name: "identical, spacing, case change",
			from: "create view v1 as select a, b, c FROM    t",
			to: `create view v1 as
				SELECT a, b, c
				from t`,
		},
		{
			name: "change of query",
			from: "create view v1 as select a from t",
			to:   "create view v1 as select a, b from t",
			diff: "alter view v1 as select a, b from t",
		},
		{
			name: "change of view name",
			from: "create view v1 as select a from t",
			to:   "create view v2 as select a, b from t",
			diff: "alter view v1 as select a, b from t",
		},
		{
			name: "change of columns, spacing",
			from: "create view v1 as select a from t",
			to: `create view v2 as
				select a, b
				from t`,
			diff: "alter view v1 as select a, b from t",
		},
		{
			name: "algorithm, case change",
			from: "create view v1 as select a from t",
			to:   "create algorithm=temptable view v2 as select a FROM t",
			diff: "alter algorithm = temptable view v1 as select a from t",
		},
		{
			name: "algorith, case change 2",
			from: "create view v1 as select a FROM t",
			to:   "create algorithm=temptable view v2 as select a from t",
			diff: "alter algorithm = temptable view v1 as select a from t",
		},
		{
			name: "algorith, case change 3",
			from: "create ALGORITHM=MERGE view v1 as select a FROM t",
			to:   "create ALGORITHM=TEMPTABLE view v2 as select a from t",
			diff: "alter algorithm = TEMPTABLE view v1 as select a from t",
		},
		{
			name: "algorith value is case sensitive",
			from: "create ALGORITHM=TEMPTABLE view v1 as select a from t",
			to:   "create ALGORITHM=temptable view v2 as select a from t",
			diff: "alter algorithm = temptable view v1 as select a from t",
		},
		{
			name: "algorith value is case sensitive 2",
			from: "create ALGORITHM=temptable view v1 as select a from t",
			to:   "create ALGORITHM=TEMPTABLE view v2 as select a from t",
			diff: "alter algorithm = TEMPTABLE view v1 as select a from t",
		},
	}
	hints := &DiffHints{}
	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			fromStmt, err := sqlparser.Parse(ts.from)
			assert.NoError(t, err)
			fromCreateView, ok := fromStmt.(*sqlparser.CreateView)
			assert.True(t, ok)

			toStmt, err := sqlparser.Parse(ts.to)
			assert.NoError(t, err)
			toCreateView, ok := toStmt.(*sqlparser.CreateView)
			assert.True(t, ok)

			c := NewCreateViewEntity(fromCreateView)
			other := NewCreateViewEntity(toCreateView)
			alter, err := c.Diff(other, hints)
			switch {
			case ts.isError:
				assert.Error(t, err)
			case ts.diff == "":
				assert.NoError(t, err)
				assert.Nil(t, alter)
			default:
				assert.NoError(t, err)
				require.NotNil(t, alter)
				require.False(t, alter.IsEmpty())
				diff := alter.StatementString()
				assert.Equal(t, ts.diff, diff)
			}
		})
	}
}
