/*
Copyright 2019 The Vitess Authors.

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

package wrangler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

func TestVDiffPlanSuccess(t *testing.T) {
	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}

	testcases := []struct {
		input *binlogdatapb.Rule
		table string
		td    *tableDiffer
	}{{
		input: &binlogdatapb.Rule{
			Match: "t1",
		},
		table: "t1",
		td: &tableDiffer{
			targetTable:      "t1",
			sourceExpression: "select c1, c2 from t1 order by c1 asc",
			targetExpression: "select c1, c2 from t1 order by c1 asc",
			compareCols:      []int{-1, 1},
			comparePKs:       []int{0},
		},
	}, {
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "-80",
		},
		table: "t1",
		td: &tableDiffer{
			targetTable:      "t1",
			sourceExpression: "select c1, c2 from t1 order by c1 asc",
			targetExpression: "select c1, c2 from t1 order by c1 asc",
			compareCols:      []int{-1, 1},
			comparePKs:       []int{0},
		},
	}, {
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select * from t1",
		},
		table: "t1",
		td: &tableDiffer{
			targetTable:      "t1",
			sourceExpression: "select c1, c2 from t1 order by c1 asc",
			targetExpression: "select c1, c2 from t1 order by c1 asc",
			compareCols:      []int{-1, 1},
			comparePKs:       []int{0},
		},
	}, {
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select c2, c1 from t1",
		},
		table: "t1",
		td: &tableDiffer{
			targetTable:      "t1",
			sourceExpression: "select c2, c1 from t1 order by c1 asc",
			targetExpression: "select c2, c1 from t1 order by c1 asc",
			compareCols:      []int{0, -1},
			comparePKs:       []int{1},
		},
	}, {
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select c0 as c1, c2 from t2",
		},
		table: "t1",
		td: &tableDiffer{
			targetTable:      "t1",
			sourceExpression: "select c0 as c1, c2 from t2 order by c1 asc",
			targetExpression: "select c1, c2 from t1 order by c1 asc",
			compareCols:      []int{-1, 1},
			comparePKs:       []int{0},
		},
	}}
	for _, tcase := range testcases {
		filter := &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{tcase.input}}
		differs, err := buildVDiffPlan(context.Background(), filter, schm)
		require.NoError(t, err, tcase.input)
		require.Equal(t, 1, len(differs), tcase.input)
		assert.Equal(t, tcase.td, differs[tcase.table], tcase.input)
	}
}
