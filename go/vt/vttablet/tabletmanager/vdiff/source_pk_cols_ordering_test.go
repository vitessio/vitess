/*
Copyright 2026 The Vitess Authors.

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

package vdiff

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// TestSourcePKColsOrdering verifies that getSourcePKCols maps PK columns
// against the source query's SELECT expression order (the actual row layout),
// not against td.table.Columns (column ordinal position).
//
// Before the fix, the function mapped PK columns against td.table.Columns,
// producing indices in ordinal order. When the source query reorders columns
// (e.g., "select c2, c1 from t1"), lastPKFromRow would index into the wrong
// position, corrupting the resume checkpoint.
func TestSourcePKColsOrdering(t *testing.T) {
	tvde := newTestVDiffEnv(t)
	defer tvde.close()

	ct := tvde.createController(t, 1)

	testCases := []struct {
		name string
		// sourceTable is the physical table the source query reads from
		// (the table in the FROM clause). getSourcePKCols resolves the source
		// schema from this name, which can differ from the target table name.
		sourceTable       string
		columns           []string
		primaryKeyColumns []string
		sourceQuery       string
		wantSourcePkCols  []int
	}{
		{
			name:              "columns in natural order",
			sourceTable:       "t",
			columns:           []string{"c1", "c2"},
			primaryKeyColumns: []string{"c1"},
			sourceQuery:       "select c1, c2 from t order by c1 asc",
			wantSourcePkCols:  []int{0},
		},
		{
			name:              "columns reordered in select",
			sourceTable:       "t",
			columns:           []string{"c1", "c2"},
			primaryKeyColumns: []string{"c1"},
			sourceQuery:       "select c2, c1 from t order by c1 asc",
			wantSourcePkCols:  []int{1},
		},
		{
			name:              "composite pk reordered in select",
			sourceTable:       "t",
			columns:           []string{"a", "b", "c"},
			primaryKeyColumns: []string{"c", "a"},
			sourceQuery:       "select a, b, c from t order by c asc, a asc",
			wantSourcePkCols:  []int{2, 0},
		},
		{
			name:              "composite pk with select reorder",
			sourceTable:       "t",
			columns:           []string{"a", "b", "c", "d"},
			primaryKeyColumns: []string{"b", "d"},
			sourceQuery:       "select d, c, b, a from t order by b asc, d asc",
			wantSourcePkCols:  []int{2, 0},
		},
		{
			// Cross-table MoveTables filter: the source table is t2 and its
			// physical PK is c0, renamed to the target column c1 in the SELECT.
			// The source PK c0 is matched via its underlying ColName.
			name:              "renamed physical source column resolves from source table",
			sourceTable:       "t2",
			columns:           []string{"c0", "c2"},
			primaryKeyColumns: []string{"c0"},
			sourceQuery:       "select c0 as c1, c2 from t2 order by c1 asc",
			wantSourcePkCols:  []int{0},
		},
		{
			name:              "pk matches ordinal order",
			sourceTable:       "t",
			columns:           []string{"a", "b", "c"},
			primaryKeyColumns: []string{"a", "b"},
			sourceQuery:       "select a, b, c from t order by a asc, b asc",
			wantSourcePkCols:  []int{0, 1},
		},
		{
			name:              "column swap alias does not shadow real pk",
			sourceTable:       "t",
			columns:           []string{"a", "b"},
			primaryKeyColumns: []string{"a"},
			sourceQuery:       "select b as a, a as b from t order by a asc",
			wantSourcePkCols:  []int{1},
		},
		{
			name:              "column swap composite pk prefers colname over alias",
			sourceTable:       "t",
			columns:           []string{"a", "b", "c"},
			primaryKeyColumns: []string{"a", "b"},
			sourceQuery:       "select b as a, a as b, c from t order by a asc, b asc",
			wantSourcePkCols:  []int{1, 0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			types := make([]string, len(tc.columns))
			for i := range types {
				types[i] = "varbinary"
			}
			fieldTypes := strings.Join(types, "|")
			fields := strings.Join(tc.columns, "|")

			// The source schema is keyed by the physical source table name so
			// that getSourcePKCols resolves it from the query's FROM clause.
			sourceTable := &tabletmanagerdatapb.TableDefinition{
				Name:              tc.sourceTable,
				Columns:           tc.columns,
				PrimaryKeyColumns: tc.primaryKeyColumns,
				Fields:            sqltypes.MakeTestFields(fields, fieldTypes),
			}

			tvde.tmc.schema = &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{sourceTable},
			}

			td := &tableDiffer{
				wd: &workflowDiffer{
					ct: ct,
				},
				table: sourceTable,
				tablePlan: &tablePlan{
					table:       sourceTable,
					sourceQuery: tc.sourceQuery,
				},
			}

			err := td.getSourcePKCols()
			require.NoError(t, err)

			assert.Equal(t, tc.wantSourcePkCols, td.tablePlan.sourcePkCols,
				"sourcePkCols should reflect PK column positions in the source SELECT expression list")
		})
	}
}

// TestSourcePKSelectIndices tests the extracted sourcePKSelectIndices function
// directly with parsed queries, without needing topo/tablet infrastructure.
func TestSourcePKSelectIndices(t *testing.T) {
	testCases := []struct {
		name        string
		sourceQuery string
		pkColumns   []string
		wantIndices []int
		// wantErr is only for the invariant guard: an unexpanded '*' in the
		// SELECT list that buildTablePlan should already have expanded.
		wantErr bool
		// wantNotProjected covers a PK column not projected as a physical column
		// (absent, or present only via a non-physical expression): no error,
		// allProjected == false, and no partial index slice.
		wantNotProjected bool
	}{
		{
			name:        "natural order single PK",
			sourceQuery: "select c1, c2 from t1 order by c1 asc",
			pkColumns:   []string{"c1"},
			wantIndices: []int{0},
		},
		{
			name:        "reordered columns single PK",
			sourceQuery: "select c2, c1 from t1 order by c1 asc",
			pkColumns:   []string{"c1"},
			wantIndices: []int{1},
		},
		{
			name:        "composite PK natural order",
			sourceQuery: "select c1, c2 from multipk order by c1 asc, c2 asc",
			pkColumns:   []string{"c1", "c2"},
			wantIndices: []int{0, 1},
		},
		{
			name:        "composite PK columns reordered in select",
			sourceQuery: "select c2, c1 from multipk order by c1 asc, c2 asc",
			pkColumns:   []string{"c1", "c2"},
			wantIndices: []int{1, 0},
		},
		{
			name:        "composite PK 4 columns fully reversed",
			sourceQuery: "select d, c, b, a from t order by b asc, d asc",
			pkColumns:   []string{"b", "d"},
			wantIndices: []int{2, 0},
		},
		{
			// Cross-table MoveTables filter: the physical source column c0 is
			// renamed to the target column c1. The source PK is the physical
			// column c0, which we match via the underlying ColName.
			name:        "renamed physical source column (cross-table MoveTables filter)",
			sourceQuery: "select c0 as c1, c2 from t2 order by c1 asc",
			pkColumns:   []string{"c0"},
			wantIndices: []int{0},
		},
		{
			// A CONVERT(col USING charset) rename must unwrap to the inner
			// physical source column, mirroring the row streamer planner.
			name:        "convert using rename unwraps to source column",
			sourceQuery: "select convert(c1 using utf8mb4) as c2, c3 from t order by c2 asc",
			pkColumns:   []string{"c1"},
			wantIndices: []int{0},
		},
		{
			// A nested CONVERT (e.g. wrapping a CAST) is NOT a direct column
			// rename, so it is not treated as a physical PK column and fails
			// closed. Here c1 is entirely absent otherwise, so the whole key is
			// reported as not projected.
			name:             "nested convert using is not a physical column",
			sourceQuery:      "select convert(cast(c1 as char) using utf8mb4) as c2, c3 from t order by c2 asc",
			pkColumns:        []string{"c1"},
			wantNotProjected: true,
		},
		{
			// A computed expression wrapped in CONVERT aliased back to the PK name
			// is a derived value, not the physical column, so it is treated as not
			// projected here. Merge-ordering safety is enforced by the caller's
			// comparisonKeyIsSourcePKPrefix check.
			name:             "computed convert aliased to PK name is not projected",
			sourceQuery:      "select convert(concat(c1, 'x') using utf8mb4) as c1, c2 from t order by c1 asc",
			pkColumns:        []string{"c1"},
			wantNotProjected: true,
		},
		{
			name:        "function expression with alias",
			sourceQuery: "select c1, c2, count(*) as c3, sum(c4) as c4 from t group by c1 order by c1 asc",
			pkColumns:   []string{"c1"},
			wantIndices: []int{0},
		},
		{
			name:        "PK at last position",
			sourceQuery: "select a, b, c, id from t order by id asc",
			pkColumns:   []string{"id"},
			wantIndices: []int{3},
		},
		{
			name:        "case insensitive match",
			sourceQuery: "select ID, Name from t order by ID asc",
			pkColumns:   []string{"id"},
			wantIndices: []int{0},
		},
		{
			// A PK column entirely absent from the SELECT list is a valid
			// subset-projection filter: no error, and allProjected is false so
			// the caller does not build a partial source key.
			name:             "PK entirely absent from select list is not projected",
			sourceQuery:      "select a, b from t order by a asc",
			pkColumns:        []string{"missing_col"},
			wantNotProjected: true,
		},
		{
			// Mirrors the customer CI case: composite source PK (cid, typ) with
			// a filter that projects only cid. typ is entirely absent, so the
			// whole key is treated as not projected (never a partial [0] slice).
			name:             "composite PK with one column absent is not projected",
			sourceQuery:      "select cid, name from customer order by cid asc",
			pkColumns:        []string{"cid", "typ"},
			wantNotProjected: true,
		},
		{
			name:        "in_keyrange filter preserves column positions",
			sourceQuery: "select c1, c2 from t1 where in_keyrange('-80') order by c1 asc",
			pkColumns:   []string{"c1"},
			wantIndices: []int{0},
		},
		{
			name:        "three PKs scattered across wide select",
			sourceQuery: "select a, b, c, d, e, f from t order by b asc, d asc, f asc",
			pkColumns:   []string{"b", "d", "f"},
			wantIndices: []int{1, 3, 5},
		},
		{
			// The source PKs are the physical columns src_a and src_b, matched
			// via their underlying ColNames even though they are renamed.
			name:        "multi-column cross-table filter matches physical columns",
			sourceQuery: "select src_a as id, src_b as name, src_c as value from source_t order by id asc",
			pkColumns:   []string{"src_a", "src_b"},
			wantIndices: []int{0, 1},
		},
		{
			// A computed alias is a derived value, not the physical column, so it is
			// treated as not projected here (correctness is enforced by the caller's
			// prefix check when this column is the comparison key).
			name:             "computed alias claiming source PK name is not projected",
			sourceQuery:      "select a + b as id, c from t order by id asc",
			pkColumns:        []string{"id"},
			wantNotProjected: true,
		},
		{
			// An alias mapping an unrelated physical column to the source PK name
			// does not match the physical PK, so it is treated as not projected
			// (the caller's prefix check rejects it when id is the comparison key).
			name:             "unrelated column aliased to source PK name is not projected",
			sourceQuery:      "select other_col as id, c from t order by id asc",
			pkColumns:        []string{"id"},
			wantNotProjected: true,
		},
		{
			// Invariant guard: buildTablePlan must expand "*" into explicit
			// columns before sourcePKSelectIndices runs. A StarExpr reaching this
			// function means a caller violated that invariant, so we fail loud
			// rather than silently treating PK columns as not projected.
			name:        "unexpanded star fails loud (invariant guard)",
			sourceQuery: "select * from t order by a asc",
			pkColumns:   []string{"a"},
			wantErr:     true,
		},
		{
			name:        "column swap alias does not shadow real PK",
			sourceQuery: "select b as a, a as b from t order by a asc",
			pkColumns:   []string{"a"},
			wantIndices: []int{1},
		},
		{
			name:        "column swap composite PK prefers ColName over alias",
			sourceQuery: "select b as a, a as b, c from t order by a asc, b asc",
			pkColumns:   []string{"a", "b"},
			wantIndices: []int{1, 0},
		},
	}

	parser := sqlparser.NewTestParser()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			statement, err := parser.Parse(tc.sourceQuery)
			require.NoError(t, err)
			sourceSelect, ok := statement.(*sqlparser.Select)
			require.True(t, ok)

			indices, allProjected, err := sourcePKSelectIndices(sourceSelect, tc.pkColumns)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.wantNotProjected {
				assert.False(t, allProjected)
				assert.Empty(t, indices, "must not return a partial index slice when a PK column is absent")
				return
			}
			assert.True(t, allProjected)
			assert.Equal(t, tc.wantIndices, indices)
		})
	}
}

// TestComparisonKeyIsSourcePKPrefix verifies the merge-safety gate used for
// subset-projection filters: the columns VDiff merge-sorts on (comparePKs) must
// be an order-preserving prefix of the physical source PK, since the row streamer
// always emits source rows ordered by that physical PK.
func TestComparisonKeyIsSourcePKPrefix(t *testing.T) {
	testCases := []struct {
		name        string
		sourceQuery string
		// comparePKColIndices are the SELECT-list positions of the comparison
		// columns, in comparison order (as findPKs would populate comparePKs).
		comparePKColIndices []int
		sourcePKColumns     []string
		wantErr             bool
	}{
		{
			// Documented safe case: source PK (cid, typ) compared on cid. cid is a
			// prefix of (cid, typ), so a source stream ordered by (cid, typ) is also
			// ordered by cid.
			name:                "prefix compare on leading pk column",
			sourceQuery:         "select cid, name from customer order by cid asc",
			comparePKColIndices: []int{0},
			sourcePKColumns:     []string{"cid", "typ"},
		},
		{
			// Unsafe: source PK (typ, cid) compared on cid. cid is not a prefix of
			// (typ, cid), so a stream ordered by (typ, cid) is not ordered by cid.
			name:                "compare on non-leading pk column is rejected",
			sourceQuery:         "select cid, name from customer order by cid asc",
			comparePKColIndices: []int{0},
			sourcePKColumns:     []string{"typ", "cid"},
			wantErr:             true,
		},
		{
			// Full-length prefix (equal) is safe.
			name:                "full pk compare in order",
			sourceQuery:         "select cid, typ from customer order by cid asc, typ asc",
			comparePKColIndices: []int{0, 1},
			sourcePKColumns:     []string{"cid", "typ"},
		},
		{
			// Comparison key has more columns than the physical source PK.
			name:                "comparison key longer than source pk is rejected",
			sourceQuery:         "select cid, typ from customer order by cid asc, typ asc",
			comparePKColIndices: []int{0, 1},
			sourcePKColumns:     []string{"cid"},
			wantErr:             true,
		},
		{
			// A comparison column that is a computed value (not a physical column)
			// cannot match the physical PK ordering.
			name:                "computed comparison column is rejected",
			sourceQuery:         "select a + b as id, c from t order by id asc",
			comparePKColIndices: []int{0},
			sourcePKColumns:     []string{"id"},
			wantErr:             true,
		},
	}

	parser := sqlparser.NewTestParser()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			statement, err := parser.Parse(tc.sourceQuery)
			require.NoError(t, err)
			sourceSelect, ok := statement.(*sqlparser.Select)
			require.True(t, ok)

			comparePKs := make([]compareColInfo, len(tc.comparePKColIndices))
			for i, idx := range tc.comparePKColIndices {
				comparePKs[i] = compareColInfo{colIndex: idx, isPK: true}
			}

			err = comparisonKeyIsSourcePKPrefix(sourceSelect, comparePKs, tc.sourcePKColumns)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestLastPKFromRowMixedTypeReorder verifies that lastPKFromRow attaches the
// correct field (and therefore type) to each PK value when the filter reorders
// columns relative to their DDL order. The checkpoint values are round-tripped
// through the same Proto3ToResult path the row streamer uses to quote the resume
// predicate, so a numeric PK value must not be reconstructed as a string.
func TestLastPKFromRowMixedTypeReorder(t *testing.T) {
	// Table PK is (id int, code varchar); DDL/Fields order is [id, code]. The
	// filter reorders them: "select code, id from t", so the streamed row layout
	// (SELECT order) is [code, id].
	table := &tabletmanagerdatapb.TableDefinition{
		Name:              "t",
		Columns:           []string{"id", "code"},
		PrimaryKeyColumns: []string{"id", "code"},
		Fields:            sqltypes.MakeTestFields("id|code", "int64|varchar"),
	}

	td := &tableDiffer{
		tablePlan: &tablePlan{
			table: table,
			// compareCols is indexed by SELECT position: 0 -> code, 1 -> id.
			compareCols: []compareColInfo{
				{colIndex: 0, colName: "code", isPK: true},
				{colIndex: 1, colName: "id", isPK: true},
			},
			// PK order is (id, code): id is at SELECT position 1, code at 0.
			pkCols:       []int{1, 0},
			sourcePkCols: []int{1, 0},
		},
	}

	// Streamed row in SELECT order: [code="abc", id=5].
	row := []sqltypes.Value{sqltypes.NewVarChar("abc"), sqltypes.NewInt64(5)}

	lastPK := td.lastPKFromRow(row)
	require.NotNil(t, lastPK.Target)

	// The lastpk is emitted in PK order (id, code): the integer id first with an
	// integer field, then the varchar code. Indexing table.Fields by SELECT
	// position would have paired id's value with code's (varchar) field.
	require.Len(t, lastPK.Target.Fields, 2)
	assert.EqualValues(t, "id", lastPK.Target.Fields[0].Name)
	assert.Equal(t, querypb.Type_INT64, lastPK.Target.Fields[0].Type)
	assert.EqualValues(t, "code", lastPK.Target.Fields[1].Name)
	assert.Equal(t, querypb.Type_VARCHAR, lastPK.Target.Fields[1].Type)

	// Round-trip through the row streamer's reconstruction path: the id value must
	// stay integral (so EncodeSQL emits an unquoted number) and code stays quoted.
	result := sqltypes.Proto3ToResult(lastPK.Target)
	require.Len(t, result.Rows, 1)
	assert.True(t, result.Rows[0][0].IsIntegral(), "id checkpoint value should be integral, got %v", result.Rows[0][0])
	assert.False(t, result.Rows[0][0].IsQuoted(), "id checkpoint value must not be quoted, got %v", result.Rows[0][0])
	assert.True(t, result.Rows[0][1].IsQuoted(), "code checkpoint value should be quoted, got %v", result.Rows[0][1])
}

// TestLastPKFromRowTypeChangingRename verifies that when a filter renames a
// physical source PK column to a differently-typed target column, the source
// checkpoint is quoted with the source type (not the target type). Otherwise the
// row streamer would build a resume predicate that quotes a numeric source key as
// a string.
func TestLastPKFromRowTypeChangingRename(t *testing.T) {
	// Source PK is "id" (int64); the filter renames it to the target column
	// "id_str" (varchar): "select id as id_str from t". The source and target PKs
	// occupy the same SELECT position but have different types.
	targetTable := &tabletmanagerdatapb.TableDefinition{
		Name:              "t",
		Columns:           []string{"id_str"},
		PrimaryKeyColumns: []string{"id_str"},
		Fields:            sqltypes.MakeTestFields("id_str", "varchar"),
	}

	td := &tableDiffer{
		tablePlan: &tablePlan{
			table:       targetTable,
			compareCols: []compareColInfo{{colIndex: 0, colName: "id_str", isPK: true}},
			pkCols:      []int{0},
			// The source PK "id" is at the same SELECT position but is an int64.
			sourcePkCols: []int{0},
		},
	}

	// Streamed source row: the physical source value is numeric. lastPKFromRow
	// takes the source column type from this value, not the target schema.
	row := []sqltypes.Value{sqltypes.NewInt64(5)}

	lastPK := td.lastPKFromRow(row)

	// The differing types force a distinct source checkpoint rather than reusing
	// the target value.
	require.NotNil(t, lastPK.Source, "a type-changing rename must persist a source checkpoint")

	// The source key stays numeric (unquoted); the target key is a varchar (quoted).
	source := sqltypes.Proto3ToResult(lastPK.Source)
	require.Len(t, source.Rows, 1)
	assert.True(t, source.Rows[0][0].IsIntegral(), "source checkpoint value should stay integral, got %v", source.Rows[0][0])
	assert.False(t, source.Rows[0][0].IsQuoted(), "source checkpoint value must not be quoted, got %v", source.Rows[0][0])

	target := sqltypes.Proto3ToResult(lastPK.Target)
	require.Len(t, target.Rows, 1)
	assert.True(t, target.Rows[0][0].IsQuoted(), "target checkpoint value should be quoted, got %v", target.Rows[0][0])
}
