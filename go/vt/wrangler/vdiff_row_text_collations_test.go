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

package wrangler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/vtenv"
)

// TestVDiffColumnCollationsThatVTGateDoesNotCompare checks that VDiff compares
// a column in latin1_german2_ci as bytes, as it did before Vitess implemented
// the collation, and one in latin1_swedish_ci in its collation. VTGate does
// not compare the text of rows in latin1_german2_ci, see
// evalengine.NeedsServerWeightString, and VDiff failed on such a column.
func TestVDiffColumnCollationsThatVTGateDoesNotCompare(t *testing.T) {
	venv := vtenv.NewTestEnv()
	table := &tabletmanagerdatapb.TableDefinition{
		Name:   "t1",
		Schema: "create table t1 (c1 varchar(10) collate latin1_german2_ci, c2 varchar(10) collate latin1_swedish_ci, primary key (c1))",
	}
	columnCollations, _, err := getColumnCollations(venv, table)
	require.NoError(t, err)
	assert.Equal(t, collations.Unknown, columnCollations["c1"])
	assert.Equal(t, venv.CollationEnv().LookupByName("latin1_swedish_ci"), columnCollations["c2"])

	td := &tableDiffer{env: venv}
	cols := []compareColInfo{
		{colIndex: 0, collation: columnCollations["c1"], isPK: true},
		{colIndex: 1, collation: columnCollations["c2"]},
	}
	row := []sqltypes.Value{sqltypes.NewVarChar("foo"), sqltypes.NewVarChar("bar")}
	cmp, err := td.compare(row, row, cols, false)
	require.NoError(t, err)
	assert.Zero(t, cmp)
}
