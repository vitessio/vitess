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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// TestPKCollationsThatVTGateDoesNotCompare checks that VDiff compares a
// primary key in latin1_german2_ci as bytes, as it did before Vitess
// implemented the collation, and a key in latin1_swedish_ci in its collation.
// VTGate does not compare the text of rows in latin1_german2_ci, see
// evalengine.NeedsServerWeightString, and VDiff failed on such a key.
func TestPKCollationsThatVTGateDoesNotCompare(t *testing.T) {
	env := collations.MySQL8()
	tp := &tablePlan{
		dbName: "vt_ks",
		table:  &tabletmanagerdatapb.TableDefinition{Name: "t1"},
		comparePKs: []compareColInfo{
			{colIndex: 0, colName: "c1", isPK: true},
			{colIndex: 1, colName: "c2", isPK: true},
		},
	}

	dbc := binlogplayer.NewMockDBClient(t)
	dbc.ExpectRequestRE("select column_name as column_name, collation_name as collation_name from information_schema.columns .*", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("column_name|collation_name", "varchar|varchar"),
		"c1|latin1_german2_ci",
		"c2|latin1_swedish_ci",
	), nil)
	require.NoError(t, tp.getPKColumnCollations(dbc, env))
	assert.Equal(t, collations.Unknown, tp.comparePKs[0].collation)
	assert.Equal(t, env.LookupByName("latin1_swedish_ci"), tp.comparePKs[1].collation)

	td := &tableDiffer{wd: &workflowDiffer{collationEnv: env}, tablePlan: tp}
	row := []sqltypes.Value{sqltypes.NewVarChar("foo"), sqltypes.NewVarChar("bar")}
	cmp, err := td.compare(row, row, tp.comparePKs, false)
	require.NoError(t, err)
	assert.Zero(t, cmp)
}
