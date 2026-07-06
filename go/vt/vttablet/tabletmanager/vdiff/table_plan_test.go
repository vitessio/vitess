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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/schemadiff"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// TestGetPKEquivalentColumnsUnparseableSchema ensures an unparseable SHOW
// CREATE TABLE result (e.g. one using the unknown SECONDARY_ENGINE option)
// yields no PKE and no error, so the caller can fall back to all columns.
func TestGetPKEquivalentColumnsUnparseableSchema(t *testing.T) {
	vdenv := newTestVDiffEnv(t)
	defer vdenv.close()

	dbc := binlogplayer.NewMockDBClient(t)
	tp := &tablePlan{
		dbName: vdiffDBName,
		table:  &tabletmanagerdatapb.TableDefinition{Name: "nopk"},
	}
	dbc.ExpectRequestRE("SHOW CREATE TABLE", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"Table|Create Table",
			"varchar|varchar",
		),
		"nopk|CREATE TABLE `nopk` (`c1` int DEFAULT NULL) ENGINE=InnoDB SECONDARY_ENGINE=RAPID",
	), nil)

	pkeCols, err := tp.getPKEquivalentColumns(dbc, schemadiff.NewEnvWithDefaults(vdenv.vde.env))
	require.NoError(t, err)
	require.Empty(t, pkeCols)
	dbc.Wait()
}
