/*
Copyright 2023 The Vitess Authors.

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
package grpcvtctldserver

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/vtctl/schematools"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vttimepb "vitess.io/vitess/go/vt/proto/vttime"
)

var now = time.Now()

func TestRowToSchemaMigration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		row       sqltypes.RowNamedValues
		expected  *vtctldatapb.SchemaMigration
		shouldErr bool
	}{
		{
			row: sqltypes.RowNamedValues(map[string]sqltypes.Value{
				"migration_uuid":      sqltypes.NewVarChar("abc"),
				"keyspace":            sqltypes.NewVarChar("testks"),
				"shard":               sqltypes.NewVarChar("shard"),
				"mysql_schema":        sqltypes.NewVarChar("_vt"),
				"mysql_table":         sqltypes.NewVarChar("t1"),
				"migration_statement": sqltypes.NewVarChar("alter table t1 rename foo to bar"),
				"strategy":            sqltypes.NewVarChar(schematools.SchemaMigrationStrategyName(vtctldatapb.SchemaMigration_ONLINE)),
				"requested_timestamp": sqltypes.NewTimestamp(mysqlTimestamp(now)),
				"eta_seconds":         sqltypes.NewInt64(10),
			}),
			expected: &vtctldatapb.SchemaMigration{
				Uuid:               "abc",
				Keyspace:           "testks",
				Shard:              "shard",
				Schema:             "_vt",
				Table:              "t1",
				MigrationStatement: "alter table t1 rename foo to bar",
				Strategy:           vtctldatapb.SchemaMigration_ONLINE,
				RequestedAt:        protoutil.TimeToProto(now.Truncate(time.Second)),
				EtaSeconds:         10,
			},
		},
		{
			name: "eta_seconds defaults to -1",
			row:  sqltypes.RowNamedValues(map[string]sqltypes.Value{}),
			expected: &vtctldatapb.SchemaMigration{
				Strategy:   vtctldatapb.SchemaMigration_DIRECT,
				EtaSeconds: -1,
			},
		},
		{
			name: "bad data",
			row: sqltypes.RowNamedValues(map[string]sqltypes.Value{
				"tablet": sqltypes.NewVarChar("not-an-alias"),
			}),
			shouldErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out, err := rowToSchemaMigration(test.row)
			if test.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, test.expected, out)
		})
	}
}

func mysqlTimestamp(t time.Time) string {
	return t.Local().Format(sqltypes.TimestampFormat)
}

func TestValueToVTTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		value     string
		expected  *vttimepb.Time
		shouldErr bool
	}{
		{
			value:    mysqlTimestamp(now),
			expected: protoutil.TimeToProto(now.Truncate(time.Second)),
		},
		{
			name:     "empty string",
			value:    "",
			expected: nil,
		},
		{
			name:      "parse error",
			value:     "2006/01/02",
			shouldErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			out, err := valueToVTTime(test.value)
			if test.shouldErr {
				assert.Error(t, err, "expected parse error")
				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, test.expected, out, "failed to convert %s into vttime", test.value)
		})
	}
}

func TestValueToVTDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		value       string
		defaultUnit string
		expected    *vttimepb.Duration
		shouldErr   bool
	}{
		{
			value:    "12s",
			expected: protoutil.DurationToProto(12 * time.Second),
		},
		{
			value:    "1h10m",
			expected: protoutil.DurationToProto(time.Hour + 10*time.Minute),
		},
		{
			name:        "no unit in value",
			value:       "120",
			defaultUnit: "s",
			expected:    protoutil.DurationToProto(120 * time.Second),
		},
		{
			name:     "empty",
			expected: nil,
		},
		{
			name:      "bad input",
			value:     "abcd",
			shouldErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out, err := valueToVTDuration(test.value, test.defaultUnit)
			if test.shouldErr {
				assert.Error(t, err, "expected parse error")
				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, test.expected, out, "failed to convert %s into vttime duration", test.value)
		})
	}
}

func TestAlterSchemaMigrationQuery(t *testing.T) {
	t.Parallel()

	uuid := "4e5dcf80_354b_11eb_82cd_f875a4d24e90"

	tcases := []struct {
		command string
		uuid    string
		expect  string
	}{
		{
			command: "cleanup",
			uuid:    uuid,
			expect:  "alter vitess_migration '4e5dcf80_354b_11eb_82cd_f875a4d24e90' cleanup",
		},
		{
			command: "cancel",
			uuid:    uuid,
			expect:  "alter vitess_migration '4e5dcf80_354b_11eb_82cd_f875a4d24e90' cancel",
		},
		{
			command: "cancel",
			uuid:    "all",
			expect:  "alter vitess_migration cancel all",
		},
		{
			command: "cancel",
			uuid:    "ALL",
			expect:  "alter vitess_migration cancel all",
		},
	}
	for _, tcase := range tcases {
		testName := fmt.Sprintf("%s %s", tcase.command, tcase.uuid)
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			query, err := alterSchemaMigrationQuery(tcase.command, tcase.uuid)
			assert.NoError(t, err)
			assert.Equal(t, tcase.expect, query)
		})
	}
}
