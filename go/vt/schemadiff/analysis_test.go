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

package schemadiff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

// AnalyzePartitionRotation analyzes a given AlterTable statement to see whether it has partition rotation
// commands, and if so, is the ALTER TABLE statement valid in MySQL. In MySQL, a single ALTER TABLE statement
// cannot apply multiple rotation commands, nor can it mix rotation commands with other types of changes.
func TestAlterTableRotatesRangePartition(t *testing.T) {
	tcases := []struct {
		create string
		alter  string
		expect bool
	}{
		{
			alter:  "ALTER TABLE t ADD PARTITION (PARTITION p1 VALUES LESS THAN (10))",
			expect: true,
		},
		{
			alter:  "ALTER TABLE t DROP PARTITION p1",
			expect: true,
		},
		{
			alter:  "ALTER TABLE t DROP PARTITION p1, p2",
			expect: true,
		},
		{
			alter: "ALTER TABLE t TRUNCATE PARTITION p3",
		},
		{
			alter: "ALTER TABLE t COALESCE PARTITION 3",
		},
		{
			alter: "ALTER TABLE t partition by range (id) (partition p1 values less than (10), partition p2 values less than (20), partition p3 values less than (30))",
		},
		{
			alter: "ALTER TABLE t ADD COLUMN c1 INT, DROP COLUMN c2",
		},
	}

	for _, tcase := range tcases {
		t.Run(tcase.alter, func(t *testing.T) {
			if tcase.create == "" {
				tcase.create = "CREATE TABLE t (id int PRIMARY KEY) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10))"
			}
			stmt, err := sqlparser.NewTestParser().ParseStrictDDL(tcase.create)
			require.NoError(t, err)
			createTable, ok := stmt.(*sqlparser.CreateTable)
			require.True(t, ok)

			stmt, err = sqlparser.NewTestParser().ParseStrictDDL(tcase.alter)
			require.NoError(t, err)
			alterTable, ok := stmt.(*sqlparser.AlterTable)
			require.True(t, ok)

			result, err := AlterTableRotatesRangePartition(createTable, alterTable)
			require.NoError(t, err)
			assert.Equal(t, tcase.expect, result)
		})
	}
}
