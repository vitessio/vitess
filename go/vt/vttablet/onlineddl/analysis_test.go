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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

var (
	tableWithPartitions = `
    CREATE TABLE tp (
        id INT NOT NULL,
        ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        primary key (id)
    )
    PARTITION BY RANGE (id) (
        PARTITION p1 VALUES LESS THAN (10),
        PARTITION p2 VALUES LESS THAN (20),
        PARTITION p3 VALUES LESS THAN (30),
        PARTITION p4 VALUES LESS THAN (40),
        PARTITION p5 VALUES LESS THAN (50),
        PARTITION p6 VALUES LESS THAN (60)
    )
`
)

func TestHasPartitionDefinition(t *testing.T) {
	tt := []struct {
		create    string
		partition string
		has       bool
	}{
		{
			create:    tableWithPartitions,
			partition: "PARTITION p1 VALUES LESS THAN (10)",
			has:       false,
		},
		{
			create:    tableWithPartitions,
			partition: "PARTITION `p1` VALUES LESS THAN (10)",
			has:       true,
		},
		{
			create:    tableWithPartitions,
			partition: "PARTITION `p6` VALUES LESS THAN (60)",
			has:       true,
		},
		{
			create:    tableWithPartitions,
			partition: "PARTITION `p6` VALUES LESS THAN (61)",
			has:       false,
		},
		{
			create:    tableWithPartitions,
			partition: "PARTITION `p7` VALUES LESS THAN (70)",
			has:       false,
		},
	}
	for _, tc := range tt {
		t.Run(tc.partition, func(t *testing.T) {
			stmt, err := sqlparser.Parse(tc.create)
			require.NoError(t, err)
			createTable, ok := stmt.(*sqlparser.CreateTable)
			require.True(t, ok)
			has := hasPartitionDefinition(createTable, tc.partition)
			assert.Equal(t, tc.has, has)
		})
	}
}
