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
	"vitess.io/vitess/go/vt/sqlparser"
)

// AlterTableRotatesRangePartition answers `true` when the given ALTER TABLE statement performs any sort
// of range partition rotation, that is applicable immediately and without moving data.
// Such would be:
// - Dropping any partition(s)
// - Adding a new partition (empty, at the end of the list)
func AlterTableRotatesRangePartition(createTable *sqlparser.CreateTable, alterTable *sqlparser.AlterTable) (bool, error) {
	// Validate original table is partitioned by RANGE
	if createTable.TableSpec.PartitionOption == nil {
		return false, nil
	}
	if createTable.TableSpec.PartitionOption.Type != sqlparser.RangeType {
		return false, nil
	}

	spec := alterTable.PartitionSpec
	if spec == nil {
		return false, nil
	}
	errorResult := func(conflictingNode sqlparser.SQLNode) error {
		return &PartitionSpecNonExclusiveError{
			Table:                alterTable.Table.Name.String(),
			PartitionSpec:        spec,
			ConflictingStatement: sqlparser.CanonicalString(conflictingNode),
		}
	}
	if len(alterTable.AlterOptions) > 0 {
		// This should never happen, unless someone programmatically tampered with the AlterTable AST.
		return false, errorResult(alterTable.AlterOptions[0])
	}
	if alterTable.PartitionOption != nil {
		// This should never happen, unless someone programmatically tampered with the AlterTable AST.
		return false, errorResult(alterTable.PartitionOption)
	}
	switch spec.Action {
	case sqlparser.AddAction:
		if len(spec.Definitions) > 1 {
			// This should never happen, unless someone programmatically tampered with the AlterTable AST.
			return false, errorResult(spec.Definitions[1])
		}
		return true, nil
	case sqlparser.DropAction:
		return true, nil
	default:
		return false, nil
	}
}
