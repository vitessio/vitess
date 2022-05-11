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

package onlineddl

import (
	"context"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type specialAlterOperation string

const (
	dropFirstPartitionSpecialOperation specialAlterOperation = "drop-first-partition"
	dropLastPartitionSpecialOperation  specialAlterOperation = "drop-last-partition"
	addPartitionSpecialOperation       specialAlterOperation = "add-partition"
)

type SpecialAlterPlan struct {
	operation specialAlterOperation
	info      string
}

func NewSpecialAlterOperation(operation specialAlterOperation, info string) *SpecialAlterPlan {
	return &SpecialAlterPlan{
		operation: operation,
		info:      info,
	}
}

// getCreateTableStatement gets a formal AlterTable representation of the given table
func (e *Executor) getCreateTableStatement(ctx context.Context, tableName string) (*sqlparser.CreateTable, error) {
	showCreateTable, err := e.showCreateTable(ctx, tableName)
	if err != nil {
		return nil, err
	}
	stmt, err := sqlparser.ParseStrictDDL(showCreateTable)
	if err != nil {
		return nil, err
	}
	createTable, ok := stmt.(*sqlparser.CreateTable)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected CREATE TABLE. Got %v", sqlparser.String(stmt))
	}
	return createTable, nil
}

// analyzeDropFirstOrLastRangePartition sees if the online DDL drops the first or last partition
//  in a range partitioned table
func (e *Executor) analyzeDropFirstOrLastRangePartition(alterTable *sqlparser.AlterTable, createTable *sqlparser.CreateTable) *SpecialAlterPlan {
	// we are looking for a `ALTER TABLE <table> DROP PARTITION <name>` statement with nothing else
	if len(alterTable.AlterOptions) > 0 {
		return nil
	}
	if alterTable.PartitionOption != nil {
		return nil
	}
	spec := alterTable.PartitionSpec
	if spec == nil {
		return nil
	}
	if spec.Action != sqlparser.DropAction {
		return nil
	}
	if len(spec.Names) != 1 {
		return nil
	}
	partitionName := spec.Names[0].String()
	// OK then!

	// Now, is this query dropping the first parititon in a RANGE partitioned table?
	part := createTable.TableSpec.PartitionOption
	if part.Type != sqlparser.RangeType {
		return nil
	}
	if len(part.Definitions) == 0 {
		return nil
	}
	// See if this is either a first partition or a last partition.
	// If there's only one partition, we consider it as first.
	firstPartitionName := part.Definitions[0].Name.String()
	if partitionName == firstPartitionName {
		// O-K! Dropping the first partition!
		return NewSpecialAlterOperation(dropFirstPartitionSpecialOperation, partitionName)
	}
	lastPartitionName := part.Definitions[len(part.Definitions)-1].Name.String()
	if partitionName == lastPartitionName {
		// O-K! Dropping the last partition!
		return NewSpecialAlterOperation(dropLastPartitionSpecialOperation, partitionName)
	}
	return nil
}

// analyzeDropFirstOrLastRangePartition sees if the online DDL drops the first or last partition
//  in a range partitioned table
func (e *Executor) analyzeAddRangePartition(alterTable *sqlparser.AlterTable, createTable *sqlparser.CreateTable) *SpecialAlterPlan {
	// we are looking for a `ALTER TABLE <table> ADD PARTITION (PARTITION ...)` statement with nothing else
	if len(alterTable.AlterOptions) > 0 {
		return nil
	}
	if alterTable.PartitionOption != nil {
		return nil
	}
	spec := alterTable.PartitionSpec
	if spec == nil {
		return nil
	}
	if spec.Action != sqlparser.AddAction {
		return nil
	}
	if len(spec.Definitions) != 1 {
		return nil
	}
	partitionName := spec.Definitions[0].Name.String()
	// OK then!

	// Now, is this query adding a parititon in a RANGE partitioned table?
	part := createTable.TableSpec.PartitionOption
	if part.Type != sqlparser.RangeType {
		return nil
	}
	if len(part.Definitions) == 0 {
		return nil
	}
	return NewSpecialAlterOperation(addPartitionSpecialOperation, partitionName)
}

// analyzeSpecialAlterScenarios checks if the given ALTER onlineDDL, and for the current state of affected table,
// can be executed in a special way
func (e *Executor) analyzeSpecialAlterPlan(ctx context.Context, onlineDDL *schema.OnlineDDL) (*SpecialAlterPlan, error) {
	ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL)
	if err != nil {
		return nil, err
	}
	alterTable, ok := ddlStmt.(*sqlparser.AlterTable)
	if !ok {
		// We only deal here with ALTER TABLE
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected ALTER TABLE. Got %v", sqlparser.String(ddlStmt))
	}

	createTable, err := e.getCreateTableStatement(ctx, onlineDDL.Table)
	if err != nil {
		return nil, err
	}

	if op := e.analyzeDropFirstOrLastRangePartition(alterTable, createTable); op != nil {
		return op, nil
	}
	return nil, nil
}
