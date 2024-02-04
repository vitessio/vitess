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
	"encoding/json"

	"vitess.io/vitess/go/mysql/capabilities"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/schemadiff"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type specialAlterOperation string

const (
	instantDDLSpecialOperation         specialAlterOperation = "instant-ddl"
	dropRangePartitionSpecialOperation specialAlterOperation = "drop-range-partition"
	addRangePartitionSpecialOperation  specialAlterOperation = "add-range-partition"
)

type SpecialAlterPlan struct {
	operation   specialAlterOperation
	details     map[string]string
	alterTable  *sqlparser.AlterTable
	createTable *sqlparser.CreateTable
}

func NewSpecialAlterOperation(operation specialAlterOperation, alterTable *sqlparser.AlterTable, createTable *sqlparser.CreateTable) *SpecialAlterPlan {
	return &SpecialAlterPlan{
		operation:   operation,
		details:     map[string]string{"operation": string(operation)},
		alterTable:  alterTable,
		createTable: createTable,
	}
}

func (p *SpecialAlterPlan) SetDetail(key string, val string) *SpecialAlterPlan {
	p.details[key] = val
	return p
}

func (p *SpecialAlterPlan) Detail(key string) string {
	return p.details[key]
}

func (p *SpecialAlterPlan) String() string {
	b, err := json.Marshal(p.details)
	if err != nil {
		return ""
	}
	return string(b)
}

// getCreateTableStatement gets a formal AlterTable representation of the given table
func (e *Executor) getCreateTableStatement(ctx context.Context, tableName string) (*sqlparser.CreateTable, error) {
	showCreateTable, err := e.showCreateTable(ctx, tableName)
	if err != nil {
		return nil, vterrors.Wrapf(err, "in Executor.getCreateTableStatement()")
	}
	stmt, err := e.env.Environment().Parser().ParseStrictDDL(showCreateTable)
	if err != nil {
		return nil, err
	}
	createTable, ok := stmt.(*sqlparser.CreateTable)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected CREATE TABLE. Got %v", sqlparser.CanonicalString(stmt))
	}
	return createTable, nil
}

// analyzeDropRangePartition sees if the online DDL drops a single partition in a range partitioned table
func analyzeDropRangePartition(alterTable *sqlparser.AlterTable, createTable *sqlparser.CreateTable) (*SpecialAlterPlan, error) {
	// we are looking for a `ALTER TABLE <table> DROP PARTITION <name>` statement with nothing else
	if len(alterTable.AlterOptions) > 0 {
		return nil, nil
	}
	if alterTable.PartitionOption != nil {
		return nil, nil
	}
	spec := alterTable.PartitionSpec
	if spec == nil {
		return nil, nil
	}
	if spec.Action != sqlparser.DropAction {
		return nil, nil
	}
	if len(spec.Names) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vitess only supports dropping a single partition per query: %v", sqlparser.CanonicalString(alterTable))
	}
	partitionName := spec.Names[0].String()
	// OK then!

	// Now, is this query dropping the first partition in a RANGE partitioned table?
	part := createTable.TableSpec.PartitionOption
	if part.Type != sqlparser.RangeType {
		return nil, nil
	}
	if len(part.Definitions) == 0 {
		return nil, nil
	}
	var partitionDefinition *sqlparser.PartitionDefinition
	var nextPartitionName string
	for i, p := range part.Definitions {
		if p.Name.String() == partitionName {
			partitionDefinition = p
			if i+1 < len(part.Definitions) {
				nextPartitionName = part.Definitions[i+1].Name.String()
			}
			break
		}
	}
	if partitionDefinition == nil {
		// dropping a nonexistent partition. We'll let the "standard" migration execution flow deal with that.
		return nil, nil
	}
	op := NewSpecialAlterOperation(dropRangePartitionSpecialOperation, alterTable, createTable)
	op.SetDetail("partition_name", partitionName)
	op.SetDetail("partition_definition", sqlparser.CanonicalString(partitionDefinition))
	op.SetDetail("next_partition_name", nextPartitionName)
	return op, nil
}

// analyzeAddRangePartition sees if the online DDL adds a partition in a range partitioned table
func analyzeAddRangePartition(alterTable *sqlparser.AlterTable, createTable *sqlparser.CreateTable) *SpecialAlterPlan {
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
	partitionDefinition := spec.Definitions[0]
	partitionName := partitionDefinition.Name.String()
	// OK then!

	// Now, is this query adding a partition in a RANGE partitioned table?
	part := createTable.TableSpec.PartitionOption
	if part.Type != sqlparser.RangeType {
		return nil
	}
	if len(part.Definitions) == 0 {
		return nil
	}
	op := NewSpecialAlterOperation(addRangePartitionSpecialOperation, alterTable, createTable)
	op.SetDetail("partition_name", partitionName)
	op.SetDetail("partition_definition", sqlparser.CanonicalString(partitionDefinition))
	return op
}

// analyzeInstantDDL takes declarative CreateTable and AlterTable, as well as a server version, and checks whether it is possible to run the ALTER
// using ALGORITHM=INSTANT for that version.
func analyzeInstantDDL(alterTable *sqlparser.AlterTable, createTable *sqlparser.CreateTable, capableOf capabilities.CapableOf) (*SpecialAlterPlan, error) {
	capable, err := schemadiff.AlterTableCapableOfInstantDDL(alterTable, createTable, capableOf)
	if err != nil {
		return nil, err
	}
	if !capable {
		return nil, nil
	}
	op := NewSpecialAlterOperation(instantDDLSpecialOperation, alterTable, createTable)
	return op, nil
}

// analyzeSpecialAlterPlan checks if the given ALTER onlineDDL, and for the current state of affected table,
// can be executed in a special way. If so, it returns with a "special plan"
func (e *Executor) analyzeSpecialAlterPlan(ctx context.Context, onlineDDL *schema.OnlineDDL, capableOf capabilities.CapableOf) (*SpecialAlterPlan, error) {
	ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL, e.env.Environment().Parser())
	if err != nil {
		return nil, err
	}
	alterTable, ok := ddlStmt.(*sqlparser.AlterTable)
	if !ok {
		// We only deal here with ALTER TABLE
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected ALTER TABLE. Got %v", sqlparser.CanonicalString(ddlStmt))
	}

	createTable, err := e.getCreateTableStatement(ctx, onlineDDL.Table)
	if err != nil {
		return nil, vterrors.Wrapf(err, "in Executor.analyzeSpecialAlterPlan(), uuid=%v, table=%v", onlineDDL.UUID, onlineDDL.Table)
	}

	// special plans which support reverts are trivially desired:
	// special plans which do not support reverts are flag protected:
	if onlineDDL.StrategySetting().IsFastRangeRotationFlag() {
		op, err := analyzeDropRangePartition(alterTable, createTable)
		if err != nil {
			return nil, err
		}
		if op != nil {
			return op, nil
		}
		if op := analyzeAddRangePartition(alterTable, createTable); op != nil {
			return op, nil
		}
	}
	if onlineDDL.StrategySetting().IsPreferInstantDDL() {
		op, err := analyzeInstantDDL(alterTable, createTable, capableOf)
		if err != nil {
			return nil, err
		}
		if op != nil {
			return op, nil
		}
	}
	return nil, nil
}
