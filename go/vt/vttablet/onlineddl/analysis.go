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

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type specialAlterOperation string

const (
	dropRangePartitionSpecialOperation specialAlterOperation = "drop-range-partition"
	addRangePartitionSpecialOperation  specialAlterOperation = "add-range-partition"
)

type SpecialAlterPlan struct {
	operation           specialAlterOperation
	details             map[string]string
	changesFoundInTable bool
	alterTable          *sqlparser.AlterTable
	createTable         *sqlparser.CreateTable
}

func NewSpecialAlterPlan(operation specialAlterOperation, alterTable *sqlparser.AlterTable, createTable *sqlparser.CreateTable) *SpecialAlterPlan {
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

func (p *SpecialAlterPlan) AddDetails(details map[string]string) *SpecialAlterPlan {
	for key, val := range details {
		p.details[key] = val
	}
	return p
}

func (p *SpecialAlterPlan) Detail(key string) string {
	return p.details[key]
}

func (p *SpecialAlterPlan) SetNewArtifact(key string) (artifactTableName string, err error) {
	artifactTableName, err = schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
	if err == nil {
		p.SetDetail(key, artifactTableName)
	}
	return artifactTableName, err
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
		return nil, err
	}
	stmt, err := sqlparser.ParseStrictDDL(showCreateTable)
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
func (e *Executor) analyzeDropRangePartition(alterTable *sqlparser.AlterTable, createTable *sqlparser.CreateTable) (*SpecialAlterPlan, error) {
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
	part := createTable.TableSpec.PartitionOption
	if part.Type != sqlparser.RangeType {
		return nil, nil
	}
	if len(part.Definitions) == 0 {
		return nil, nil
	}
	var partitionDefinition *sqlparser.PartitionDefinition
	var nextPartitionDefinition *sqlparser.PartitionDefinition
	for i, p := range part.Definitions {
		if p.Name.String() == partitionName {
			partitionDefinition = p
			if i+1 < len(part.Definitions) {
				nextPartitionDefinition = part.Definitions[i+1]
			}
			break
		}
	}
	if partitionDefinition == nil {
		// dropping a nonexistent partition. We'll let the "standard" migration execution flow deal with that.
		return nil, nil
	}
	artifactTableName, err := schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
	if err != nil {
		return nil, err
	}

	plan := NewSpecialAlterPlan(dropRangePartitionSpecialOperation, alterTable, createTable)
	plan.SetDetail("artifact", artifactTableName)
	plan.SetDetail("partition_name", partitionName)
	plan.SetDetail("partition_definition", sqlparser.CanonicalString(partitionDefinition))
	if nextPartitionDefinition != nil {
		plan.SetDetail("next_partition_name", nextPartitionDefinition.Name.String())
		plan.SetDetail("next_partition_definition", sqlparser.CanonicalString(nextPartitionDefinition))
		artifactTableName, err := schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
		if err != nil {
			return nil, err
		}
		plan.SetDetail("next_partition_artifact", artifactTableName)
	}
	return plan, nil
}

// analyzeAddRangePartition sees if the online DDL adds a partition in a range partitioned table
func (e *Executor) analyzeAddRangePartition(alterTable *sqlparser.AlterTable, createTable *sqlparser.CreateTable) (*SpecialAlterPlan, error) {
	// we are looking for a `ALTER TABLE <table> ADD PARTITION (PARTITION ...)` statement with nothing else
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
	if spec.Action != sqlparser.AddAction {
		return nil, nil
	}
	if len(spec.Definitions) != 1 {
		return nil, nil
	}
	partitionDefinition := spec.Definitions[0]
	partitionName := partitionDefinition.Name.String()
	// OK then!

	// Now, is this query adding a partition in a RANGE partitioned table?
	part := createTable.TableSpec.PartitionOption
	if part.Type != sqlparser.RangeType {
		return nil, nil
	}
	if len(part.Definitions) == 0 {
		return nil, nil
	}
	plan := NewSpecialAlterPlan(addRangePartitionSpecialOperation, alterTable, createTable)
	plan.SetDetail("partition_name", partitionName)
	plan.SetDetail("partition_definition", sqlparser.CanonicalString(partitionDefinition))
	return plan, nil
}

// analyzeSpecialAlterPlan checks if the given ALTER onlineDDL, and for the current state of affected table,
// can be executed in a special way. If so, it returns with a "special plan"
func (e *Executor) analyzeSpecialAlterPlan(ctx context.Context, onlineDDL *schema.OnlineDDL) (*SpecialAlterPlan, error) {
	ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL)
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
		return nil, err
	}

	// special plans which support reverts are trivially desired:
	{
		op, err := e.analyzeDropRangePartition(alterTable, createTable)
		if err != nil {
			return nil, err
		}
		if op != nil {
			return op, nil
		}
	}
	{
		op, err := e.analyzeAddRangePartition(alterTable, createTable)
		if err != nil {
			return nil, err
		}
		if op != nil {
			return op, nil
		}
	}
	// special plans which do not support reverts are flag protected:
	if onlineDDL.StrategySetting().IsFastOverRevertibleFlag() {
	}
	return nil, nil
}

// analyzeSpecialRevertAlterPlan checks if the given migration can be reverted using a special plan.
// This can happen if the migration itself was executed with a special plan.
func (e *Executor) analyzeSpecialRevertAlterPlan(ctx context.Context, revertOnlineDDL *schema.OnlineDDL, revertPlanDetails string) (*SpecialAlterPlan, error) {
	if revertPlanDetails == "" {
		return nil, nil
	}
	var details map[string]string
	err := json.Unmarshal([]byte(revertPlanDetails), &details)
	if err != nil {
		return nil, err
	}
	specialOperation := specialAlterOperation(details["operation"])

	switch specialOperation {
	case addRangePartitionSpecialOperation:
		artifactTableName, err := schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
		if err != nil {
			return nil, err
		}
		plan := NewSpecialAlterPlan(dropRangePartitionSpecialOperation, nil, nil)
		plan.SetDetail("partition_name", details["partition_name"])
		plan.SetDetail("partition_definition", details["partition_definition"])
		plan.SetDetail("artifact", artifactTableName)

		nextPartitionName := details["next_partition_name"]
		if nextPartitionName != "" {
			plan.SetDetail("next_partition_name", nextPartitionName)
			plan.SetDetail("next_partition_definition", details["next_partition_definition"])
			artifactTableName, err := schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
			if err != nil {
				return nil, err
			}
			plan.SetDetail("next_partition_artifact", artifactTableName)
		}

		return plan, nil
	case dropRangePartitionSpecialOperation:
		createTable, err := e.getCreateTableStatement(ctx, revertOnlineDDL.Table)
		if err != nil {
			return nil, err
		}
		plan := NewSpecialAlterPlan(addRangePartitionSpecialOperation, nil, nil)
		plan.SetDetail("artifact", details["artifact"])
		plan.SetDetail("partition_name", details["partition_name"])
		partitionDefinition := details["partition_definition"]
		plan.SetDetail("partition_definition", partitionDefinition)
		plan.changesFoundInTable = hasPartitionDefinition(createTable, partitionDefinition)
		plan.SetDetail("next_partition_name", details["next_partition_name"])
		plan.SetDetail("next_partition_definition", details["next_partition_definition"])
		plan.SetDetail("next_partition_artifact", details["next_partition_artifact"])
		// nextPartitionName := details["next_partition_name"]
		// if nextPartitionName != "" {
		// 	plan.SetDetail("next_partition_name", nextPartitionName)
		// 	plan.SetDetail("next_partition_definition", details["next_partition_definition"])
		// 	artifactTableName, err := schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
		// 	if err != nil {
		// 		return nil, err
		// 	}
		// 	plan.SetDetail("next_partition_artifact", artifactTableName)
		// }
		return plan, nil
	default:
		return nil, nil
	}
}

func hasPartitionDefinition(createTable *sqlparser.CreateTable, canonicalPartitionDefinition string) bool {
	if createTable == nil {
		return false
	}
	if createTable.TableSpec == nil {
		return false
	}
	partitionOption := createTable.GetTableSpec().PartitionOption
	if partitionOption == nil {
		return false
	}
	for _, p := range partitionOption.Definitions {
		canonicalDefinition := sqlparser.CanonicalString(p)
		if canonicalPartitionDefinition == canonicalDefinition {
			return true
		}
	}
	return false
}
