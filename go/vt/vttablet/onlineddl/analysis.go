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
	"strings"

	"vitess.io/vitess/go/mysql"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/schema"
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

// alterOptionAvailableViaInstantDDL chcks if the specific alter option is eligible to run via ALGORITHM=INSTANT
// reference: https://dev.mysql.com/doc/refman/8.0/en/innodb-online-ddl-operations.html
func alterOptionAvailableViaInstantDDL(alterOption sqlparser.AlterOption, createTable *sqlparser.CreateTable, capableOf mysql.CapableOf) (bool, error) {
	findColumn := func(colName string) *sqlparser.ColumnDefinition {
		if createTable == nil {
			return nil
		}
		for _, col := range createTable.TableSpec.Columns {
			if strings.EqualFold(colName, col.Name.String()) {
				return col
			}
		}
		return nil
	}
	findTableOption := func(optName string) *sqlparser.TableOption {
		if createTable == nil {
			return nil
		}
		for _, opt := range createTable.TableSpec.Options {
			if strings.EqualFold(optName, opt.Name) {
				return opt
			}
		}
		return nil
	}
	isVirtualColumn := func(colName string) bool {
		col := findColumn(colName)
		if col == nil {
			return false
		}
		if col.Type.Options == nil {
			return false
		}
		if col.Type.Options.As == nil {
			return false
		}
		return col.Type.Options.Storage == sqlparser.VirtualStorage
	}
	colStringStrippedDown := func(col *sqlparser.ColumnDefinition, stripDefault bool, stripEnum bool) string {
		strippedCol := sqlparser.CloneRefOfColumnDefinition(col)
		if stripDefault {
			strippedCol.Type.Options.Default = nil
		}
		if stripEnum {
			strippedCol.Type.EnumValues = nil
		}
		return sqlparser.CanonicalString(strippedCol)
	}
	hasPrefix := func(vals []string, prefix []string) bool {
		if len(vals) < len(prefix) {
			return false
		}
		for i := range prefix {
			if vals[i] != prefix[i] {
				return false
			}
		}
		return true
	}
	// Up to 8.0.26 we could only ADD COLUMN as last column
	switch opt := alterOption.(type) {
	case *sqlparser.ChangeColumn:
		// We do not support INSTANT for renaming a column (ALTER TABLE ...CHANGE) because:
		// 1. We discourage column rename
		// 2. We do not produce CHANGE statements in declarative diff
		// 3. The success of the operation depends on whether the column is referenced by a foreign key
		//    in another table. Which is a bit too much to compute here.
		return false, nil
	case *sqlparser.AddColumns:
		if opt.First || opt.After != nil {
			// not a "last" column. Only supported as of 8.0.29
			return capableOf(mysql.InstantAddDropColumnFlavorCapability)
		}
		// Adding a *last* column is supported in 8.0
		return capableOf(mysql.InstantAddLastColumnFlavorCapability)
	case *sqlparser.DropColumn:
		// not supported in COMPRESSED tables
		if opt := findTableOption("ROW_FORMAT"); opt != nil {
			if strings.EqualFold(opt.String, "COMPRESSED") {
				return false, nil
			}
		}
		if isVirtualColumn(opt.Name.Name.String()) {
			// supported by all 8.0 versions
			return capableOf(mysql.InstantAddDropVirtualColumnFlavorCapability)
		}
		return capableOf(mysql.InstantAddDropColumnFlavorCapability)
	case *sqlparser.ModifyColumn:
		if col := findColumn(opt.NewColDefinition.Name.String()); col != nil {
			// Check if only diff is change of default
			// we temporarily remove the DEFAULT expression (if any) from both
			// table and ALTER statement, and compare the columns: if they're otherwise equal,
			// then the only change can be an addition/change/removal of DEFAULT, which
			// is instant-table.
			tableColDefinition := colStringStrippedDown(col, true, false)
			newColDefinition := colStringStrippedDown(opt.NewColDefinition, true, false)
			if tableColDefinition == newColDefinition {
				return capableOf(mysql.InstantChangeColumnDefaultFlavorCapability)
			}
			// Check if:
			// 1. this an ENUM/SET
			// 2. and the change is to append values to the end of the list
			// 3. and the number of added values does not increase the storage size for the enum/set
			// 4. while still not caring about a change in the default value
			if len(col.Type.EnumValues) > 0 && len(opt.NewColDefinition.Type.EnumValues) > 0 {
				// both are enum or set
				if !hasPrefix(opt.NewColDefinition.Type.EnumValues, col.Type.EnumValues) {
					return false, nil
				}
				// we know the new column definition is identical to, or extends, the old definition.
				// Now validate storage:
				if strings.EqualFold(col.Type.Type, "enum") {
					if len(col.Type.EnumValues) <= 255 && len(opt.NewColDefinition.Type.EnumValues) > 255 {
						// this increases the SET storage size (1 byte for up to 8 values, 2 bytes beyond)
						return false, nil
					}
				}
				if strings.EqualFold(col.Type.Type, "set") {
					if (len(col.Type.EnumValues)+7)/8 != (len(opt.NewColDefinition.Type.EnumValues)+7)/8 {
						// this increases the SET storage size (1 byte for up to 8 values, 2 bytes for 8-15, etc.)
						return false, nil
					}
				}
				// Now don't care about change of default:
				tableColDefinition := colStringStrippedDown(col, true, true)
				newColDefinition := colStringStrippedDown(opt.NewColDefinition, true, true)
				if tableColDefinition == newColDefinition {
					return capableOf(mysql.InstantExpandEnumCapability)
				}
			}
		}
		return false, nil
	default:
		return false, nil
	}
}

// AnalyzeInstantDDL takes declarative CreateTable and AlterTable, as well as a server version, and checks whether it is possible to run the ALTER
// using ALGORITM=INSTANT for that version.
// This function is INTENTIONALLY public, even though we do not guarantee that it will remain so.
func AnalyzeInstantDDL(alterTable *sqlparser.AlterTable, createTable *sqlparser.CreateTable, capableOf mysql.CapableOf) (*SpecialAlterPlan, error) {
	capable, err := capableOf(mysql.InstantDDLFlavorCapability)
	if err != nil {
		return nil, err
	}
	if !capable {
		return nil, nil
	}
	if alterTable.PartitionOption != nil {
		// no INSTANT for partitions
		return nil, nil
	}
	if alterTable.PartitionSpec != nil {
		// no INSTANT for partitions
		return nil, nil
	}
	// For the ALTER statement to qualify for ALGORITHM=INSTANT, all alter options must each qualify.
	for _, alterOption := range alterTable.AlterOptions {
		instantOK, err := alterOptionAvailableViaInstantDDL(alterOption, createTable, capableOf)
		if err != nil {
			return nil, err
		}
		if !instantOK {
			return nil, nil
		}
	}
	op := NewSpecialAlterOperation(instantDDLSpecialOperation, alterTable, createTable)
	return op, nil
}

// analyzeSpecialAlterPlan checks if the given ALTER onlineDDL, and for the current state of affected table,
// can be executed in a special way. If so, it returns with a "special plan"
func (e *Executor) analyzeSpecialAlterPlan(ctx context.Context, onlineDDL *schema.OnlineDDL, capableOf mysql.CapableOf) (*SpecialAlterPlan, error) {
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
		op, err := AnalyzeInstantDDL(alterTable, createTable, capableOf)
		if err != nil {
			return nil, err
		}
		if op != nil {
			return op, nil
		}
	}
	return nil, nil
}
