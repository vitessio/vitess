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
	instantDDLSpecialOperation     specialAlterOperation = "instant-ddl"
	rangePartitionSpecialOperation specialAlterOperation = "range-partition"
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
	//
	// - nothing here thus far
	//
	// special plans that do not support revert, but are always desired over Online DDL,
	// hence not flag protected:
	{
		// Dropping a range partition has to run directly. It is incorrect to run with Online DDL
		// because the table copy will make the second-oldest partition "adopt" the rows which
		// we really want purged from the oldest partition.
		// Adding a range partition _can_ technically run with Online DDL, but it is wasteful
		// and pointless. The user fully expects the operation to run immediately and without
		// any copy of data.
		isRangeRotation, err := schemadiff.AlterTableRotatesRangePartition(createTable, alterTable)
		if err != nil {
			return nil, err
		}
		if isRangeRotation {
			op := NewSpecialAlterOperation(rangePartitionSpecialOperation, alterTable, createTable)
			return op, nil
		}
	}
	// special plans which do not support reverts are flag protected:
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
