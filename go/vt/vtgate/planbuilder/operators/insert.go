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

package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// Insert represents an insert operation on a table.
type Insert struct {
	// VTable represents the target table for the insert operation.
	VTable *vindexes.Table
	// AST represents the insert statement from the SQL syntax.
	AST *sqlparser.Insert

	// AutoIncrement represents the auto-increment generator for the insert operation.
	AutoIncrement *Generate
	// Ignore specifies whether to ignore duplicate key errors during insertion.
	Ignore bool
	// ForceNonStreaming when true, select first then insert, this is to avoid locking rows by select for insert.
	ForceNonStreaming bool

	// ColVindexes are the vindexes that will use the VindexValues or VindexValueOffset
	ColVindexes []*vindexes.ColumnVindex

	// VindexValues specifies values for all the vindex columns.
	VindexValues [][][]evalengine.Expr

	// VindexValueOffset stores the offset for each column in the ColumnVindex
	// that will appear in the result set of the select query.
	VindexValueOffset [][]int

	// Insert using select query will have select plan as input operator for the insert operation.
	Input ops.Operator

	noColumns
	noPredicates
}

func (i *Insert) Inputs() []ops.Operator {
	if i.Input == nil {
		return nil
	}
	return []ops.Operator{i.Input}
}

func (i *Insert) SetInputs(inputs []ops.Operator) {
	if len(inputs) > 0 {
		i.Input = inputs[0]
	}
}

// Generate represents an auto-increment generator for the insert operation.
type Generate struct {
	// Keyspace represents the keyspace information for the table.
	Keyspace *vindexes.Keyspace
	// TableName represents the name of the table.
	TableName sqlparser.TableName

	// Values are the supplied values for the column, which
	// will be stored as a list within the expression. New
	// values will be generated based on how many were not
	// supplied (NULL).
	Values evalengine.Expr
	// Insert using Select, offset for auto increment column
	Offset int

	// added indicates whether the auto-increment column was already present in the insert column list or added.
	added bool
}

func (i *Insert) ShortDescription() string {
	return i.VTable.String()
}

func (i *Insert) GetOrdering() ([]ops.OrderBy, error) {
	panic("does not expect insert operator to receive get ordering call")
}

var _ ops.Operator = (*Insert)(nil)

func (i *Insert) Clone(inputs []ops.Operator) ops.Operator {
	var input ops.Operator
	if len(inputs) > 0 {
		input = inputs[0]
	}
	return &Insert{
		Input:             input,
		VTable:            i.VTable,
		AST:               i.AST,
		AutoIncrement:     i.AutoIncrement,
		Ignore:            i.Ignore,
		ForceNonStreaming: i.ForceNonStreaming,
		ColVindexes:       i.ColVindexes,
		VindexValues:      i.VindexValues,
		VindexValueOffset: i.VindexValueOffset,
	}
}

func (i *Insert) TablesUsed() []string {
	return SingleQualifiedIdentifier(i.VTable.Keyspace, i.VTable.Name)
}
