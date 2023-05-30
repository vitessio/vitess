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

type Insert struct {
	VTable *vindexes.Table
	AST    *sqlparser.Insert

	AutoIncrement *Generate
	Ignore        bool

	// ColVindexes are the vindexes that will use the VindexValues
	ColVindexes []*vindexes.ColumnVindex

	// VindexValues specifies values for all the vindex columns.
	// This is a three-dimensional data structure:
	// Insert.Values[i] represents the values to be inserted for the i'th colvindex (i < len(Insert.Table.ColumnVindexes))
	// Insert.Values[i].Values[j] represents values for the j'th column of the given colVindex (j < len(colVindex[i].Columns)
	// Insert.Values[i].Values[j].Values[k] represents the value pulled from row k for that column: (k < len(ins.rows))
	VindexValues [][][]evalengine.Expr

	// VindexValueOffset stores the offset for each column in the ColumnVindex
	// that will appear in the result set of the select query.
	VindexValueOffset [][]int

	// Insert using select query will have select plan as input
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

type Generate struct {
	Keyspace  *vindexes.Keyspace
	TableName sqlparser.TableName

	// Values are the supplied values for the column, which
	// will be stored as a list within the expression. New
	// values will be generated based on how many were not
	// supplied (NULL).
	Values evalengine.Expr

	// Insert using Select, offset for auto increment column
	Offset int

	// The auto incremeent column was already present in the insert column list or was added.
	added bool
}

func (i *Insert) Description() ops.OpDescription {
	//TODO implement me
	panic("implement me")
}

func (i *Insert) ShortDescription() string {
	//TODO implement me
	panic("implement me")
}

func (i *Insert) GetOrdering() ([]ops.OrderBy, error) {
	//TODO implement me
	panic("implement me")
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
		ColVindexes:       i.ColVindexes,
		VindexValues:      i.VindexValues,
		VindexValueOffset: i.VindexValueOffset,
	}
}

func (i *Insert) TablesUsed() []string {
	if i.VTable != nil {
		return SingleQualifiedIdentifier(i.VTable.Keyspace, i.VTable.Name)
	}
	return nil
}
