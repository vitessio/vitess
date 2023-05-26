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
	QTable *QueryTable
	VTable *vindexes.Table
	AST    *sqlparser.Insert

	// VindexValues specifies values for all the vindex columns.
	// This is a three-dimensional data structure:
	// Insert.Values[i] represents the values to be inserted for the i'th colvindex (i < len(Insert.Table.ColumnVindexes))
	// Insert.Values[i].Values[j] represents values for the j'th column of the given colVindex (j < len(colVindex[i].Columns)
	// Insert.Values[i].Values[j].Values[k] represents the value pulled from row k for that column: (k < len(ins.rows))
	VindexValues [][][]evalengine.Expr

	// ColVindexes are the vindexes that will use the VindexValues
	ColVindexes []*vindexes.ColumnVindex

	AutoIncrement Generate
	Ignore        bool

	noInputs
	noColumns
	noPredicates
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
	return &Insert{
		QTable:        i.QTable,
		VTable:        i.VTable,
		AST:           i.AST,
		VindexValues:  i.VindexValues,
		ColVindexes:   i.ColVindexes,
		AutoIncrement: i.AutoIncrement,
		Ignore:        i.Ignore,
	}
}

func (i *Insert) TablesUsed() []string {
	if i.VTable != nil {
		return SingleQualifiedIdentifier(i.VTable.Keyspace, i.VTable.Name)
	}
	return nil
}
