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

package sqlparser

import "fmt"

// this file defines two structs (Col and TableT) which represent the schema of the database used
// currently used in random expression generation and random query generation in endtoend testing

type (
	Col struct {
		TableName string
		Name      string
		Typ       string
	}
	TableT struct {
		// Name will be a TableName object if it is used, with Name: alias or name if no alias is provided
		// Name will only be a DerivedTable for moving its data around
		Name SimpleTableExpr
		Cols []Col
	}
)

// GetColumnName returns TableName.Name
func (c *Col) GetColumnName() string {
	return fmt.Sprintf("%s.%s", c.TableName, c.Name)
}

// SetName sets the alias for t, as well as setting the TableName for all columns in Cols
func (t *TableT) SetName(newName string) {
	t.Name = NewTableName(newName)
	for i := range t.Cols {
		t.Cols[i].TableName = newName
	}
}

// SetColumns sets the columns of t, and automatically assigns TableName
// this makes it unnatural (but still possible as Cols is exportable) to modify TableName
func (t *TableT) SetColumns(col ...Col) {
	t.Cols = make([]Col, len(col))
	t.AddColumns(col...)
}

// AddColumns adds columns to t, and automatically assigns TableName
// this makes it unnatural (but still possible as Cols is exportable) to modify TableName
func (t *TableT) AddColumns(col ...Col) {
	for i := range col {
		// only change the Col's TableName if t is of type TableName
		if tName, ok := t.Name.(TableName); ok {
			col[i].TableName = tName.Name.String()
		}

		t.Cols = append(t.Cols, col[i])
	}
}

// copy returns a deep copy of t
func (t *TableT) copy() *TableT {
	newCols := make([]Col, len(t.Cols))
	copy(newCols, t.Cols)
	return &TableT{
		Name: t.Name,
		Cols: newCols,
	}
}
