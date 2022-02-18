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

package schemadiff

import (
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

var charsetTypes = map[string]bool{
	"CHAR":       true,
	"VARCHAR":    true,
	"TEXT":       true,
	"TINYTEXT":   true,
	"MEDIUMTEXT": true,
	"LONGTEXT":   true,
	"ENUM":       true,
	"SET":        true,
	"JSON":       true,
}

func getColName(colIdent *sqlparser.ColIdent) *sqlparser.ColName {
	return &sqlparser.ColName{Name: *colIdent}
}

type ModifyColumnDiff struct {
	sqlparser.ModifyColumn
}

func NewModifyColumnDiff(modifyColumn *sqlparser.ModifyColumn) *ModifyColumnDiff {
	return &ModifyColumnDiff{ModifyColumn: *modifyColumn}
}

func NewModifyColumnDiffByDefinition(definition *sqlparser.ColumnDefinition) *ModifyColumnDiff {
	modifyColumn := &sqlparser.ModifyColumn{
		NewColDefinition: definition,
	}
	return NewModifyColumnDiff(modifyColumn)
}

type ColumnDefinitionEntity struct {
	sqlparser.ColumnDefinition
}

func NewColumnDefinitionEntity(c *sqlparser.ColumnDefinition) *ColumnDefinitionEntity {
	return &ColumnDefinitionEntity{ColumnDefinition: *c}
}

// Diff compares this table statement with another table statement, and sees what it takes to
// change this table to look like the other table.
// It returns an AlterTable statement if changes are found, or nil if not.
// the other table may be of different name; its name is ignored.
func (c *ColumnDefinitionEntity) ColumnDiff(other *ColumnDefinitionEntity, hints *DiffHints) *ModifyColumnDiff {
	format := sqlparser.String(&c.ColumnDefinition)
	otherFormat := sqlparser.String(&other.ColumnDefinition)
	if format == otherFormat {
		return nil
	}

	modifyColumn := &sqlparser.ModifyColumn{
		NewColDefinition: &other.ColumnDefinition,
	}
	return &ModifyColumnDiff{ModifyColumn: *modifyColumn}
}

// IsTextual
func (c *ColumnDefinitionEntity) IsTextual() bool {
	return charsetTypes[strings.ToUpper(c.Type.Type)]
}
