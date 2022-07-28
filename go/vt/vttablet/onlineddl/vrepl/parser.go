/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/
/*
Copyright 2021 The Vitess Authors.

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

package vrepl

import (
	"strings"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

// AlterTableParser is a parser tool for ALTER TABLE statements
// This is imported from gh-ost. In the future, we should replace that with Vitess parsing.
type AlterTableParser struct {
	columnRenameMap        map[string]string
	droppedColumns         map[string]bool
	isRenameTable          bool
	isAutoIncrementDefined bool
}

// NewAlterTableParser creates a new parser
func NewAlterTableParser() *AlterTableParser {
	return &AlterTableParser{
		columnRenameMap: make(map[string]string),
		droppedColumns:  make(map[string]bool),
	}
}

// NewParserFromAlterStatement creates a new parser with a ALTER TABLE statement
func NewParserFromAlterStatement(alterTable *sqlparser.AlterTable) *AlterTableParser {
	parser := NewAlterTableParser()
	parser.analyzeAlter(alterTable)
	return parser
}

// analyzeAlter looks for specific changes in the AlterTable statement, that are relevant
// to OnlineDDL/VReplication
func (p *AlterTableParser) analyzeAlter(alterTable *sqlparser.AlterTable) {
	for _, opt := range alterTable.AlterOptions {
		switch opt := opt.(type) {
		case *sqlparser.RenameTableName:
			p.isRenameTable = true
		case *sqlparser.DropColumn:
			p.droppedColumns[opt.Name.Name.String()] = true
		case *sqlparser.ChangeColumn:
			if opt.OldColumn != nil && opt.NewColDefinition != nil {
				oldName := opt.OldColumn.Name.String()
				newName := opt.NewColDefinition.Name.String()
				p.columnRenameMap[oldName] = newName
			}
		case sqlparser.TableOptions:
			for _, tableOption := range opt {
				if strings.ToUpper(tableOption.Name) == "AUTO_INCREMENT" {
					p.isAutoIncrementDefined = true
				}
			}
		}
	}
}

// ParseAlterStatement is the main function of th eparser, and parses an ALTER TABLE statement
func (p *AlterTableParser) ParseAlterStatement(alterQuery string) (err error) {
	stmt, err := sqlparser.ParseStrictDDL(alterQuery)
	if err != nil {
		return err
	}
	alterTable, ok := stmt.(*sqlparser.AlterTable)
	if !ok {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "expected AlterTable statement, got %v", sqlparser.CanonicalString(stmt))
	}
	p.analyzeAlter(alterTable)
	return nil
}

// GetNonTrivialRenames gets a list of renamed column
func (p *AlterTableParser) GetNonTrivialRenames() map[string]string {
	result := make(map[string]string)
	for column, renamed := range p.columnRenameMap {
		if column != renamed {
			result[column] = renamed
		}
	}
	return result
}

// HasNonTrivialRenames is true when columns have been renamed
func (p *AlterTableParser) HasNonTrivialRenames() bool {
	return len(p.GetNonTrivialRenames()) > 0
}

// DroppedColumnsMap returns list of dropped columns
func (p *AlterTableParser) DroppedColumnsMap() map[string]bool {
	return p.droppedColumns
}

// IsRenameTable returns true when the ALTER TABLE statement inclusdes renaming the table
func (p *AlterTableParser) IsRenameTable() bool {
	return p.isRenameTable
}

// IsAutoIncrementDefined returns true when alter options include an explicit AUTO_INCREMENT value
func (p *AlterTableParser) IsAutoIncrementDefined() bool {
	return p.isAutoIncrementDefined
}

// ColumnRenameMap returns the renamed column mapping
func (p *AlterTableParser) ColumnRenameMap() map[string]string {
	return p.columnRenameMap
}
