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

// columnDetails decorates a column with more details, used by diffing logic
type columnDetails struct {
	col     *sqlparser.ColumnDefinition
	prevCol *columnDetails // previous in sequence in table definition
	nextCol *columnDetails // next in sequence in table definition
}

func (c *columnDetails) identicalOtherThanName(other *sqlparser.ColumnDefinition) bool {
	if other == nil {
		return false
	}
	return sqlparser.Equals.SQLNode(c.col.Type, other.Type)
}

func (c *columnDetails) prevColName() string {
	if c.prevCol == nil {
		return ""
	}
	return c.prevCol.col.Name.String()
}

func (c *columnDetails) nextColName() string {
	if c.nextCol == nil {
		return ""
	}
	return c.nextCol.col.Name.String()
}

func getColName(id *sqlparser.IdentifierCI) *sqlparser.ColName {
	return &sqlparser.ColName{Name: *id}
}

type ModifyColumnDiff struct {
	modifyColumn *sqlparser.ModifyColumn
}

func NewModifyColumnDiff(modifyColumn *sqlparser.ModifyColumn) *ModifyColumnDiff {
	return &ModifyColumnDiff{modifyColumn: modifyColumn}
}

func NewModifyColumnDiffByDefinition(definition *sqlparser.ColumnDefinition) *ModifyColumnDiff {
	modifyColumn := &sqlparser.ModifyColumn{
		NewColDefinition: definition,
	}
	return NewModifyColumnDiff(modifyColumn)
}

type ColumnDefinitionEntity struct {
	columnDefinition *sqlparser.ColumnDefinition
}

func NewColumnDefinitionEntity(c *sqlparser.ColumnDefinition) *ColumnDefinitionEntity {
	return &ColumnDefinitionEntity{columnDefinition: c}
}

// ColumnDiff compares this table statement with another table statement, and sees what it takes to
// change this table to look like the other table.
// It returns an ModifyColumnDiff statement if changes are found, or nil if not.
// The function also requires the charset/collate on the source & target tables. This is because the column's
// charset & collation, if undefined, are really defined by the table's charset & collation.
//
//	Anecdotally, in CreateTableEntity.normalize() we actually actively strip away the charset/collate properties
//	from the column definition, to get a cleaner table definition.
//
// Things get complicated when we consider hints.TableCharsetCollateStrategy. Consider this test case:
//
//	from: "create table t (a varchar(64)) default charset=latin1",
//	to:   "create table t (a varchar(64) CHARACTER SET latin1 COLLATE latin1_bin)",
//
// In both cases, the column is really a latin1. But the tables themselves have different collations.
// In TableCharsetCollateStrict, the situation is handled correctly by converting the table charset as well as
// pinning the column's charset.
// But in TableCharsetCollateIgnoreEmpty or in TableCharsetCollateIgnoreAlways, schemadiff is not supposed to
// change the table's charset. In which case it does need to identify the fact that the two columns are really of
// the same charset, and not report a diff.
func (c *ColumnDefinitionEntity) ColumnDiff(
	other *ColumnDefinitionEntity,
	hints *DiffHints,
	t1cc *charsetCollate,
	t2cc *charsetCollate,
) *ModifyColumnDiff {
	// We need to understand whether the table's charset is going to be ignored. If so, we need to denormalize
	// the columns' charset/collate properties for the sake of this comparison.
	// Denormalization means that if the column does not have charset/collate defined, then it must borrow those
	// propeties from the table.
	denormalizeCharsetCollate := false
	if c.IsTextual() || other.IsTextual() {
		switch hints.TableCharsetCollateStrategy {
		case TableCharsetCollateStrict:
			// No need to do anything
		case TableCharsetCollateIgnoreEmpty:
			if t1cc.charset != t2cc.charset {
				if t1cc.charset == "" || t2cc.charset == "" {
					denormalizeCharsetCollate = true
				}
			}
			if t1cc.collate != t2cc.collate {
				if t1cc.collate == "" || t2cc.collate == "" {
					denormalizeCharsetCollate = true
				}
			}
		case TableCharsetCollateIgnoreAlways:
			if t1cc.charset != t2cc.charset {
				denormalizeCharsetCollate = true
			}
			if t1cc.collate != t2cc.collate {
				denormalizeCharsetCollate = true
			}
		}
	}

	if denormalizeCharsetCollate {
		// We have concluded that the two tables have different charset/collate, and that the diff hints will cause
		// that difference to be ignored.
		// Now is the time to populate (denormalize) the column's charset/collate properties.
		if c.columnDefinition.Type.Charset.Name == "" {
			c.columnDefinition.Type.Charset.Name = t1cc.charset
			defer func() {
				c.columnDefinition.Type.Charset.Name = ""
			}()
		}
		if c.columnDefinition.Type.Options.Collate == "" {
			c.columnDefinition.Type.Options.Collate = t1cc.collate
			defer func() {
				c.columnDefinition.Type.Options.Collate = ""
			}()
		}
		if other.columnDefinition.Type.Charset.Name == "" {
			other.columnDefinition.Type.Charset.Name = t2cc.charset
			defer func() {
				other.columnDefinition.Type.Charset.Name = ""
			}()
		}
		if other.columnDefinition.Type.Options.Collate == "" {
			other.columnDefinition.Type.Options.Collate = t2cc.collate
			defer func() {
				other.columnDefinition.Type.Options.Collate = ""
			}()
		}
	}

	if sqlparser.Equals.RefOfColumnDefinition(c.columnDefinition, other.columnDefinition) {
		return nil
	}

	return NewModifyColumnDiffByDefinition(other.columnDefinition)
}

// IsTextual returns true when this column is of textual type, and is capable of having a character set property
func (c *ColumnDefinitionEntity) IsTextual() bool {
	return charsetTypes[strings.ToLower(c.columnDefinition.Type.Type)]
}
