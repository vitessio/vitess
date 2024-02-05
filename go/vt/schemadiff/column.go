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

	"vitess.io/vitess/go/mysql/collations"
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
// We need to denormalize the column's charset/collate properties, so that the comparison can be done.
func (c *ColumnDefinitionEntity) ColumnDiff(
	env *Environment,
	tableName string,
	other *ColumnDefinitionEntity,
	t1cc *charsetCollate,
	t2cc *charsetCollate,
	hints *DiffHints,
) (*ModifyColumnDiff, error) {
	if c.IsTextual() || other.IsTextual() {
		// We will now denormalize the columns charset & collate as needed (if empty, populate from table.)

		if c.columnDefinition.Type.Charset.Name == "" && c.columnDefinition.Type.Options.Collate != "" {
			// Column has explicit collation but no charset. We can infer the charset from the collation.
			collationID := env.CollationEnv().LookupByName(c.columnDefinition.Type.Options.Collate)
			charset := env.CollationEnv().LookupCharsetName(collationID)
			if charset == "" {
				return nil, &UnknownColumnCollationCharsetError{Column: c.columnDefinition.Name.String(), Collation: c.columnDefinition.Type.Options.Collate}
			}
			defer func() {
				c.columnDefinition.Type.Charset.Name = ""
			}()
			c.columnDefinition.Type.Charset.Name = charset
		}
		if c.columnDefinition.Type.Charset.Name == "" {
			defer func() {
				c.columnDefinition.Type.Charset.Name = ""
				c.columnDefinition.Type.Options.Collate = ""
			}()
			c.columnDefinition.Type.Charset.Name = t1cc.charset
			if c.columnDefinition.Type.Options.Collate == "" {
				defer func() {
					c.columnDefinition.Type.Options.Collate = ""
				}()
				c.columnDefinition.Type.Options.Collate = t1cc.collate
			}
			if c.columnDefinition.Type.Options.Collate = t1cc.collate; c.columnDefinition.Type.Options.Collate == "" {
				collation := env.CollationEnv().DefaultCollationForCharset(t1cc.charset)
				if collation == collations.Unknown {
					return nil, &UnknownColumnCharsetCollationError{Column: c.columnDefinition.Name.String(), Charset: t1cc.charset}
				}
				c.columnDefinition.Type.Options.Collate = env.CollationEnv().LookupName(collation)
			}
		}
		if other.columnDefinition.Type.Charset.Name == "" && other.columnDefinition.Type.Options.Collate != "" {
			// Column has explicit collation but no charset. We can infer the charset from the collation.
			collationID := env.CollationEnv().LookupByName(other.columnDefinition.Type.Options.Collate)
			charset := env.CollationEnv().LookupCharsetName(collationID)
			if charset == "" {
				return nil, &UnknownColumnCollationCharsetError{Column: other.columnDefinition.Name.String(), Collation: other.columnDefinition.Type.Options.Collate}
			}
			defer func() {
				other.columnDefinition.Type.Charset.Name = ""
			}()
			other.columnDefinition.Type.Charset.Name = charset
		}

		if other.columnDefinition.Type.Charset.Name == "" {
			defer func() {
				other.columnDefinition.Type.Charset.Name = ""
				other.columnDefinition.Type.Options.Collate = ""
			}()
			other.columnDefinition.Type.Charset.Name = t2cc.charset
			if other.columnDefinition.Type.Options.Collate = t2cc.collate; other.columnDefinition.Type.Options.Collate == "" {
				collation := env.CollationEnv().DefaultCollationForCharset(t2cc.charset)
				if collation == collations.Unknown {
					return nil, &UnknownColumnCharsetCollationError{Column: other.columnDefinition.Name.String(), Charset: t2cc.charset}
				}
				other.columnDefinition.Type.Options.Collate = env.CollationEnv().LookupName(collation)
			}
		}
	}

	if sqlparser.Equals.RefOfColumnDefinition(c.columnDefinition, other.columnDefinition) {
		return nil, nil
	}

	getEnumValuesMap := func(enumValues []string) map[string]int {
		m := make(map[string]int, len(enumValues))
		for i, enumValue := range enumValues {
			m[enumValue] = i
		}
		return m
	}
	switch hints.EnumReorderStrategy {
	case EnumReorderStrategyReject:
		otherEnumValuesMap := getEnumValuesMap(other.columnDefinition.Type.EnumValues)
		for ordinal, enumValue := range c.columnDefinition.Type.EnumValues {
			if otherOrdinal, ok := otherEnumValuesMap[enumValue]; ok {
				if ordinal != otherOrdinal {
					return nil, &EnumValueOrdinalChangedError{Table: tableName, Column: c.columnDefinition.Name.String(), Value: enumValue, Ordinal: ordinal, NewOrdinal: otherOrdinal}
				}
			}
		}
	}
	return NewModifyColumnDiffByDefinition(other.columnDefinition), nil
}

// IsTextual returns true when this column is of textual type, and is capable of having a character set property
func (c *ColumnDefinitionEntity) IsTextual() bool {
	return charsetTypes[strings.ToLower(c.columnDefinition.Type.Type)]
}
