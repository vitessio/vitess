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
	"vitess.io/vitess/go/ptr"
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
	columnDefinition    *sqlparser.ColumnDefinition
	inPK                bool // Does this column appear in the primary key?
	tableCharsetCollate *charsetCollate
	Env                 *Environment
}

func NewColumnDefinitionEntity(env *Environment, c *sqlparser.ColumnDefinition, inPK bool, tableCharsetCollate *charsetCollate) *ColumnDefinitionEntity {
	return &ColumnDefinitionEntity{
		columnDefinition:    c,
		inPK:                inPK,
		tableCharsetCollate: tableCharsetCollate,
		Env:                 env,
	}
}

func (c *ColumnDefinitionEntity) Name() string {
	return c.columnDefinition.Name.String()
}

func (c *ColumnDefinitionEntity) Clone() *ColumnDefinitionEntity {
	clone := &ColumnDefinitionEntity{
		columnDefinition:    sqlparser.Clone(c.columnDefinition),
		inPK:                c.inPK,
		tableCharsetCollate: c.tableCharsetCollate,
		Env:                 c.Env,
	}
	return clone
}

// SetExplicitDefaultAndNull sets:
// - NOT NULL, if the columns is part of the PRIMARY KEY
// - DEFAULT NULL, if the columns is NULLable and no DEFAULT was mentioned
// Normally in schemadiff we work the opposite way: we strive to have the minimal equivalent representation
// of a definition. But this function can be used (often in conjunction with Clone()) to enrich a column definition
// so as to have explicit and authoritative view on any particular column.
func (c *ColumnDefinitionEntity) SetExplicitDefaultAndNull() {
	if c.inPK {
		// Any column in the primary key is implicitly NOT NULL.
		c.columnDefinition.Type.Options.Null = ptr.Of(false)
	}
	if c.columnDefinition.Type.Options.Null == nil || *c.columnDefinition.Type.Options.Null {
		// Nullable column, let'se see if there's already a DEFAULT.
		if c.columnDefinition.Type.Options.Default == nil {
			// nope, let's add a DEFAULT NULL
			c.columnDefinition.Type.Options.Default = &sqlparser.NullVal{}
		}
	}
}

// SetExplicitCharsetCollate enriches this column definition with collation and charset. Those may be
// already present, or perhaps just one of them is present (in which case we use the one to populate the other),
// or both might be missing, in which case we use the table's charset/collation.
// Normally in schemadiff we work the opposite way: we strive to have the minimal equivalent representation
// of a definition. But this function can be used (often in conjunction with Clone()) to enrich a column definition
// so as to have explicit and authoritative view on any particular column.
func (c *ColumnDefinitionEntity) SetExplicitCharsetCollate() error {
	if !c.IsTextual() {
		return nil
	}
	// We will now denormalize the columns charset & collate as needed (if empty, populate from table.)
	// Normalizing _this_ column definition:
	if c.columnDefinition.Type.Charset.Name != "" && c.columnDefinition.Type.Options.Collate == "" {
		// Charset defined without collation. Assign the default collation for that charset.
		collation := c.Env.CollationEnv().DefaultCollationForCharset(c.columnDefinition.Type.Charset.Name)
		if collation == collations.Unknown {
			return &UnknownColumnCharsetCollationError{Column: c.columnDefinition.Name.String(), Charset: c.tableCharsetCollate.charset}
		}
		c.columnDefinition.Type.Options.Collate = c.Env.CollationEnv().LookupName(collation)
	}
	if c.columnDefinition.Type.Charset.Name == "" && c.columnDefinition.Type.Options.Collate != "" {
		// Column has explicit collation but no charset. We can infer the charset from the collation.
		collationID := c.Env.CollationEnv().LookupByName(c.columnDefinition.Type.Options.Collate)
		charset := c.Env.CollationEnv().LookupCharsetName(collationID)
		if charset == "" {
			return &UnknownColumnCollationCharsetError{Column: c.columnDefinition.Name.String(), Collation: c.columnDefinition.Type.Options.Collate}
		}
		c.columnDefinition.Type.Charset.Name = charset
	}
	if c.columnDefinition.Type.Charset.Name == "" {
		// Still nothing? Assign the table's charset/collation.
		c.columnDefinition.Type.Charset.Name = c.tableCharsetCollate.charset
		if c.columnDefinition.Type.Options.Collate == "" {
			c.columnDefinition.Type.Options.Collate = c.tableCharsetCollate.collate
		}
		if c.columnDefinition.Type.Options.Collate = c.tableCharsetCollate.collate; c.columnDefinition.Type.Options.Collate == "" {
			collation := c.Env.CollationEnv().DefaultCollationForCharset(c.tableCharsetCollate.charset)
			if collation == collations.Unknown {
				return &UnknownColumnCharsetCollationError{Column: c.columnDefinition.Name.String(), Charset: c.tableCharsetCollate.charset}
			}
			c.columnDefinition.Type.Options.Collate = c.Env.CollationEnv().LookupName(collation)
		}
	}
	return nil
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
	hints *DiffHints,
) (*ModifyColumnDiff, error) {
	cClone := c         // not real clone yet
	otherClone := other // not real clone yet
	if c.IsTextual() || other.IsTextual() {
		cClone = c.Clone()
		if err := cClone.SetExplicitCharsetCollate(); err != nil {
			return nil, err
		}
		otherClone = other.Clone()
		if err := otherClone.SetExplicitCharsetCollate(); err != nil {
			return nil, err
		}
	}

	if sqlparser.Equals.RefOfColumnDefinition(cClone.columnDefinition, otherClone.columnDefinition) {
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
		otherEnumValuesMap := getEnumValuesMap(otherClone.columnDefinition.Type.EnumValues)
		for ordinal, enumValue := range cClone.columnDefinition.Type.EnumValues {
			if otherOrdinal, ok := otherEnumValuesMap[enumValue]; ok {
				if ordinal != otherOrdinal {
					return nil, &EnumValueOrdinalChangedError{Table: tableName, Column: cClone.columnDefinition.Name.String(), Value: enumValue, Ordinal: ordinal, NewOrdinal: otherOrdinal}
				}
			}
		}
	}
	return NewModifyColumnDiffByDefinition(other.columnDefinition), nil
}

// Type returns the column's type
func (c *ColumnDefinitionEntity) Type() string {
	return c.columnDefinition.Type.Type
}

// IsTextual returns true when this column is of textual type, and is capable of having a character set property
func (c *ColumnDefinitionEntity) IsTextual() bool {
	return charsetTypes[strings.ToLower(c.Type())]
}

// IsGenerated returns true when this column is generated, and indicates the storage type (virtual/stored)
func IsGeneratedColumn(col *sqlparser.ColumnDefinition) (bool, sqlparser.ColumnStorage) {
	if col == nil {
		return false, 0
	}
	if col.Type.Options == nil {
		return false, 0
	}
	if col.Type.Options.As == nil {
		return false, 0
	}
	return true, col.Type.Options.Storage
}

// IsGenerated returns true when this column is generated, and indicates the storage type (virtual/stored)
func (c *ColumnDefinitionEntity) IsGenerated() (bool, sqlparser.ColumnStorage) {
	return IsGeneratedColumn(c.columnDefinition)
}

// IsNullable returns true when this column is NULLable
func (c *ColumnDefinitionEntity) IsNullable() bool {
	return c.columnDefinition.Type.Options.Null == nil || *c.columnDefinition.Type.Options.Null
}

// IsDefaultNull returns true when this column has DEFAULT NULL
func (c *ColumnDefinitionEntity) IsDefaultNull() bool {
	if !c.IsNullable() {
		return false
	}
	_, ok := c.columnDefinition.Type.Options.Default.(*sqlparser.NullVal)
	return ok
}

// IsDefaultNull returns true when this column has DEFAULT NULL
func (c *ColumnDefinitionEntity) HasDefault() bool {
	if c.columnDefinition.Type.Options.Default == nil {
		return false
	}
	if c.IsDefaultNull() {
		return true
	}
	return true
}

// IsUnsigned returns true when this column is UNSIGNED
func (c *ColumnDefinitionEntity) IsUnsigned() bool {
	return c.columnDefinition.Type.Unsigned
}

// IsNumeric returns true when this column is a numeric type
func (c *ColumnDefinitionEntity) IsIntegralType() bool {
	return IsIntegralType(c.Type())
}

// IsFloatingPointType returns true when this column is a floating point type
func (c *ColumnDefinitionEntity) IsFloatingPointType() bool {
	return IsFloatingPointType(c.Type())
}

// IsDecimalType returns true when this column is a decimal type
func (c *ColumnDefinitionEntity) IsDecimalType() bool {
	return IsDecimalType(c.Type())
}

// Charset returns the column's charset
func (c *ColumnDefinitionEntity) Charset() string {
	return c.columnDefinition.Type.Charset.Name
}

// Collate returns the column's collation
func (c *ColumnDefinitionEntity) Collate() string {
	return c.columnDefinition.Type.Options.Collate
}

func (c *ColumnDefinitionEntity) EnumValues() []string {
	return c.columnDefinition.Type.EnumValues
}

func (c *ColumnDefinitionEntity) EnumValuesOrdinals() map[string]int {
	m := make(map[string]int, len(c.columnDefinition.Type.EnumValues))
	for i, enumValue := range c.columnDefinition.Type.EnumValues {
		m[enumValue] = i
	}
	return m
}

func (c *ColumnDefinitionEntity) EnumOrdinalValues() map[int]string {
	m := make(map[int]string, len(c.columnDefinition.Type.EnumValues))
	for i, enumValue := range c.columnDefinition.Type.EnumValues {
		// SET and ENUM values are 1 indexed.
		m[i+1] = enumValue
	}
	return m
}

// Length returns the type length (e.g. 17 for VARCHAR(17), 10 for DECIMAL(10,2), 6 for TIMESTAMP(6), etc.)
func (c *ColumnDefinitionEntity) Length() int {
	if c.columnDefinition.Type.Length == nil {
		return 0
	}
	return *c.columnDefinition.Type.Length
}

// Scale returns the type scale (e.g. 2 for DECIMAL(10,2))
func (c *ColumnDefinitionEntity) Scale() int {
	if c.columnDefinition.Type.Scale == nil {
		return 0
	}
	return *c.columnDefinition.Type.Scale
}

// isExpandedColumn sees if target column has any value set/range that is impossible in source column. See GetExpandedColumns comment for examples
func (c *ColumnDefinitionEntity) Expands(source *ColumnDefinitionEntity) (bool, string) {
	if c.IsNullable() && !source.IsNullable() {
		return true, "target is NULL-able, source is not"
	}
	if c.Length() > source.Length() {
		return true, "increased length"
	}
	if c.Scale() > source.Scale() {
		return true, "increased scale"
	}
	if source.IsUnsigned() && !c.IsUnsigned() {
		return true, "source is unsigned, target is signed"
	}
	if IntegralTypeStorage(c.Type()) > IntegralTypeStorage(source.Type()) && IntegralTypeStorage(source.Type()) != 0 {
		return true, "increased integer range"
	}
	if IntegralTypeStorage(source.Type()) <= IntegralTypeStorage(c.Type()) &&
		!source.IsUnsigned() && c.IsUnsigned() {
		// e.g. INT SIGNED => INT UNSIGNED, INT SIGNED => BIGINT UNSIGNED
		return true, "target unsigned value exceeds source unsigned value"
	}
	if FloatingPointTypeStorage(c.Type()) > FloatingPointTypeStorage(source.Type()) && FloatingPointTypeStorage(source.Type()) != 0 {
		return true, "increased floating point range"
	}
	if c.IsFloatingPointType() && !source.IsFloatingPointType() {
		return true, "target is floating point, source is not"
	}
	if c.IsDecimalType() && !source.IsDecimalType() {
		return true, "target is decimal, source is not"
	}
	if c.IsDecimalType() && source.IsDecimalType() {
		if c.Length()-c.Scale() > source.Length()-source.Scale() {
			return true, "increased decimal range"
		}
	}
	if IsExpandingDataType(source.Type(), c.Type()) {
		return true, "target is expanded data type of source"
	}
	if BlobTypeStorage(c.Type()) > BlobTypeStorage(source.Type()) && BlobTypeStorage(source.Type()) != 0 {
		return true, "increased blob range"
	}
	if source.Charset() != c.Charset() {
		if c.Charset() == "utf8mb4" {
			return true, "expand character set to utf8mb4"
		}
		if strings.HasPrefix(c.Charset(), "utf8") && !strings.HasPrefix(source.Charset(), "utf8") {
			// not utf to utf
			return true, "expand character set to utf8"
		}
	}
	for _, colType := range []string{"enum", "set"} {
		// enums and sets have very similar properties, and are practically identical in our analysis
		if source.Type() == colType {
			// this is an enum or a set
			if c.Type() != colType {
				return true, "conversion from enum/set to non-enum/set adds potential values"
			}
			// target is an enum or a set. See if all values on target exist in source
			sourceEnumTokensMap := source.EnumOrdinalValues()
			targetEnumTokensMap := c.EnumOrdinalValues()
			for k, v := range targetEnumTokensMap {
				if sourceEnumTokensMap[k] != v {
					return true, "target enum/set expands or reorders source enum/set"
				}
			}
		}
	}
	return false, ""
}
