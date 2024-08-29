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
	ColumnDefinition    *sqlparser.ColumnDefinition
	inPK                bool // Does this column appear in the primary key?
	tableCharsetCollate *charsetCollate
	Env                 *Environment
}

func NewColumnDefinitionEntity(env *Environment, c *sqlparser.ColumnDefinition, inPK bool, tableCharsetCollate *charsetCollate) *ColumnDefinitionEntity {
	return &ColumnDefinitionEntity{
		ColumnDefinition:    c,
		inPK:                inPK,
		tableCharsetCollate: tableCharsetCollate,
		Env:                 env,
	}
}

func (c *ColumnDefinitionEntity) Name() string {
	return c.ColumnDefinition.Name.String()
}

func (c *ColumnDefinitionEntity) NameLowered() string {
	return c.ColumnDefinition.Name.Lowered()
}

func (c *ColumnDefinitionEntity) Clone() *ColumnDefinitionEntity {
	clone := &ColumnDefinitionEntity{
		ColumnDefinition:    sqlparser.Clone(c.ColumnDefinition),
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
		c.ColumnDefinition.Type.Options.Null = ptr.Of(false)
	}
	if c.ColumnDefinition.Type.Options.Null == nil || *c.ColumnDefinition.Type.Options.Null {
		// Nullable column, let'se see if there's already a DEFAULT.
		if c.ColumnDefinition.Type.Options.Default == nil {
			// nope, let's add a DEFAULT NULL
			c.ColumnDefinition.Type.Options.Default = &sqlparser.NullVal{}
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
	if c.ColumnDefinition.Type.Charset.Name != "" && c.ColumnDefinition.Type.Options.Collate == "" {
		// Charset defined without collation. Assign the default collation for that charset.
		collation := c.Env.CollationEnv().DefaultCollationForCharset(c.ColumnDefinition.Type.Charset.Name)
		if collation == collations.Unknown {
			return &UnknownColumnCharsetCollationError{Column: c.ColumnDefinition.Name.String(), Charset: c.tableCharsetCollate.charset}
		}
		c.ColumnDefinition.Type.Options.Collate = c.Env.CollationEnv().LookupName(collation)
	}
	if c.ColumnDefinition.Type.Charset.Name == "" && c.ColumnDefinition.Type.Options.Collate != "" {
		// Column has explicit collation but no charset. We can infer the charset from the collation.
		collationID := c.Env.CollationEnv().LookupByName(c.ColumnDefinition.Type.Options.Collate)
		charset := c.Env.CollationEnv().LookupCharsetName(collationID)
		if charset == "" {
			return &UnknownColumnCollationCharsetError{Column: c.ColumnDefinition.Name.String(), Collation: c.ColumnDefinition.Type.Options.Collate}
		}
		c.ColumnDefinition.Type.Charset.Name = charset
	}
	if c.ColumnDefinition.Type.Charset.Name == "" {
		// Still nothing? Assign the table's charset/collation.
		c.ColumnDefinition.Type.Charset.Name = c.tableCharsetCollate.charset
		if c.ColumnDefinition.Type.Options.Collate == "" {
			c.ColumnDefinition.Type.Options.Collate = c.tableCharsetCollate.collate
		}
		if c.ColumnDefinition.Type.Options.Collate = c.tableCharsetCollate.collate; c.ColumnDefinition.Type.Options.Collate == "" {
			collation := c.Env.CollationEnv().DefaultCollationForCharset(c.tableCharsetCollate.charset)
			if collation == collations.Unknown {
				return &UnknownColumnCharsetCollationError{Column: c.ColumnDefinition.Name.String(), Charset: c.tableCharsetCollate.charset}
			}
			c.ColumnDefinition.Type.Options.Collate = c.Env.CollationEnv().LookupName(collation)
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

	if sqlparser.Equals.RefOfColumnDefinition(cClone.ColumnDefinition, otherClone.ColumnDefinition) {
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
		otherEnumValuesMap := getEnumValuesMap(otherClone.ColumnDefinition.Type.EnumValues)
		for ordinal, enumValue := range cClone.ColumnDefinition.Type.EnumValues {
			if otherOrdinal, ok := otherEnumValuesMap[enumValue]; ok {
				if ordinal != otherOrdinal {
					return nil, &EnumValueOrdinalChangedError{Table: tableName, Column: cClone.ColumnDefinition.Name.String(), Value: enumValue, Ordinal: ordinal, NewOrdinal: otherOrdinal}
				}
			}
		}
	}
	return NewModifyColumnDiffByDefinition(other.ColumnDefinition), nil
}

// Type returns the column's type
func (c *ColumnDefinitionEntity) Type() string {
	return c.ColumnDefinition.Type.Type
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
func (c *ColumnDefinitionEntity) IsGenerated() bool {
	isGenerated, _ := IsGeneratedColumn(c.ColumnDefinition)
	return isGenerated
}

// IsNullable returns true when this column is NULLable
func (c *ColumnDefinitionEntity) IsNullable() bool {
	if c.inPK {
		return false
	}
	return c.ColumnDefinition.Type.Options.Null == nil || *c.ColumnDefinition.Type.Options.Null
}

// IsDefaultNull returns true when this column has DEFAULT NULL
func (c *ColumnDefinitionEntity) IsDefaultNull() bool {
	if !c.IsNullable() {
		return false
	}
	_, ok := c.ColumnDefinition.Type.Options.Default.(*sqlparser.NullVal)
	return ok
}

// IsDefaultNull returns true when this column has DEFAULT NULL
func (c *ColumnDefinitionEntity) HasDefault() bool {
	if c.ColumnDefinition.Type.Options.Default == nil {
		return false
	}
	if c.IsDefaultNull() {
		return true
	}
	return true
}

// IsAutoIncrement returns true when this column is AUTO_INCREMENT
func (c *ColumnDefinitionEntity) IsAutoIncrement() bool {
	return c.ColumnDefinition.Type.Options.Autoincrement
}

// IsUnsigned returns true when this column is UNSIGNED
func (c *ColumnDefinitionEntity) IsUnsigned() bool {
	return c.ColumnDefinition.Type.Unsigned
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

// HasBlobTypeStorage returns true when this column is a text/blob type
func (c *ColumnDefinitionEntity) HasBlobTypeStorage() bool {
	return BlobTypeStorage(c.Type()) != 0
}

// Charset returns the column's charset
func (c *ColumnDefinitionEntity) Charset() string {
	return c.ColumnDefinition.Type.Charset.Name
}

// Collate returns the column's collation
func (c *ColumnDefinitionEntity) Collate() string {
	return c.ColumnDefinition.Type.Options.Collate
}

func (c *ColumnDefinitionEntity) EnumValues() []string {
	return c.ColumnDefinition.Type.EnumValues
}

func (c *ColumnDefinitionEntity) HasEnumValues() bool {
	return len(c.EnumValues()) > 0
}

// EnumValuesOrdinals returns a map of enum values to their ordinals
func (c *ColumnDefinitionEntity) EnumValuesOrdinals() map[string]int {
	m := make(map[string]int, len(c.ColumnDefinition.Type.EnumValues))
	for i, enumValue := range c.ColumnDefinition.Type.EnumValues {
		m[enumValue] = i + 1
	}
	return m
}

// EnumOrdinalValues returns a map of enum ordinals to their values
func (c *ColumnDefinitionEntity) EnumOrdinalValues() map[int]string {
	m := make(map[int]string, len(c.ColumnDefinition.Type.EnumValues))
	for i, enumValue := range c.ColumnDefinition.Type.EnumValues {
		// SET and ENUM values are 1 indexed.
		m[i+1] = enumValue
	}
	return m
}

// Length returns the type length (e.g. 17 for VARCHAR(17), 10 for DECIMAL(10,2), 6 for TIMESTAMP(6), etc.)
func (c *ColumnDefinitionEntity) Length() int {
	if c.ColumnDefinition.Type.Length == nil {
		return 0
	}
	return *c.ColumnDefinition.Type.Length
}

// Scale returns the type scale (e.g. 2 for DECIMAL(10,2))
func (c *ColumnDefinitionEntity) Scale() int {
	if c.ColumnDefinition.Type.Scale == nil {
		return 0
	}
	return *c.ColumnDefinition.Type.Scale
}

// ColumnDefinitionEntityList is a formalized list of ColumnDefinitionEntity, with some
// utility functions.
type ColumnDefinitionEntityList struct {
	Entities []*ColumnDefinitionEntity
	byName   map[string]*ColumnDefinitionEntity
}

func NewColumnDefinitionEntityList(entities []*ColumnDefinitionEntity) *ColumnDefinitionEntityList {
	list := &ColumnDefinitionEntityList{
		Entities: entities,
		byName:   make(map[string]*ColumnDefinitionEntity),
	}
	for _, entity := range entities {
		list.byName[entity.Name()] = entity
		list.byName[entity.NameLowered()] = entity
	}
	return list
}

func (l *ColumnDefinitionEntityList) Len() int {
	return len(l.Entities)
}

// Names returns the names of all the columns in this list
func (l *ColumnDefinitionEntityList) Names() []string {
	names := make([]string, len(l.Entities))
	for i, entity := range l.Entities {
		names[i] = entity.Name()
	}
	return names
}

// GetColumn returns the column with the given name, or nil if not found
func (l *ColumnDefinitionEntityList) GetColumn(name string) *ColumnDefinitionEntity {
	return l.byName[name]
}

// Contains returns true when this list contains all the entities from the other list
func (l *ColumnDefinitionEntityList) Contains(other *ColumnDefinitionEntityList) bool {
	for _, entity := range other.Entities {
		if l.GetColumn(entity.NameLowered()) == nil {
			return false
		}
	}
	return true
}

// Union returns a new ColumnDefinitionEntityList with all the entities from this list and the other list
func (l *ColumnDefinitionEntityList) Union(other *ColumnDefinitionEntityList) *ColumnDefinitionEntityList {
	entities := append(l.Entities, other.Entities...)
	return NewColumnDefinitionEntityList(entities)
}

// Clone creates a copy of this list, with copies of the entities
func (l *ColumnDefinitionEntityList) Clone() *ColumnDefinitionEntityList {
	entities := make([]*ColumnDefinitionEntity, len(l.Entities))
	for i, entity := range l.Entities {
		entities[i] = entity.Clone()
	}
	return NewColumnDefinitionEntityList(entities)
}

// Filter returns a new subset ColumnDefinitionEntityList with only the entities that pass the filter
func (l *ColumnDefinitionEntityList) Filter(include func(entity *ColumnDefinitionEntity) bool) *ColumnDefinitionEntityList {
	var entities []*ColumnDefinitionEntity
	for _, entity := range l.Entities {
		if include(entity) {
			entities = append(entities, entity)
		}
	}
	return NewColumnDefinitionEntityList(entities)
}
