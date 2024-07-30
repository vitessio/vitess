/*
Copyright 2024 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/sqlparser"
)

// IndexDefinitionEntity represents an index definition in a CREATE TABLE statement,
// and includes the list of columns that are part of the index.
type IndexDefinitionEntity struct {
	IndexDefinition *sqlparser.IndexDefinition
	ColumnList      *ColumnDefinitionEntityList
	Env             *Environment
}

func NewIndexDefinitionEntity(env *Environment, indexDefinition *sqlparser.IndexDefinition, columnDefinitionEntitiesList *ColumnDefinitionEntityList) *IndexDefinitionEntity {
	return &IndexDefinitionEntity{
		IndexDefinition: indexDefinition,
		ColumnList:      columnDefinitionEntitiesList,
		Env:             env,
	}
}

func (i *IndexDefinitionEntity) Name() string {
	return i.IndexDefinition.Info.Name.String()
}

func (i *IndexDefinitionEntity) NameLowered() string {
	return i.IndexDefinition.Info.Name.Lowered()
}

// Clone returns a copy of this list, with copies of all the entities.
func (i *IndexDefinitionEntity) Clone() *IndexDefinitionEntity {
	clone := &IndexDefinitionEntity{
		IndexDefinition: sqlparser.Clone(i.IndexDefinition),
		ColumnList:      i.ColumnList.Clone(),
		Env:             i.Env,
	}
	return clone
}

func (i *IndexDefinitionEntity) Len() int {
	return len(i.IndexDefinition.Columns)
}

// IsPrimary returns true if the index is a primary key.
func (i *IndexDefinitionEntity) IsPrimary() bool {
	return i.IndexDefinition.Info.Type == sqlparser.IndexTypePrimary
}

// IsUnique returns true if the index is a unique key.
func (i *IndexDefinitionEntity) IsUnique() bool {
	return i.IndexDefinition.Info.IsUnique()
}

// HasNullable returns true if any of the columns in the index are nullable.
func (i *IndexDefinitionEntity) HasNullable() bool {
	for _, col := range i.ColumnList.Entities {
		if col.IsNullable() {
			return true
		}
	}
	return false
}

// HasFloat returns true if any of the columns in the index are floating point types.
func (i *IndexDefinitionEntity) HasFloat() bool {
	for _, col := range i.ColumnList.Entities {
		if col.IsFloatingPointType() {
			return true
		}
	}
	return false
}

// HasColumnPrefix returns true if any of the columns in the index have a length prefix.
func (i *IndexDefinitionEntity) HasColumnPrefix() bool {
	for _, col := range i.IndexDefinition.Columns {
		if col.Length != nil {
			return true
		}
	}
	return false
}

// ColumnNames returns the names of the columns in the index.
func (i *IndexDefinitionEntity) ColumnNames() []string {
	names := make([]string, 0, len(i.IndexDefinition.Columns))
	for _, col := range i.IndexDefinition.Columns {
		names = append(names, col.Column.String())
	}
	return names
}

// ContainsColumns returns true if the index contains all the columns in the given list.
func (i *IndexDefinitionEntity) ContainsColumns(columns *ColumnDefinitionEntityList) bool {
	return i.ColumnList.Contains(columns)
}

// CoveredByColumns returns true if the index is covered by the given list of columns.
func (i *IndexDefinitionEntity) CoveredByColumns(columns *ColumnDefinitionEntityList) bool {
	return columns.Contains(i.ColumnList)
}

// IndexDefinitionEntityList is a formalized list of IndexDefinitionEntity objects with a few
// utility methods.
type IndexDefinitionEntityList struct {
	Entities []*IndexDefinitionEntity
}

func NewIndexDefinitionEntityList(entities []*IndexDefinitionEntity) *IndexDefinitionEntityList {
	return &IndexDefinitionEntityList{
		Entities: entities,
	}
}

func (l *IndexDefinitionEntityList) Len() int {
	return len(l.Entities)
}

// Names returns the names of the indexes in the list.
func (l *IndexDefinitionEntityList) Names() []string {
	names := make([]string, len(l.Entities))
	for i, entity := range l.Entities {
		names[i] = entity.Name()
	}
	return names
}

// SubsetCoveredByColumns returns a new list of indexes that are covered by the given list of columns.
func (l *IndexDefinitionEntityList) SubsetCoveredByColumns(columns *ColumnDefinitionEntityList) *IndexDefinitionEntityList {
	var subset []*IndexDefinitionEntity
	for _, entity := range l.Entities {
		if entity.CoveredByColumns(columns) {
			subset = append(subset, entity)
		}
	}
	return NewIndexDefinitionEntityList(subset)
}

// First returns the first index in the list, or nil if the list is empty.
func (l *IndexDefinitionEntityList) First() *IndexDefinitionEntity {
	if len(l.Entities) == 0 {
		return nil
	}
	return l.Entities[0]
}
