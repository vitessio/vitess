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

func (i *IndexDefinitionEntity) Clone() *IndexDefinitionEntity {
	clone := &IndexDefinitionEntity{
		IndexDefinition: sqlparser.Clone(i.IndexDefinition),
		Env:             i.Env,
	}
	return clone
}

func (i *IndexDefinitionEntity) Len() int {
	return len(i.IndexDefinition.Columns)
}

func (i *IndexDefinitionEntity) IsPrimary() bool {
	return i.IndexDefinition.Info.Type == sqlparser.IndexTypePrimary
}

func (i *IndexDefinitionEntity) IsUnique() bool {
	return i.IndexDefinition.Info.IsUnique()
}

func (i *IndexDefinitionEntity) HasNullable() bool {
	for _, col := range i.ColumnList.Entities {
		if col.IsNullable() {
			return true
		}
	}
	return false
}

func (i *IndexDefinitionEntity) HasFloat() bool {
	for _, col := range i.ColumnList.Entities {
		if col.IsFloatingPointType() {
			return true
		}
	}
	return false
}

func (i *IndexDefinitionEntity) HasColumnPrefix() bool {
	for _, col := range i.IndexDefinition.Columns {
		if col.Length != nil {
			return true
		}
	}
	return false
}

func (i *IndexDefinitionEntity) ColumnNames() []string {
	var names []string
	for _, col := range i.IndexDefinition.Columns {
		names = append(names, col.Column.String())
	}
	return names
}

func (i *IndexDefinitionEntity) ContainsColumns(columns *ColumnDefinitionEntityList) bool {
	return i.ColumnList.Contains(columns)
}

func (i *IndexDefinitionEntity) CoveredByColumns(columns *ColumnDefinitionEntityList) bool {
	return columns.Contains(i.ColumnList)
}

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

func (l *IndexDefinitionEntityList) Names() []string {
	names := make([]string, len(l.Entities))
	for i, entity := range l.Entities {
		names[i] = entity.Name()
	}
	return names
}

func (l *IndexDefinitionEntityList) SubsetCoveredByColumns(columns *ColumnDefinitionEntityList) *IndexDefinitionEntityList {
	var subset []*IndexDefinitionEntity
	for _, entity := range l.Entities {
		if entity.CoveredByColumns(columns) {
			subset = append(subset, entity)
		}
	}
	return NewIndexDefinitionEntityList(subset)
}

func (l *IndexDefinitionEntityList) First() *IndexDefinitionEntity {
	if len(l.Entities) == 0 {
		return nil
	}
	return l.Entities[0]
}
