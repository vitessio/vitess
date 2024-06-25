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
	Env             *Environment
}

func NewIndexDefinitionEntity(env *Environment, i *sqlparser.IndexDefinition) *IndexDefinitionEntity {
	return &IndexDefinitionEntity{
		IndexDefinition: i,
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
