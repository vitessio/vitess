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

package schemadiff

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// semanticKS is a bogus keyspace, used for consistency purposes. The name is not important
var semanticKS = &vindexes.Keyspace{
	Name:    "ks",
	Sharded: false,
}

// declarativeSchemaInformation is a utility wrapper arounf FakeSI, and adds a few utility functions
// to make it more simple and accessible to schemadiff's logic.
type declarativeSchemaInformation struct {
	semantics.FakeSI
}

func newDeclarativeSchemaInformation() *declarativeSchemaInformation {
	return &declarativeSchemaInformation{
		semantics.FakeSI{
			Tables: make(map[string]*vindexes.Table),
		},
	}
}

// addTable adds a fake table with an empty column list
func (si *declarativeSchemaInformation) addTable(tableName string) {
	tbl := &vindexes.Table{
		Name:                    sqlparser.NewIdentifierCS(tableName),
		Columns:                 []vindexes.Column{},
		ColumnListAuthoritative: true,
		Keyspace:                semanticKS,
	}
	si.Tables[tableName] = tbl
}

// addColumn adds a fake column with n otype. It assumes the table already exists
func (si *declarativeSchemaInformation) addColumn(tableName string, columnName string) {
	col := &vindexes.Column{
		Name: sqlparser.NewIdentifierCI(columnName),
	}
	si.Tables[tableName].Columns = append(si.Tables[tableName].Columns, *col)
}
