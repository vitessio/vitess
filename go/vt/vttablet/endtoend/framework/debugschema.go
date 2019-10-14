/*
Copyright 2017 Google Inc.

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

package framework

import (
	"encoding/json"
	"fmt"
	"net/http"

	"vitess.io/vitess/go/vt/vterrors"
)

// Table is a subset of schema.Table.
// TODO(sougou): I'm getting a json parsing error
// on the 'Default' field of schema.TabletColumn. Otherwise,
// we should just be able to decode the json output into a schema.Table.
type Table struct {
	Name    string
	Columns []TableColumn
	Type    int
}

// TableColumn contains info about a table's column.
type TableColumn struct {
	Name     string
	Category int
	IsAuto   bool
}

// DebugSchema parses /debug/schema and returns
// a map of the tables keyed by the table name.
func DebugSchema() map[string]Table {
	out := make(map[string]Table)
	var list []Table
	response, err := http.Get(fmt.Sprintf("%s/debug/schema", ServerAddress))
	if err != nil {
		return out
	}
	defer func() { vterrors.LogIfError(response.Body.Close()) }()
	_ = json.NewDecoder(response.Body).Decode(&list)
	for _, table := range list {
		out[table.Name] = table
	}
	return out
}
