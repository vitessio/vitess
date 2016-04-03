// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package framework

import (
	"encoding/json"
	"fmt"
	"net/http"
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
	defer response.Body.Close()
	_ = json.NewDecoder(response.Body).Decode(&list)
	for _, table := range list {
		out[table.Name] = table
	}
	return out
}
