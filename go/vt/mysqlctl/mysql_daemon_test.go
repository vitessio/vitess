// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"reflect"
	"testing"

	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

// Test that the fake mysql .GetSchema() implementation respects the excludeTables parameter.
func TestFakeMysqlDaemonGetSchemaExcludeTables(t *testing.T) {
	fmd := FakeMysqlDaemon{}

	includedTable := &myproto.TableDefinition{
		Name:              "includedTable",
		Columns:           []string{"id", "msg", "keyspace_id"},
		PrimaryKeyColumns: []string{"id"},
		Type:              myproto.TABLE_BASE_TABLE,
	}
	excludedTable := &myproto.TableDefinition{
		Name:              "excludedTable",
		Columns:           []string{"id", "msg", "keyspace_id"},
		PrimaryKeyColumns: []string{"id"},
		Type:              myproto.TABLE_BASE_TABLE,
	}

	fmd.Schema = &myproto.SchemaDefinition{
		DatabaseSchema: "",
		TableDefinitions: []*myproto.TableDefinition{
			includedTable,
			excludedTable,
		},
	}

	schema, error := fmd.GetSchema("dbName", nil /* tables */, []string{excludedTable.Name}, true /* includeViews */)
	if error != nil {
		t.Fatal("GetSchema must not return an error")
	}
	if len(schema.TableDefinitions) != 1 {
		t.Fatal("there should be only one included table")
	}
	if !reflect.DeepEqual(schema.TableDefinitions[0], includedTable) {
		t.Errorf("included table must be included")
	}
}
