/*
Copyright 2019 The Vitess Authors.

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

package tmutils

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/protobuf/proto"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

var basicTable1 = &tabletmanagerdatapb.TableDefinition{
	Name:   "table1",
	Schema: "table schema 1",
	Type:   TableBaseTable,
}
var basicTable2 = &tabletmanagerdatapb.TableDefinition{
	Name:   "table2",
	Schema: "table schema 2",
	Type:   TableBaseTable,
}

var table3 = &tabletmanagerdatapb.TableDefinition{
	Name: "table2",
	Schema: "CREATE TABLE `table3` (\n" +
		"id bigint not null,\n" +
		") Engine=InnoDB",
	Type: TableBaseTable,
}

var view1 = &tabletmanagerdatapb.TableDefinition{
	Name:   "view1",
	Schema: "view schema 1",
	Type:   TableView,
}

var view2 = &tabletmanagerdatapb.TableDefinition{
	Name:   "view2",
	Schema: "view schema 2",
	Type:   TableView,
}

func TestToSQLStrings(t *testing.T) {
	var testcases = []struct {
		input *tabletmanagerdatapb.SchemaDefinition
		want  []string
	}{
		{
			// basic SchemaDefinition with create db statement, basic table and basic view
			input: &tabletmanagerdatapb.SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable1,
					view1,
				},
			},
			want: []string{"CREATE DATABASE {{.DatabaseName}}", basicTable1.Schema, view1.Schema},
		},
		{
			// SchemaDefinition doesn't need any tables or views
			input: &tabletmanagerdatapb.SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
			},
			want: []string{"CREATE DATABASE {{.DatabaseName}}"},
		},
		{
			// and can even have an empty DatabaseSchema
			input: &tabletmanagerdatapb.SchemaDefinition{},
			want:  []string{""},
		},
		{
			// with tables but no views
			input: &tabletmanagerdatapb.SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable1,
					basicTable2,
				},
			},
			want: []string{"CREATE DATABASE {{.DatabaseName}}", basicTable1.Schema, basicTable2.Schema},
		},
		{
			// multiple tables and views should be ordered with all tables before views
			input: &tabletmanagerdatapb.SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					view1,
					view2,
					basicTable1,
					basicTable2,
				},
			},
			want: []string{
				"CREATE DATABASE {{.DatabaseName}}",
				basicTable1.Schema, basicTable2.Schema,
				view1.Schema, view2.Schema,
			},
		},
		{
			// valid table schema gets correctly rewritten to include DatabaseName
			input: &tabletmanagerdatapb.SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable1,
					table3,
				},
			},
			want: []string{
				"CREATE DATABASE {{.DatabaseName}}",
				basicTable1.Schema,
				"CREATE TABLE `{{.DatabaseName}}`.`table3` (\n" +
					"id bigint not null,\n" +
					") Engine=InnoDB",
			},
		},
	}

	for _, tc := range testcases {
		got := SchemaDefinitionToSQLStrings(tc.input)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("ToSQLStrings() on SchemaDefinition %v returned %v; want %v", tc.input, got, tc.want)
		}
	}
}

func testDiff(t *testing.T, left, right *tabletmanagerdatapb.SchemaDefinition, leftName, rightName string, expected []string) {
	t.Helper()

	actual := DiffSchemaToArray(leftName, left, rightName, right)

	equal := false
	if len(actual) == len(expected) {
		equal = true
		for i, val := range actual {
			if val != expected[i] {
				equal = false
				break
			}
		}
	}

	if !equal {
		t.Logf("Expected: %v", expected)
		t.Logf("Actual: %v", actual)
		t.Fail()
	}
}

func TestSchemaDiff(t *testing.T) {
	sd1 := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:   "table1",
				Schema: "schema1",
				Type:   TableBaseTable,
			},
			{
				Name:   "table2",
				Schema: "schema2",
				Type:   TableBaseTable,
			},
		},
	}

	sd2 := &tabletmanagerdatapb.SchemaDefinition{TableDefinitions: make([]*tabletmanagerdatapb.TableDefinition, 0, 2)}

	sd3 := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:   "table2",
				Schema: "schema2",
				Type:   TableBaseTable,
			},
		},
	}

	sd4 := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:   "table2",
				Schema: "table2",
				Type:   TableView,
			},
		},
	}

	sd5 := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:   "table2",
				Schema: "table2",
				Type:   TableBaseTable,
			},
		},
	}

	testDiff(t, sd1, sd1, "sd1", "sd2", []string{})

	testDiff(t, sd2, sd2, "sd2", "sd2", []string{})

	// two schemas are considered the same if both nil
	testDiff(t, nil, nil, "sd1", "sd2", nil)

	testDiff(t, sd1, nil, "sd1", "sd2", []string{
		fmt.Sprintf("schemas are different:\nsd1: %v, sd2: <nil>", sd1),
	})

	testDiff(t, sd1, sd3, "sd1", "sd3", []string{
		"sd1 has an extra table named table1",
	})

	testDiff(t, sd3, sd1, "sd3", "sd1", []string{
		"sd1 has an extra table named table1",
	})

	testDiff(t, sd2, sd4, "sd2", "sd4", []string{
		"sd4 has an extra view named table2",
	})

	testDiff(t, sd4, sd2, "sd4", "sd2", []string{
		"sd4 has an extra view named table2",
	})

	testDiff(t, sd4, sd5, "sd4", "sd5", []string{
		fmt.Sprintf("schemas differ on table type for table table2:\nsd4: VIEW\n differs from:\nsd5: BASE TABLE"),
	})

	sd1.DatabaseSchema = "CREATE DATABASE {{.DatabaseName}}"
	sd2.DatabaseSchema = "DONT CREATE DATABASE {{.DatabaseName}}"
	testDiff(t, sd1, sd2, "sd1", "sd2", []string{"schemas are different:\nsd1: CREATE DATABASE {{.DatabaseName}}\n differs from:\nsd2: DONT CREATE DATABASE {{.DatabaseName}}", "sd1 has an extra table named table1", "sd1 has an extra table named table2"})
	sd2.DatabaseSchema = "CREATE DATABASE {{.DatabaseName}}"
	testDiff(t, sd2, sd1, "sd2", "sd1", []string{"sd1 has an extra table named table1", "sd1 has an extra table named table2"})

	sd2.TableDefinitions = append(sd2.TableDefinitions, &tabletmanagerdatapb.TableDefinition{Name: "table1", Schema: "schema1", Type: TableBaseTable})
	testDiff(t, sd1, sd2, "sd1", "sd2", []string{"sd1 has an extra table named table2"})

	sd2.TableDefinitions = append(sd2.TableDefinitions, &tabletmanagerdatapb.TableDefinition{Name: "table2", Schema: "schema3", Type: TableBaseTable})
	testDiff(t, sd1, sd2, "sd1", "sd2", []string{"schemas differ on table table2:\nsd1: schema2\n differs from:\nsd2: schema3"})
}

func TestFilterTables(t *testing.T) {
	var testcases = []struct {
		desc          string
		input         *tabletmanagerdatapb.SchemaDefinition
		tables        []string
		excludeTables []string
		includeViews  bool
		want          *tabletmanagerdatapb.SchemaDefinition
		wantError     error
	}{
		{
			desc: "filter based on tables (whitelist)",
			input: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable1,
					basicTable2,
				},
			},
			tables: []string{basicTable1.Name},
			want: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable1,
				},
			},
		},
		{
			desc: "filter based on excludeTables (blacklist)",
			input: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable1,
					basicTable2,
				},
			},
			excludeTables: []string{basicTable1.Name},
			want: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable2,
				},
			},
		},
		{
			desc: "excludeTables may filter out a whitelisted item from tables",
			input: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable1,
					basicTable2,
				},
			},
			tables:        []string{basicTable1.Name, basicTable2.Name},
			excludeTables: []string{basicTable1.Name},
			want: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable2,
				},
			},
		},
		{
			desc: "exclude views",
			input: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable1,
					basicTable2,
					view1,
				},
			},
			includeViews: false,
			want: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable1,
					basicTable2,
				},
			},
		},
		{
			desc: "update schema version hash when list of tables has changed",
			input: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable1,
					basicTable2,
				},
				Version: "dummy-version",
			},
			excludeTables: []string{basicTable1.Name},
			want: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable2,
				},
				Version: "6d1d294def9febdb21b35dd19a1dd4c6",
			},
		},
		{
			desc: "invalid regex for tables returns an error",
			input: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable1,
				},
			},
			tables:    []string{"/(/"},
			wantError: errors.New("cannot compile regexp ( for table: error parsing regexp: missing closing ): `(`"),
		},
		{
			desc: "invalid regex for excludeTables returns an error",
			input: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable1,
				},
			},
			excludeTables: []string{"/(/"},
			wantError:     errors.New("cannot compile regexp ( for excludeTable: error parsing regexp: missing closing ): `(`"),
		},
		{
			desc: "table substring doesn't match without regexp (include)",
			input: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable1,
					basicTable2,
				},
			},
			tables: []string{basicTable1.Name[1:]},
			want: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{},
			},
		},
		{
			desc: "table substring matches with regexp (include)",
			input: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable1,
					basicTable2,
				},
			},
			tables: []string{"/" + basicTable1.Name[1:] + "/"},
			want: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable1,
				},
			},
		},
		{
			desc: "table substring doesn't match without regexp (exclude)",
			input: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable1,
					basicTable2,
				},
			},
			excludeTables: []string{basicTable1.Name[1:]},
			want: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable1,
					basicTable2,
				},
			},
		},
		{
			desc: "table substring matches with regexp (exclude)",
			input: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable1,
					basicTable2,
				},
			},
			excludeTables: []string{"/" + basicTable1.Name[1:] + "/"},
			want: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					basicTable2,
				},
			},
		},
	}

	for _, tc := range testcases {
		got, err := FilterTables(tc.input, tc.tables, tc.excludeTables, tc.includeViews)
		if tc.wantError != nil {
			if err == nil {
				t.Fatalf("FilterTables() test '%v' on SchemaDefinition %v did not return an error (result: %v), but should have, wantError %v", tc.desc, tc.input, got, tc.wantError)
			}
			if err.Error() != tc.wantError.Error() {
				t.Errorf("FilterTables() test '%v' on SchemaDefinition %v returned wrong error '%v'; wanted error '%v'", tc.desc, tc.input, err, tc.wantError)
			}
		} else {
			if err != nil {
				t.Errorf("FilterTables() test '%v' on SchemaDefinition %v failed with error %v, want %v", tc.desc, tc.input, err, tc.want)
			}
			if !proto.Equal(got, tc.want) {
				t.Errorf("FilterTables() test '%v' on SchemaDefinition %v returned %v; want %v", tc.desc, tc.input, got, tc.want)
			}
		}
	}
}
