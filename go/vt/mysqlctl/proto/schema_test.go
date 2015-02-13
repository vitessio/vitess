// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"reflect"
	"testing"
)

var basicTable1 = &TableDefinition{
	Name:   "table1",
	Schema: "table schema 1",
	Type:   TABLE_BASE_TABLE,
}
var basicTable2 = &TableDefinition{
	Name:   "table2",
	Schema: "table schema 2",
	Type:   TABLE_BASE_TABLE,
}

var table3 = &TableDefinition{
	Name: "table2",
	Schema: "CREATE TABLE `table3` (\n" +
		"id bigint not null,\n" +
		") Engine=InnoDB",
	Type: TABLE_BASE_TABLE,
}

var view1 = &TableDefinition{
	Name:   "view1",
	Schema: "view schema 1",
	Type:   TABLE_VIEW,
}

var view2 = &TableDefinition{
	Name:   "view2",
	Schema: "view schema 2",
	Type:   TABLE_VIEW,
}

func TestToSQLStrings(t *testing.T) {
	var testcases = []struct {
		input *SchemaDefinition
		want  []string
	}{
		{
			// basic SchemaDefinition with create db statement, basic table and basic view
			input: &SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
				TableDefinitions: []*TableDefinition{
					basicTable1,
					view1,
				},
			},
			want: []string{"CREATE DATABASE {{.DatabaseName}}", basicTable1.Schema, view1.Schema},
		},
		{
			// SchemaDefinition doesn't need any tables or views
			input: &SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
			},
			want: []string{"CREATE DATABASE {{.DatabaseName}}"},
		},
		{
			// and can even have an empty DatabaseSchema
			input: &SchemaDefinition{},
			want:  []string{""},
		},
		{
			// with tables but no views
			input: &SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
				TableDefinitions: []*TableDefinition{
					basicTable1,
					basicTable2,
				},
			},
			want: []string{"CREATE DATABASE {{.DatabaseName}}", basicTable1.Schema, basicTable2.Schema},
		},
		{
			// multiple tables and views should be ordered with all tables before views
			input: &SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
				TableDefinitions: []*TableDefinition{
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
			input: &SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
				TableDefinitions: []*TableDefinition{
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
		got := tc.input.ToSQLStrings()
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("ToSQLStrings() on SchemaDefinition %v returned %v; want %v", tc.input, got, tc.want)
		}
	}
}

func testDiff(t *testing.T, left, right *SchemaDefinition, leftName, rightName string, expected []string) {

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
	sd1 := &SchemaDefinition{
		TableDefinitions: []*TableDefinition{
			&TableDefinition{
				Name:   "table1",
				Schema: "schema1",
				Type:   TABLE_BASE_TABLE,
			},
			&TableDefinition{
				Name:   "table2",
				Schema: "schema2",
				Type:   TABLE_BASE_TABLE,
			},
		},
	}
	testDiff(t, sd1, sd1, "sd1", "sd2", []string{})

	sd2 := &SchemaDefinition{TableDefinitions: make([]*TableDefinition, 0, 2)}
	testDiff(t, sd2, sd2, "sd2", "sd2", []string{})

	sd1.DatabaseSchema = "CREATE DATABASE {{.DatabaseName}}"
	sd2.DatabaseSchema = "DONT CREATE DATABASE {{.DatabaseName}}"
	testDiff(t, sd1, sd2, "sd1", "sd2", []string{"sd1 and sd2 don't agree on database creation command:\nCREATE DATABASE {{.DatabaseName}}\n differs from:\nDONT CREATE DATABASE {{.DatabaseName}}", "sd1 has an extra table named table1", "sd1 has an extra table named table2"})
	sd2.DatabaseSchema = "CREATE DATABASE {{.DatabaseName}}"
	testDiff(t, sd2, sd1, "sd2", "sd1", []string{"sd1 has an extra table named table1", "sd1 has an extra table named table2"})

	sd2.TableDefinitions = append(sd2.TableDefinitions, &TableDefinition{Name: "table1", Schema: "schema1", Type: TABLE_BASE_TABLE})
	testDiff(t, sd1, sd2, "sd1", "sd2", []string{"sd1 has an extra table named table2"})

	sd2.TableDefinitions = append(sd2.TableDefinitions, &TableDefinition{Name: "table2", Schema: "schema3", Type: TABLE_BASE_TABLE})
	testDiff(t, sd1, sd2, "sd1", "sd2", []string{"sd1 and sd2 disagree on schema for table table2:\nschema2\n differs from:\nschema3"})
}

func TestFilterTables(t *testing.T) {
	var testcases = []struct {
		desc          string
		input         *SchemaDefinition
		tables        []string
		excludeTables []string
		includeViews  bool
		want          *SchemaDefinition
	}{
		{
			desc: "filter based on tables (whitelist)",
			input: &SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
				TableDefinitions: []*TableDefinition{
					basicTable1,
					basicTable2,
				},
			},
			tables: []string{basicTable1.Name},
			want: &SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
				TableDefinitions: []*TableDefinition{
					basicTable1,
				},
			},
		},
		{
			desc: "filter based on excludeTables (blacklist)",
			input: &SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
				TableDefinitions: []*TableDefinition{
					basicTable1,
					basicTable2,
				},
			},
			excludeTables: []string{basicTable1.Name},
			want: &SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
				TableDefinitions: []*TableDefinition{
					basicTable2,
				},
			},
		},
		{
			desc: "excludeTables may filter out a whitelisted item from tables",
			input: &SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
				TableDefinitions: []*TableDefinition{
					basicTable1,
					basicTable2,
				},
			},
			tables:        []string{basicTable1.Name, basicTable2.Name},
			excludeTables: []string{basicTable1.Name},
			want: &SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
				TableDefinitions: []*TableDefinition{
					basicTable2,
				},
			},
		},
		{
			desc: "exclude views",
			input: &SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
				TableDefinitions: []*TableDefinition{
					basicTable1,
					basicTable2,
					view1,
				},
			},
			includeViews: false,
			want: &SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
				TableDefinitions: []*TableDefinition{
					basicTable1,
					basicTable2,
				},
			},
		},
		{
			desc: "generate new schema version hash when list of tables has changed",
			input: &SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
				TableDefinitions: []*TableDefinition{
					basicTable1,
					basicTable2,
				},
				Version: "dummy-version",
			},
			excludeTables: []string{basicTable1.Name},
			want: &SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
				TableDefinitions: []*TableDefinition{
					basicTable2,
				},
				Version: "6d1d294def9febdb21b35dd19a1dd4c6",
			},
		},
	}

	for _, tc := range testcases {
		got, err := tc.input.FilterTables(tc.tables, tc.excludeTables, tc.includeViews)
		if err != nil {
			t.Errorf("FilterTables() test '%v' on SchemaDefinition %v failed with error %v, want %v", tc.desc, tc.input, err, tc.want)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("FilterTables() test '%v' on SchemaDefinition %v returned %v; want %v", tc.desc, tc.input, got, tc.want)
		}
	}
}
