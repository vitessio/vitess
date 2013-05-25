// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"testing"
)

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
	sd1 := &SchemaDefinition{TableDefinitions: make([]TableDefinition, 2)}
	sd1.TableDefinitions[0].Name = "table1"
	sd1.TableDefinitions[0].Schema = "schema1"
	sd1.TableDefinitions[0].Type = TABLE_BASE_TABLE
	sd1.TableDefinitions[1].Name = "table2"
	sd1.TableDefinitions[1].Schema = "schema2"
	sd1.TableDefinitions[1].Type = TABLE_BASE_TABLE
	testDiff(t, sd1, sd1, "sd1", "sd2", []string{})

	sd2 := &SchemaDefinition{TableDefinitions: make([]TableDefinition, 0, 2)}
	testDiff(t, sd2, sd2, "sd2", "sd2", []string{})

	sd1.DatabaseSchema = "CREATE DATABASE {{.DatabaseName}}"
	sd2.DatabaseSchema = "DONT CREATE DATABASE {{.DatabaseName}}"
	testDiff(t, sd1, sd2, "sd1", "sd2", []string{"sd1 and sd2 don't agree on database creation command:\nCREATE DATABASE {{.DatabaseName}}\n differs from:\nDONT CREATE DATABASE {{.DatabaseName}}", "sd1 has an extra table named table1", "sd1 has an extra table named table2"})
	sd2.DatabaseSchema = "CREATE DATABASE {{.DatabaseName}}"
	testDiff(t, sd2, sd1, "sd2", "sd1", []string{"sd1 has an extra table named table1", "sd1 has an extra table named table2"})

	sd2.TableDefinitions = append(sd2.TableDefinitions, TableDefinition{Name: "table1", Schema: "schema1", Type: TABLE_BASE_TABLE})
	testDiff(t, sd1, sd2, "sd1", "sd2", []string{"sd1 has an extra table named table2"})

	sd2.TableDefinitions = append(sd2.TableDefinitions, TableDefinition{Name: "table2", Schema: "schema3", Type: TABLE_BASE_TABLE})
	testDiff(t, sd1, sd2, "sd1", "sd2", []string{"sd1 and sd2 disagree on schema for table table2:\nschema2\n differs from:\nschema3"})
}
