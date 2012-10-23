// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"testing"
)

func channelToArray(stream chan string) (result []string) {
	close(stream)
	result = make([]string, 0, 10)
	for text := range stream {
		result = append(result, text)
	}
	return result
}

func testDiff(t *testing.T, left, right *SchemaDefinition, leftName, rightName string, expected []string) {

	result := make(chan string, 10)
	left.DiffSchema(leftName, rightName, right, result)
	actual := channelToArray(result)

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
	sd1.TableDefinitions[1].Name = "table2"
	sd1.TableDefinitions[1].Schema = "schema2"
	testDiff(t, sd1, sd1, "sd1", "sd2", []string{})

	sd2 := &SchemaDefinition{TableDefinitions: make([]TableDefinition, 0, 2)}
	testDiff(t, sd2, sd2, "sd1", "sd2", []string{})
	testDiff(t, sd1, sd2, "sd1", "sd2", []string{"sd1 has an extra table named table1", "sd1 has an extra table named table2"})
	testDiff(t, sd2, sd1, "sd2", "sd1", []string{"sd1 has an extra table named table1", "sd1 has an extra table named table2"})

	sd2.TableDefinitions = append(sd2.TableDefinitions, TableDefinition{Name: "table1", Schema: "schema1"})
	testDiff(t, sd1, sd2, "sd1", "sd2", []string{"sd1 has an extra table named table2"})

	sd2.TableDefinitions = append(sd2.TableDefinitions, TableDefinition{Name: "table2", Schema: "schema3"})
	testDiff(t, sd1, sd2, "sd1", "sd2", []string{"sd1 and sd2 disagree on schema for table table2"})
}
