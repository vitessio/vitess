// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package autoschema

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

func TestNewValidator(t *testing.T) {
	tables := map[string]*TableSchema{
		"x": &TableSchema{numRows: 100, columns: []string{"col1"}}}
	_, err := NewValidator(true, 1000000, tables)
	if err != nil {
		t.Fatalf("Unable to create a validator: %v", err)
	}
}

func TestValidate(t *testing.T) {
	sqlChan := make(chan string, 1)
	sqlChan <- "alter table x int"
	close(sqlChan)
	validator, err := NewValidator(false, 1000000, map[string]*TableSchema{
		"x": &TableSchema{numRows: 100, columns: []string{"col1"}},
	})
	if err != nil {
		t.Fatalf("%v", err)
	}
	err = validator.Validate(sqlChan)
	if err != nil {
		t.Fatalf("%v", err)
	}
	sqlChan = make(chan string, 1)
	sqlChan <- "invalid sql"
	close(sqlChan)
	err = validator.Validate(sqlChan)
	if err == nil {
		t.Fatalf("Validate should fail")
	}
	validator.isInTransaction = true
	sqlChan = make(chan string, 1)
	sqlChan <- "alter table x int"
	close(sqlChan)
	err = validator.Validate(sqlChan)
	if err == nil {
		t.Fatalf("Validate should fail")
	}
}

func TestValidateAlter(t *testing.T) {
	sql := "alter table x int"

	validator, err := NewValidator(false, 1000000, map[string]*TableSchema{
		"x": &TableSchema{numRows: 100, columns: []string{"col1"}},
	})
	if err != nil {
		t.Fatalf("Unable to create a validator: %v", err)
	}
	err = validator.validateAlter(toAlter(sql))
	if err != nil {
		t.Fatalf("%v", err)
	}
	sql = "alter table unknown int"
	err = validator.validateAlter(toAlter(sql))
	if err == nil {
		t.Fatalf("Validate should fail")
	}
	validator.tables["x"].numRows = validator.maxAlterRows + 10
	sql = "alter table x int"
	err = validator.validateAlter(toAlter(sql))
	if err == nil {
		t.Fatalf("validateAlter should fail")
	}
}

func toAlter(sql string) *sqlparser.DDL {
	ast, err := sqlparser.Parse(sql)
	if err != nil {
		panic(fmt.Sprintf("%v", err))
	}
	ddl, ok := ast.(*sqlparser.DDL)
	if !ok {
		panic(fmt.Sprintf("ddl is %s, but should be sqlparser.DDL", reflect.TypeOf(ast)))
	}
	return ddl
}
