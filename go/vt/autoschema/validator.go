// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package autoschema

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// TableSchema provides a view of MySQL table schema
type TableSchema struct {
	numRows int
	columns []string
}

// Validator apply a set of rules to perform on a schema change
type Validator struct {
	isInTransaction bool
	maxAlterRows    int
	tables          map[string]*TableSchema
}

// NewValidator creates a new Validator instance with given list of rules
func NewValidator(isInTransaction bool, maxAlterRows int, tables map[string]*TableSchema) (*Validator, error) {
	return &Validator{isInTransaction: isInTransaction,
		maxAlterRows: maxAlterRows,
		tables:       tables}, nil
}

// Validate a list of sql string
func (v *Validator) Validate(sqlChan <-chan string) error {
	for sql := range sqlChan {
		// TODO: add Transaction statement support
		ast, err := sqlparser.Parse(sql)
		if err != nil {
			return fmt.Errorf("validation failed: %v", err)
		}
		switch ast.(type) {
		case *sqlparser.DDL:
			v.validateAlter(ast.(*sqlparser.DDL))
		}
	}
	if v.isInTransaction {
		return fmt.Errorf("in Transaction at the end of sql statements")
	}
	return nil
}

// validateAlter checks whether table that being altered has more than preconfigured #rows
// TODO: enhance sqlparser.DDL to extract columns that being altered
func (v *Validator) validateAlter(ddl *sqlparser.DDL) error {
	tableName := string(ddl.Table)
	table, ok := v.tables[tableName]
	if !ok {
		return fmt.Errorf("table: %s does not exist\n", tableName)
	}
	switch ddl.Action {
	case sqlparser.AST_ALTER:
		if v.maxAlterRows < table.numRows {
			return fmt.Errorf("alter statement changes too many rows,"+
				"max rows allowed: %d, rows may be changed: %d\n", v.maxAlterRows, table.numRows)
		}
	}
	return nil
}
