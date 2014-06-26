// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

// analyzer.go contains utility analysis functions.
// TODO(sougou): Move some generic functions out of execution.go
// and router.go into this file.

import "fmt"

// GetDBName parses the specified DML and returns the
// db name if it was used to qualify the table name.
// It returns an error if parsing fails or if the statement
// is not a DML.
func GetDBName(sql string) (string, error) {
	statement, err := Parse(sql)
	if err != nil {
		return "", err
	}
	switch stmt := statement.(type) {
	case *Insert:
		return string(stmt.Table.Qualifier), nil
	case *Update:
		return string(stmt.Table.Qualifier), nil
	case *Delete:
		return string(stmt.Table.Qualifier), nil
	}
	return "", fmt.Errorf("statement '%s' is not a dml", sql)
}
