// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

// analyzer.go contains utility analysis functions.
// TODO(sougou): Move some generic functions out of exection.go
// and router.go into this file.

import "fmt"

// IsCrossDB returns true if the DML uses
// schema qualifier for table names.
// It returns an error if parsing fails.
func IsCrossDB(sql string) (bool, error) {
	rootNode, err := Parse(sql)
	if err != nil {
		return false, err
	}
	switch rootNode.Type {
	case INSERT:
		return rootNode.At(INSERT_TABLE_OFFSET).Type == '.', nil
	case UPDATE:
		return rootNode.At(UPDATE_TABLE_OFFSET).Type == '.', nil
	case DELETE:
		return rootNode.At(DELETE_TABLE_OFFSET).Type == '.', nil
	}
	return false, fmt.Errorf("statement '%s' is not a dml", sql)
}
