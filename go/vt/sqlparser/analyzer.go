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
	rootNode, err := Parse(sql)
	if err != nil {
		return "", err
	}
	switch rootNode.Type {
	case INSERT:
		return extractDBName(rootNode.At(INSERT_TABLE_OFFSET)), nil
	case UPDATE:
		return extractDBName(rootNode.At(UPDATE_TABLE_OFFSET)), nil
	case DELETE:
		return extractDBName(rootNode.At(DELETE_TABLE_OFFSET)), nil
	}
	return "", fmt.Errorf("statement '%s' is not a dml", sql)
}

func extractDBName(node *Node) string {
	if node.Type != '.' {
		return ""
	}
	return string(node.At(0).Value)
}
