// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sqlannotations provides functions
// for annotating DML statements with keyspace-id
// comments and parsing them. These annotations
// are used during filtered-replication to route
// the DML statement to the correct shard.
package sqlannotations

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

// AnnotateKeyspaceIdIfDML annotates 'sql' with the given keyspace id
// if 'sql' is a DML statement; otherwise, it does nothing.
func AnnotateKeyspaceIdIfDML(keyspaceId []byte, sql *string) {
	if isDml(*sql) {
		*sql = fmt.Sprintf("%s /* vtgate:: keyspace_id:%s */",
			*sql, hex.EncodeToString(keyspaceId))
	}
}

// AnnotateAsFilteredReplicationUnfriendlyIfDML annotates the given 'sql'
// query as filtered-replication-unfriendly if its a DML statement
// (filtered-replication will log an error if it encounters such a annotation).
// Does nothing if the statement is not a DML statement.
func AnnotateAsFilteredReplicationUnfriendlyIfDML(sql *string) {
	if isDml(*sql) {
		*sql = *sql + " /* vtgate:: filtered_replication_unfriendly */"
	}
}

// Copied from go/vt/vtgate/resolver.go
// TODO(erez): Refactor this.
var (
	insertDML = "insert"
	updateDML = "update"
	deleteDML = "delete"
)

// Returns true if 'querySQL' is an INSERT, UPDATE or DELETE
// statement.
func isDml(querySQL string) bool {
	var sqlKW string
	if i := strings.Index(querySQL, " "); i >= 0 {
		sqlKW = querySQL[:i]
	}
	sqlKW = strings.ToLower(sqlKW)
	if sqlKW == insertDML || sqlKW == updateDML || sqlKW == deleteDML {
		return true
	}
	return false
}

var (
	keyspaceIdReplicationRegexp         = regexp.MustCompile("/\\* vtgate:: keyspace_id:([^ ]*) \\*/")
	unfriendlyFilteredReplicationRegexp = regexp.MustCompile("/\\* vtgate:: filtered_replication_unfriendly \\*/")
)

// ParseSQLAnnotation the annotation from the given statement.
// Returns:
// If a keyspace-id comment exists
//   'keyspaceId' is set to the parsed keyspace id, 'unfriendly' is set to 'false',
//   and err is set to nil.
// Otherwise, if a filtered-replication-unfriendly comment exists
//   'keyspaceId' is set to nil, 'unfriendly' is set to true, and err is nil,
// Otherwise, an error occured (no annotation is considered an error here), and
//   'keyspaceId' is set to nil, 'unfriendly' is set to false, and err is set
//   to an error value.
func ParseSQLAnnotation(sql string) (keyspaceId []byte, unfriendly bool, err error) {
	keyspaceIdMatches := keyspaceIdReplicationRegexp.FindAllStringSubmatch(sql, -1)
	unfriendlyMatches := unfriendlyFilteredReplicationRegexp.FindAllString(sql, -1)
	numKeyspaceMatches := len(keyspaceIdMatches)
	numUnfriendlyMatches := len(unfriendlyMatches)

	if numKeyspaceMatches > 0 {
		unfriendly = false
		if numKeyspaceMatches > 1 || numUnfriendlyMatches > 0 {
			keyspaceId = nil
			err = fmt.Errorf(
				"conflicting annotations found in statement: %v",
				sql)
			return
		}
		// The first element of keyspaceIdMatches[0] is the entire
		// match.
		keyspaceIdHex := keyspaceIdMatches[0][1]
		keyspaceId, err = hex.DecodeString(keyspaceIdHex)
		if err != nil {
			keyspaceId = nil
			err = fmt.Errorf(
				"error parsing keyspace id value in statement: %v (%v)", sql, err)
			return
		}
		return
	}

	if numUnfriendlyMatches > 0 {
		keyspaceId = nil
		unfriendly = true
		err = nil
		return
	}

	// Both numUnfriendlyMatches and numKeyspaceMatches are 0.
	keyspaceId = nil
	unfriendly = false
	err = fmt.Errorf("no annotations found in statement: %v", sql)
	return
}
