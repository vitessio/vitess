/*
Copyright 2017 Google Inc.

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

// Package sqlannotation provides functions
// for annotating DML statements with keyspace-id
// comments and parsing them. These annotations
// are used during filtered-replication to route
// the DML statement to the correct shard.
// TOOD(erez): Move the code for the "_stream" annotations
// from vttablet to here.
package sqlannotation

import (
	"encoding/hex"
	"fmt"
	"log"
	"strings"

	"bytes"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	filteredReplicationUnfriendlyAnnotation = "/* vtgate:: filtered_replication_unfriendly */"
)

var (
	filteredReplicationUnfriendlyStatementsCount = stats.NewCounter("FilteredReplicationUnfriendlyStatementsCount", "Count of unfriendly statements found in filtered replication")
)

// AnnotateIfDML annotates 'sql' based on 'keyspaceIDs'
//
// If 'sql' is not a DML statement no annotation is added.
// If 'sql' is a DML statement and contains exactly one keyspaceID
//    it is used to annotate 'sql'
// Otherwise 'sql' is annotated as replication-unfriendly.
func AnnotateIfDML(sql string, keyspaceIDs [][]byte) string {
	if !sqlparser.IsDML(sql) {
		return sql
	}
	if len(keyspaceIDs) == 1 {
		return AddKeyspaceIDs(sql, keyspaceIDs, "")
	}
	filteredReplicationUnfriendlyStatementsCount.Add(1)
	return sql + filteredReplicationUnfriendlyAnnotation
}

// AddKeyspaceIDs returns a copy of 'sql' annotated
// with the given keyspace id. It also appends the
// additional marginComments, if any.
func AddKeyspaceIDs(sql string, keyspaceIDs [][]byte, marginComments string) string {
	encodedIDs := make([][]byte, len(keyspaceIDs))
	for i, src := range keyspaceIDs {
		encodedIDs[i] = make([]byte, hex.EncodedLen(len(src)))
		hex.Encode(encodedIDs[i], src)
	}
	return fmt.Sprintf("%s /* vtgate:: keyspace_id:%s */%s",
		sql, bytes.Join(encodedIDs, []byte(",")), marginComments)
}

// ExtractKeyspaceIDS parses the annotation of the given statement and tries
// to extract the keyspace id.
// If a keyspace-id comment exists 'keyspaceID' is set to the parsed keyspace id
// and err is set to nil; otherwise, if a filtered-replication-unfriendly comment exists
// or some other parsing error occured, keyspaceID is set to nil and err is set to a non-nil
// error value.
func ExtractKeyspaceIDS(sql string) (keyspaceIDs [][]byte, err error) {
	_, comments := sqlparser.SplitMarginComments(sql)
	keyspaceIDString, hasKeyspaceID := extractStringBetween(comments.Trailing, "/* vtgate:: keyspace_id:", " ")
	hasUnfriendlyAnnotation := strings.Contains(sql, filteredReplicationUnfriendlyAnnotation)
	if !hasKeyspaceID {
		if hasUnfriendlyAnnotation {
			return nil, &ExtractKeySpaceIDError{
				Kind:    ExtractKeySpaceIDReplicationUnfriendlyError,
				Message: fmt.Sprintf("Statement: %v", sql),
			}
		}
		// No annotations.
		return nil, &ExtractKeySpaceIDError{
			Kind:    ExtractKeySpaceIDParseError,
			Message: fmt.Sprintf("No annotation found in '%v'", sql),
		}
	}
	if hasUnfriendlyAnnotation {
		return nil, &ExtractKeySpaceIDError{
			Kind:    ExtractKeySpaceIDParseError,
			Message: fmt.Sprintf("Conflicting annotations in statement '%v'", sql),
		}
	}
	ksidStr := strings.Split(keyspaceIDString, ",")
	keyspaceIDs = make([][]byte, len(ksidStr))
	for row, ksid := range ksidStr {
		err = nil
		keyspaceIDs[row], err = hex.DecodeString(ksid)
		if err != nil {
			keyspaceIDs[row] = nil
			err = &ExtractKeySpaceIDError{
				Kind: ExtractKeySpaceIDParseError,
				Message: fmt.Sprintf(
					"Error parsing keyspace id value in statement: %v (%v)", sql, err),
			}
		}
	}
	return
}

// Extracts the string from source contained between the leftmost instance of
// 'leftDelim' and the next instance of 'rightDelim'. If there is no next instance
// of 'rightDelim', returns the string contained between the end of the leftmost instance
// of 'leftDelim' to the end of 'source'. If 'leftDelim' does not appear in 'source',
// sets 'found' to false and 'match' to the empty string, otherwise 'found' is set to true
// and 'match' is set to the extracted string.
func extractStringBetween(source string, leftDelim string, rightDelim string) (match string, found bool) {
	leftDelimStart := strings.Index(source, leftDelim)
	if leftDelimStart == -1 {
		found = false
		match = ""
		return
	}
	found = true
	matchStart := leftDelimStart + len(leftDelim)
	matchEnd := strings.Index(source[matchStart:], rightDelim)
	if matchEnd != -1 {
		match = source[matchStart : matchStart+matchEnd]
		return
	}
	match = source[matchStart:]
	return
}

// ExtractKeySpaceIDError is the error type returned
// from ExtractKeySpaceID
// Kind is a numeric code for the error (see constants below)
// and Message is an error message string.
type ExtractKeySpaceIDError struct {
	Kind    int
	Message string
}

// Possible values for ExtractKeySpaceIDError.Kind
const (
	ExtractKeySpaceIDParseError                 = iota
	ExtractKeySpaceIDReplicationUnfriendlyError = iota
)

func (err ExtractKeySpaceIDError) Error() string {
	switch err.Kind {
	case ExtractKeySpaceIDParseError:
		return fmt.Sprintf("Parse-Error. %v", err.Message)
	case ExtractKeySpaceIDReplicationUnfriendlyError:
		return fmt.Sprintf(
			"Statement is filtered-replication-unfriendly. %v", err.Message)
	default:
		log.Fatalf("Unknown error type: %v", err)
		return "" // Unreachable.
	}
}
