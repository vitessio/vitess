/*
Copyright 2019 The Vitess Authors.

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

package worker

import (
	"bytes"
	"regexp"

	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

//
// This file contains utility functions for clone workers.
//

var errExtract = regexp.MustCompile(`\(errno (\d+)\)`)

// makeValueString returns a string that contains all the passed-in rows
// as an insert SQL command's parameters.
func makeValueString(fields []*querypb.Field, rows [][]sqltypes.Value) string {
	buf := bytes.Buffer{}
	for i, row := range rows {
		if i > 0 {
			buf.Write([]byte(",("))
		} else {
			buf.WriteByte('(')
		}
		for j, value := range row {
			if j > 0 {
				buf.WriteByte(',')
			}
			value.EncodeSQL(&buf)
		}
		buf.WriteByte(')')
	}
	return buf.String()
}

// escapeAll runs sqlescape.EscapeID() for all entries in the slice.
func escapeAll(identifiers []string) []string {
	result := make([]string, len(identifiers))
	for i := range identifiers {
		result[i] = sqlescape.EscapeID(identifiers[i])
	}
	return result
}
