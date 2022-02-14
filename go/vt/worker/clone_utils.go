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
	return sqlescape.EscapeIDs(identifiers)
}
