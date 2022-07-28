/*
Copyright 2022 The Vitess Authors.

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

package cli

import (
	"io"

	"github.com/olekukonko/tablewriter"

	"vitess.io/vitess/go/sqltypes"
)

// WriteQueryResultTable writes a QueryResult as a human-friendly table to the
// the provided io.Writer.
func WriteQueryResultTable(w io.Writer, qr *sqltypes.Result) {
	if qr == nil || len(qr.Rows) == 0 {
		return
	}

	table := tablewriter.NewWriter(w)
	table.SetAutoFormatHeaders(false)

	header := make([]string, 0, len(qr.Fields))
	for _, field := range qr.Fields {
		header = append(header, field.Name)
	}

	table.SetHeader(header)

	for _, row := range qr.Rows {
		vals := make([]string, 0, len(row))
		for _, val := range row {
			vals = append(vals, val.ToString())
		}

		table.Append(vals)
	}

	table.Render()
}
