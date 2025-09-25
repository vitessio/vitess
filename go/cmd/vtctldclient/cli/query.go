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
	"github.com/olekukonko/tablewriter/tw"

	"vitess.io/vitess/go/sqltypes"
)

// WriteQueryResultTable writes a QueryResult as a human-friendly table to the
// the provided io.Writer.
func WriteQueryResultTable(w io.Writer, qr *sqltypes.Result) {
	if qr == nil || len(qr.Rows) == 0 {
		return
	}

	table := tablewriter.NewTable(w,
		tablewriter.WithSymbols(tw.NewSymbols(tw.StyleASCII)),
		tablewriter.WithHeaderAutoFormat(tw.State(-1)),
		tablewriter.WithRowMaxWidth(30),
		tablewriter.WithRendition(tw.Rendition{
			Settings: tw.Settings{
				Separators: tw.Separators{
					BetweenRows: tw.On,
				},
			},
		}),
	)

	header := make([]any, 0, len(qr.Fields))
	for _, field := range qr.Fields {
		header = append(header, field.Name)
	}

	table.Header(header...)

	for _, row := range qr.Rows {
		vals := make([]any, 0, len(row))
		for _, val := range row {
			vals = append(vals, val.ToString())
		}

		if err := table.Append(vals...); err != nil {
			// If append fails, we can't continue building the table
			return
		}
	}

	_ = table.Render() // Ignore render error as this is output formatting
}
