/*
Copyright 2021 The Vitess Authors.

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

package onlineddl

import (
	"io"
	"strings"

	"vitess.io/vitess/go/sqltypes"

	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
)

// PrintQueryResult will pretty-print a QueryResult to the logger.
func PrintQueryResult(writer io.Writer, qr *sqltypes.Result) {
	if qr == nil {
		return
	}
	if len(qr.Rows) == 0 {
		return
	}

	table := tablewriter.NewTable(writer,
		tablewriter.WithSymbols(tw.NewSymbols(tw.StyleASCII)),
		tablewriter.WithHeaderAutoFormat(tw.State(-1)),
		tablewriter.WithRowMaxWidth(30),
	)

	// Make header.
	header := make([]any, 0, len(qr.Fields))
	for _, field := range qr.Fields {
		header = append(header, field.Name)
	}
	table.Header(header...)

	// Add rows.
	for _, row := range qr.Rows {
		vals := make([]any, 0, len(row))
		for _, val := range row {
			v := val.ToString()
			v = strings.ReplaceAll(v, "\r", " ")
			v = strings.ReplaceAll(v, "\n", " ")
			vals = append(vals, v)
		}
		if err := table.Append(vals...); err != nil {
			// If append fails, continue with remaining rows
			continue
		}
	}

	// Print table.
	_ = table.Render() // Ignore render error as this is output formatting
}
