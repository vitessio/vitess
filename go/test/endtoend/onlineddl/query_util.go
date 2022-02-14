package onlineddl

import (
	"io"
	"strings"

	"vitess.io/vitess/go/sqltypes"

	"github.com/olekukonko/tablewriter"
)

// PrintQueryResult will pretty-print a QueryResult to the logger.
func PrintQueryResult(writer io.Writer, qr *sqltypes.Result) {
	if qr == nil {
		return
	}
	if len(qr.Rows) == 0 {
		return
	}

	table := tablewriter.NewWriter(writer)
	table.SetAutoFormatHeaders(false)

	// Make header.
	header := make([]string, 0, len(qr.Fields))
	for _, field := range qr.Fields {
		header = append(header, field.Name)
	}
	table.SetHeader(header)

	// Add rows.
	for _, row := range qr.Rows {
		vals := make([]string, 0, len(row))
		for _, val := range row {
			v := val.ToString()
			v = strings.ReplaceAll(v, "\r", " ")
			v = strings.ReplaceAll(v, "\n", " ")
			vals = append(vals, v)
		}
		table.Append(vals)
	}

	// Print table.
	table.Render()
}
