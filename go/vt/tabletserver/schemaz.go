// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"html/template"
	"net/http"
	"sort"

	"github.com/henryanand/vitess/go/acl"
	"github.com/henryanand/vitess/go/vt/schema"
)

var (
	schemazHeader = []byte(`
		<tr>
			<th>Table</th>
			<th>Columns</th>
			<th>Indexes</th>
			<th>CacheType</th>
		</tr>
	`)
	schemazTmpl = template.Must(template.New("example").Parse(`
	{{$top := .}}{{with .Table}}<tr class="low">
			<td>{{.Name}}</td>
			<td>{{range .Columns}}{{.Name}}: {{index $top.ColumnCategory .Category}}, {{if .IsAuto}}autoinc{{end}}, {{.Default}}<br>{{end}}</td>
			<td>{{range .Indexes}}{{.Name}}: ({{range .Columns}}{{.}},{{end}}), ({{range .Cardinality}}{{.}},{{end}})<br>{{end}}</td>
			<td>{{index $top.CacheType .CacheType}}</td>
		</tr>{{end}}
	`))
)

func init() {
	http.HandleFunc("/schemaz", schemazHandler)
}

type schemazSorter struct {
	rows []*schema.Table
	less func(row1, row2 *schema.Table) bool
}

func (sorter *schemazSorter) Len() int {
	return len(sorter.rows)
}

func (sorter *schemazSorter) Swap(i, j int) {
	sorter.rows[i], sorter.rows[j] = sorter.rows[j], sorter.rows[i]
}

func (sorter *schemazSorter) Less(i, j int) bool {
	return sorter.less(sorter.rows[i], sorter.rows[j])
}

// schemazHandler displays the schema read by the query service.
func schemazHandler(w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.DEBUGGING); err != nil {
		acl.SendError(w, err)
		return
	}
	startHTMLTable(w)
	defer endHTMLTable(w)
	w.Write(schemazHeader)

	tables := SqlQueryRpcService.qe.schemaInfo.GetSchema()
	sorter := schemazSorter{
		rows: tables,
		less: func(row1, row2 *schema.Table) bool {
			return row1.Name > row2.Name
		},
	}
	sort.Sort(&sorter)
	envelope := struct {
		ColumnCategory []string
		CacheType      []string
		Table          *schema.Table
	}{
		ColumnCategory: []string{"other", "number", "varbinary"},
		CacheType:      []string{"none", "read-write", "write-only"},
	}
	for _, Value := range sorter.rows {
		envelope.Table = Value
		schemazTmpl.Execute(w, envelope)
	}
}
