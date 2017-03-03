// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schema

import (
	"html/template"
	"net/http"
	"sort"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/vt/logz"
)

var (
	schemazHeader = []byte(`
		<tr>
			<th>Table</th>
			<th>Columns</th>
			<th>Indexes</th>
			<th>Type</th>
			<th>TableRows</th>
			<th>DataLength</th>
			<th>IndexLength</th>
			<th>DataFree</th>
			<th>MaxDataLength</th>
		</tr>
	`)
	schemazTmpl = template.Must(template.New("example").Parse(`
	{{$top := .}}{{with .Table}}<tr class="low">
			<td>{{.Name}}</td>
			<td>{{range .Columns}}{{.Name}}: {{.Type}}, {{if .IsAuto}}autoinc{{end}}, {{.Default}}<br>{{end}}</td>
			<td>{{range .Indexes}}{{.Name}}: ({{range .Columns}}{{.}},{{end}}), ({{range .Cardinality}}{{.}},{{end}})<br>{{end}}</td>
			<td>{{index $top.Type .Type}}</td>
			<td>{{.TableRows.Get}}</td>
			<td>{{.DataLength.Get}}</td>
			<td>{{.IndexLength.Get}}</td>
			<td>{{.DataFree.Get}}</td>
			<td>{{.MaxDataLength.Get}}</td>
		</tr>{{end}}
	`))
)

type schemazSorter struct {
	rows []*Table
	less func(row1, row2 *Table) bool
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

func schemazHandler(tables map[string]*Table, w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.DEBUGGING); err != nil {
		acl.SendError(w, err)
		return
	}
	logz.StartHTMLTable(w)
	defer logz.EndHTMLTable(w)
	w.Write(schemazHeader)

	tableList := make([]*Table, 0, len(tables))
	for _, t := range tables {
		tableList = append(tableList, t)
	}

	sorter := schemazSorter{
		rows: tableList,
		less: func(row1, row2 *Table) bool {
			return row1.Name.String() > row2.Name.String()
		},
	}
	sort.Sort(&sorter)
	envelope := struct {
		Type  []string
		Table *Table
	}{
		Type: TypeNames,
	}
	for _, Value := range sorter.rows {
		envelope.Table = Value
		if err := schemazTmpl.Execute(w, envelope); err != nil {
			log.Errorf("schemaz: couldn't execute template: %v", err)
		}
	}
}
