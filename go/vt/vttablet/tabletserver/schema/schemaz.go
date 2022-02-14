package schema

import (
	"html/template"
	"net/http"
	"sort"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logz"
)

var (
	schemazHeader = []byte(`
		<tr>
			<th>Table</th>
			<th>Fields</th>
			<th>Primary Key</th>
			<th>Type</th>
			<th>Metadata</th>
		</tr>
	`)
	schemazTmpl = template.Must(template.New("example").Parse(`
	{{$top := .}}{{with .Table}}<tr class="low">
			<td>{{.Name}}</td>
			<td>{{range .Fields}}{{.Name}}: {{.Type}}<br>{{end}}</td>
			<td>{{range .PKColumns}}{{with index $top.Table.Fields .}}{{.Name}}{{end}}<br>{{end}}</td>
			<td>{{index $top.Type .Type}}</td>
			<td>{{.SequenceInfo}}{{.MessageInfo}}</td>
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
