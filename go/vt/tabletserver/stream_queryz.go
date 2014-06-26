package tabletserver

import (
	"net/http"
	"text/template"

	"github.com/youtube/vitess/go/acl"
)

var (
	streamqueryzHeader = []byte(`<thead>
		<tr>
			<th>Query</th>
			<th>RemoteAddr</th>
			<th>Username</th>
			<th>Duration</th>
			<th>Start</th>
			<th>SessionID</th>
			<th>TransactionID</th>
			<th>ConnectionID</th>
		</tr>
        </thead>
	`)
	streamqueryzTmpl = template.Must(template.New("example").Parse(`
		<tr> 
			<td>{{.Query}}</td>
			<td>{{.RemoteAddr}}</td>
			<td>{{.Username}}</td>
			<td>{{.Duration}}</td>
			<td>{{.Start}}</td>
			<td>{{.SessionID}}</td>
			<td>{{.TransactionID}}</td>
			<td>{{.ConnID}}</td>
		</tr>
	`))
)

func init() {
	http.HandleFunc("/streamqueryz", streamqueryzHandler)
}

func streamqueryzHandler(w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.DEBUGGING); err != nil {
		acl.SendError(w, err)
		return
	}
	startHTMLTable(w)
	defer endHTMLTable(w)
	w.Write(streamqueryzHeader)
	rows := SqlQueryRpcService.qe.streamQList.GetQueryzRows()
	for i := range rows {
		streamqueryzTmpl.Execute(w, rows[i])
	}
}
