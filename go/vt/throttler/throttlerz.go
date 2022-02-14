package throttler

import (
	"fmt"
	"html/template"
	"net/http"
	"strings"

	"vitess.io/vitess/go/vt/log"
)

const listHTML = `<!DOCTYPE html>
<title>{{len .Throttlers}} Active Throttler(s)</title>
<ul>
{{range .Throttlers}}
  <li>
    <a href="/throttlerz/{{.}}">{{.}}</a>
  </li>
{{end}}
</ul>
`

const detailsHTML = `<!DOCTYPE html>
<title>Details for Throttler '{{.}}'</title>
<a href="/throttlerlogz/{{.}}">adapative throttling log</a>
TODO(mberlin): Add graphs here.
`

var (
	listTemplate = template.Must(template.New("list").Parse(listHTML))

	detailsTemplate = template.Must(template.New("details").Parse(detailsHTML))
)

func init() {
	http.HandleFunc("/throttlerz/", func(w http.ResponseWriter, r *http.Request) {
		throttlerzHandler(w, r, GlobalManager)
	})
}

func throttlerzHandler(w http.ResponseWriter, r *http.Request, m *managerImpl) {
	// Longest supported URL: /throttlerz/<name>
	parts := strings.SplitN(r.URL.Path, "/", 3)

	if len(parts) != 3 {
		errMsg := fmt.Sprintf("invalid /throttlerz path: %q expected paths: /throttlerz or /throttlerz/<throttler name>", r.URL.Path)
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}

	name := parts[2]
	if name == "" {
		listThrottlers(w, m)
		return
	}

	showThrottlerDetails(w, name)
}

func listThrottlers(w http.ResponseWriter, m *managerImpl) {
	throttlers := m.Throttlers()

	// Log error
	if err := listTemplate.Execute(w, map[string]interface{}{
		"Throttlers": throttlers,
	}); err != nil {
		log.Errorf("listThrottlers failed :%v", err)
	}
}

func showThrottlerDetails(w http.ResponseWriter, name string) {
	// Log error
	if err := detailsTemplate.Execute(w, name); err != nil {
		log.Errorf("showThrottlerDetails failed :%v", err)
	}
}
