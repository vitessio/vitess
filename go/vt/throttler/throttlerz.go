/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package throttler

import (
	"fmt"
	"html/template"
	"net/http"
	"strings"
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
	listTemplate.Execute(w, map[string]interface{}{
		"Throttlers": throttlers,
	})
}

func showThrottlerDetails(w http.ResponseWriter, name string) {
	detailsTemplate.Execute(w, name)
}
