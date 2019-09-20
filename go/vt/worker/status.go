/*
Copyright 2019 The Vitess Authors.

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

package worker

import (
	"fmt"
	"html/template"
	"net/http"
	"strings"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/servenv"
)

const workerStatusPartHTML = servenv.JQueryIncludes + `
<script type="text/javascript">

$(function() {
    getStatus();
});

function getStatus() {
    $('div#status').load('/status');
    setTimeout("getStatus()",10000);
}

</script>
<div id="status"></div>
`

const workerStatusHTML = `
<html>
<head>
<title>Worker Status</title>
</head>
<body>
{{if .Status}}
  <h2>Worker status:</h2>
  <blockquote>
    {{.Status}}
  </blockquote>
  <h2>Worker logs:</h2>
  <blockquote>
    {{.Logs}}
  </blockquote>
  {{if .Done}}
  <p><a href="/reset">Reset Job</a></p>
  {{else}}
  <p><a href="/cancel">Cancel Job</a></p>
  {{end}}
{{else}}
  <p>This worker is idle.</p>
  <p><a href="/">Toplevel Menu</a></p>
{{end}}
</body>
</html>
`

// InitStatusHandling installs webserver handlers for global actions like /status, /reset and /cancel.
func (wi *Instance) InitStatusHandling() {
	// code to serve /status
	workerTemplate := mustParseTemplate("worker", workerStatusHTML)
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if err := acl.CheckAccessHTTP(r, acl.ADMIN); err != nil {
			acl.SendError(w, err)
			return
		}

		wi.currentWorkerMutex.Lock()
		wrk := wi.currentWorker
		logger := wi.currentMemoryLogger
		ctx := wi.currentContext
		err := wi.lastRunError
		stopTime := wi.lastRunStopTime
		wi.currentWorkerMutex.Unlock()

		data := make(map[string]interface{})
		if wrk != nil {
			status := template.HTML("Current worker:<br>\n") + wrk.StatusAsHTML()
			if ctx == nil {
				data["Done"] = true
				if err != nil {
					status += template.HTML(fmt.Sprintf("<br>\nEnded with an error: %v<br>\n", err))
				}
				status += template.HTML(fmt.Sprintf("<br>\n<b>End Time:</b> %v<br>\n", stopTime))
			}
			data["Status"] = status
			if logger != nil {
				data["Logs"] = template.HTML(strings.Replace(logger.String(), "\n", "</br>\n", -1))
			} else {
				data["Logs"] = template.HTML("See console for logs</br>\n")
			}
		}
		executeTemplate(w, workerTemplate, data)
	})

	// add the section in status that does auto-refresh of status div
	servenv.AddStatusPart("Worker Status", workerStatusPartHTML, func() interface{} {
		return nil
	})

	// reset handler
	http.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) {
		if err := acl.CheckAccessHTTP(r, acl.ADMIN); err != nil {
			acl.SendError(w, err)
			return
		}

		if err := wi.Reset(); err != nil {
			httpError(w, err.Error(), nil)
		} else {
			// No worker currently running, we go to the menu.
			http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		}
	})

	// cancel handler
	http.HandleFunc("/cancel", func(w http.ResponseWriter, r *http.Request) {
		if err := acl.CheckAccessHTTP(r, acl.ADMIN); err != nil {
			acl.SendError(w, err)
			return
		}

		if wi.Cancel() {
			// We canceled the running worker. Go back to the status page.
			http.Redirect(w, r, servenv.StatusURLPath(), http.StatusTemporaryRedirect)
		} else {
			// No worker, or not running, we go to the menu.
			http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		}
	})
}
