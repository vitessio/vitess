// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"net/http"

	"github.com/youtube/vitess/go/vt/servenv"
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
{{if .Status}}
  <h2>Worker status:</h2>
  <blockquote>
    {{.Status}}
  </blockquote>
  {{if .Done}}
  <p><a href="/reset">Reset Job</a></p>
  {{end}}
{{else}}
  <p>This worker is idle.</p>
  <p><a href="/">Toplevel Menu</a></p>
{{end}}
`

func initStatusHandling() {
	// code to serve /status
	workerTemplate := loadTemplate("worker", workerStatusHTML)
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		currentWorkerMutex.Lock()
		wrk := currentWorker
		done := currentDone
		currentWorkerMutex.Unlock()

		data := make(map[string]interface{})
		if wrk != nil {
			data["Status"] = wrk.StatusAsHTML()
			select {
			case <-done:
				data["Done"] = true
			default:
			}
		}
		executeTemplate(w, workerTemplate, data)
	})

	// add the section in statusz that does auto-refresh of status div
	servenv.AddStatusPart("Worker Status", workerStatusPartHTML, func() interface{} {
		return nil
	})

	// reset handler
	http.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) {
		currentWorkerMutex.Lock()
		wrk := currentWorker
		done := currentDone
		currentWorkerMutex.Unlock()

		// no worker, we go to the menu
		if wrk == nil {
			http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
			return
		}

		// check the worker is really done
		select {
		case <-done:
			currentWorkerMutex.Lock()
			currentWorker = nil
			currentDone = nil
			currentWorkerMutex.Unlock()
			http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		default:
			httpError(w, "worker still executing", nil)
		}
	})
}
