// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"html/template"
	"net/http"
)

const idleHTML = `
<!DOCTYPE html>
<head>
	<title>Worker Status</title>
        <meta http-equiv="refresh" content="2">
</head>
<body>
<p>This worker is idle.</p>
<p><a href="/">Toplevel Menu</a></p>
</body>
`

const workerHTML = `
<!DOCTYPE html>
<head>
	<title>Worker Status</title>
        <meta http-equiv="refresh" content="2">
</head>
<body>
<h2>Worker status:</h2>
<blockquote>
{{.Status}}
</blockquote>
{{if .Done}}
<p><a href="/reset">Reset Job</a></p>
{{end}}
</body>
`

func initStatusHandling() {
	idleTemplate := loadTemplate("idle", idleHTML)
	workerTemplate := loadTemplate("worker", workerHTML)

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		currentWorkerMutex.Lock()
		wrk := currentWorker
		done := currentDone
		currentWorkerMutex.Unlock()

		// no worker -> we serve the idle template
		if wrk == nil {
			executeTemplate(w, idleTemplate, nil)
			return
		}

		// worker -> we serve that
		data := map[string]interface{}{
			"Status": template.HTML(wrk.StatusAsHTML()),
		}
		select {
		case <-done:
			data["Done"] = true
		default:
		}
		executeTemplate(w, workerTemplate, data)
	})

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
