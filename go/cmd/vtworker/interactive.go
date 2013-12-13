// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"html/template"
	"net/http"

	log "github.com/golang/glog"
)

const indexHTML = `
<!DOCTYPE html>
<head>
	<title>Worker Action Index</title>
</head>
<body>
<h1>Worker Action Index</h1>

<li><a href="/diffs">Diffs</a>: shows a list of all the possible diffs to run.</li>
</body>
`

func httpError(w http.ResponseWriter, format string, err error) {
	log.Errorf(format, err)
	http.Error(w, fmt.Sprintf(format, err), http.StatusInternalServerError)
}

func initInteractiveMode() {
	indexTemplate, err := template.New("index").Parse(indexHTML)
	if err != nil {
		log.Fatalf("Cannot parse index template: %v", err)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if err := indexTemplate.Execute(w, nil); err != nil {
			httpError(w, "error executing template", err)
		}
	})
}
