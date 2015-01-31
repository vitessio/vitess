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
    {{range $i, $group := . }}
      <li><a href="/{{$group.Name}}">{{$group.Name}}</a>: {{$group.Description}}.</li>
    {{end}}
</body>
`

const subIndexHTML = `
{{$name := .Name}}
<!DOCTYPE html>
<head>
  <title>{{$name}} Index</title>
</head>
<body>
  <h1>{{$name}} Index</h1>
    <p>{{.Description}}</p>
    {{range $i, $cmd := .Commands }}
      <li><a href="{{$name}}/{{$cmd.Name}}">{{$cmd.Name}}</a>: {{$cmd.Help}}.</li>
    {{end}}
</body>
`

func httpError(w http.ResponseWriter, format string, err error) {
	log.Errorf(format, err)
	http.Error(w, fmt.Sprintf(format, err), http.StatusInternalServerError)
}

func mustParseTemplate(name, contents string) *template.Template {
	t, err := template.New(name).Parse(contents)
	if err != nil {
		// An invalid template here is a programming error.
		panic(fmt.Sprintf("cannot parse %v template: %v", name, err))
	}
	return t
}

func executeTemplate(w http.ResponseWriter, t *template.Template, data interface{}) {
	if err := t.Execute(w, data); err != nil {
		httpError(w, "error executing template", err)
	}
}

func initInteractiveMode() {
	indexTemplate := mustParseTemplate("index", indexHTML)
	subIndexTemplate := mustParseTemplate("subIndex", subIndexHTML)

	// toplevel menu
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		executeTemplate(w, indexTemplate, commands)
	})

	// command group menus
	for _, cg := range commands {
		// keep a local copy of the Command pointer for the
		// closure.
		pcg := cg
		http.HandleFunc("/"+cg.Name, func(w http.ResponseWriter, r *http.Request) {
			executeTemplate(w, subIndexTemplate, pcg)
		})

		for _, c := range cg.Commands {
			// keep a local copy of the Command pointer for the
			// closure.
			pc := c
			http.HandleFunc("/"+cg.Name+"/"+c.Name, func(w http.ResponseWriter, r *http.Request) {
				pc.interactive(wr, w, r)
			})
		}
	}

	log.Infof("Interactive mode ready")
}
