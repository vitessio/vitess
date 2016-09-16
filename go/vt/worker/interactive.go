// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"html/template"
	"net/http"

	"golang.org/x/net/context"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/vt/servenv"
)

const indexHTML = `
<!DOCTYPE html>
<head>
  <title>Worker Action Index</title>
</head>
<body>
  <h1>Worker Action Index</h1>
    {{range $i, $group := . }}
      <li><a href="/{{$group.Name}}">{{$group.Name}}</a>: {{$group.Description}}</li>
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
      <li><a href="{{$name}}/{{$cmd.Name}}">{{$cmd.Name}}</a>: {{$cmd.Help}}</li>
    {{end}}
</body>
`

func httpError(w http.ResponseWriter, format string, args ...interface{}) {
	log.Errorf(format, args)
	http.Error(w, fmt.Sprintf(format, args), http.StatusInternalServerError)
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

// InitInteractiveMode installs webserver handlers for each known command.
func (wi *Instance) InitInteractiveMode() {
	indexTemplate := mustParseTemplate("index", indexHTML)
	subIndexTemplate := mustParseTemplate("subIndex", subIndexHTML)

	// toplevel menu
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if err := acl.CheckAccessHTTP(r, acl.ADMIN); err != nil {
			acl.SendError(w, err)
			return
		}

		executeTemplate(w, indexTemplate, commands)
	})

	// command group menus
	for _, cg := range commands {
		// keep a local copy of the Command pointer for the
		// closure.
		pcg := cg
		http.HandleFunc("/"+cg.Name, func(w http.ResponseWriter, r *http.Request) {
			if err := acl.CheckAccessHTTP(r, acl.ADMIN); err != nil {
				acl.SendError(w, err)
				return
			}

			executeTemplate(w, subIndexTemplate, pcg)
		})

		for _, c := range cg.Commands {
			// keep a local copy of the Command pointer for the closure.
			pc := c
			http.HandleFunc("/"+cg.Name+"/"+c.Name, func(w http.ResponseWriter, r *http.Request) {
				if err := acl.CheckAccessHTTP(r, acl.ADMIN); err != nil {
					acl.SendError(w, err)
					return
				}

				wrk, template, data, err := pc.Interactive(context.Background(), wi, wi.wr, w, r)
				if err != nil {
					httpError(w, "%s", err)
				} else if template != nil && data != nil {
					executeTemplate(w, template, data)
					return
				}

				if wrk == nil {
					httpError(w, "Internal server error. Command: %s did not return correct response.", c.Name)
					return
				}

				if _, err := wi.setAndStartWorker(context.Background(), wrk, wi.wr); err != nil {
					httpError(w, "Could not set %s worker: %s", c.Name, err)
					return
				}
				http.Redirect(w, r, servenv.StatusURLPath(), http.StatusTemporaryRedirect)
			})
		}
	}

	log.Infof("Interactive mode ready")
}
