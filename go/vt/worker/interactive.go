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

	"golang.org/x/net/context"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
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
	log.Errorf(format, args...)
	http.Error(w, fmt.Sprintf(format, args...), http.StatusInternalServerError)
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
		httpError(w, "error executing template: %v", err)
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
