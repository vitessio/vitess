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

package servenv

import (
	"bytes"
	"fmt"
	"html"
	"html/template"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/log"
)

// AddStatusPart adds a new section to status. frag is used as a
// subtemplate of the template used to render /debug/status, and will
// be executed using the value of invoking f at the time of the
// /debug/status request. frag is parsed and executed with the
// html/template package. Functions registered with AddStatusFuncs
// may be used in the template.
func AddStatusPart(banner, frag string, f func() interface{}) {
	globalStatus.addStatusPart(banner, frag, f)
}

// AddStatusFuncs merges the provided functions into the set of
// functions used to render /debug/status. Call this before AddStatusPart
// if your template requires custom functions.
func AddStatusFuncs(fmap template.FuncMap) {
	globalStatus.addStatusFuncs(fmap)
}

// AddStatusSection registers a function that generates extra
// information for the global status page, it will be
// used as a header before the information. If more complex output
// than a simple string is required use AddStatusPart instead.
func AddStatusSection(banner string, f func() string) {
	globalStatus.addStatusSection(banner, f)
}

// StatusURLPath returns the path to the status page.
func StatusURLPath() string {
	return "/debug/status"
}

//-----------------------------------------------------------------

var (
	binaryName  = filepath.Base(os.Args[0])
	hostname    string
	serverStart = time.Now()

	globalStatus = newStatusPage("")
)

var statusHTML = `<!DOCTYPE html>
<html>
<head>
<title>Status for {{.BinaryName}}</title>
<style>
body {
font-family: sans-serif;
}
h1 {
clear: both;
width: 100%;
text-align: center;
font-size: 120%;
background: #eef;
}
.lefthand {
float: left;
width: 80%;
}
.righthand {
text-align: right;
}
</style>
</head>

<h1>Status for {{.BinaryName}}</h1>

<div>
<div class=lefthand>
Started: {{.StartTime}}<br>
</div>
<div class=righthand>
Running on {{.Hostname}}<br>
View <a href=/debug/vars>variables</a>,
     <a href=/debug/pprof>debugging profiles</a>,
</div>
</div>`

type statusPage struct {
	mu       sync.RWMutex
	sections []section
	tmpl     *template.Template
	funcMap  template.FuncMap
}

type section struct {
	Banner   string
	Fragment string
	F        func() interface{}
}

func newStatusPage(name string) *statusPage {
	sp := &statusPage{
		funcMap: make(template.FuncMap),
	}
	sp.tmpl = template.Must(sp.reparse(nil))
	if name == "" {
		http.HandleFunc("/debug/status", sp.statusHandler)
	} else {
		http.HandleFunc("/"+name+"/debug/status", sp.statusHandler)
	}
	return sp
}

func (sp *statusPage) reset() {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	sp.sections = nil
	sp.tmpl = template.Must(sp.reparse(nil))
	sp.funcMap = make(template.FuncMap)
}

func (sp *statusPage) addStatusFuncs(fmap template.FuncMap) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	for name, fun := range fmap {
		if !strings.HasPrefix(name, "github_com_vitessio_vitess_") {
			panic("status func registered without proper prefix, need github_com_vitessio_vitess_:" + name)
		}
		if _, ok := sp.funcMap[name]; ok {
			panic("duplicate status func registered: " + name)
		}
		sp.funcMap[name] = fun
	}
}

func (sp *statusPage) addStatusPart(banner, frag string, f func() interface{}) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	secs := append(sp.sections, section{
		Banner:   banner,
		Fragment: frag,
		F:        f,
	})

	var err error
	sp.tmpl, err = sp.reparse(secs)
	if err != nil {
		secs[len(secs)-1] = section{
			Banner:   banner,
			Fragment: "<code>bad status template: {{.}}</code>",
			F:        func() interface{} { return err },
		}
	}
	sp.tmpl, _ = sp.reparse(secs)
	sp.sections = secs
}

func (sp *statusPage) addStatusSection(banner string, f func() string) {
	sp.addStatusPart(banner, `{{.}}`, func() interface{} { return f() })
}

func (sp *statusPage) statusHandler(w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.DEBUGGING); err != nil {
		acl.SendError(w, err)
		return
	}
	sp.mu.Lock()
	defer sp.mu.Unlock()

	data := struct {
		Sections   []section
		BinaryName string
		Hostname   string
		StartTime  string
	}{
		Sections:   sp.sections,
		BinaryName: binaryName,
		Hostname:   hostname,
		StartTime:  serverStart.Format(time.RFC1123),
	}

	if err := sp.tmpl.ExecuteTemplate(w, "status", data); err != nil {
		if _, ok := err.(net.Error); !ok {
			log.Errorf("servenv: couldn't execute template: %v", err)
		}
	}
}

func (sp *statusPage) reparse(sections []section) (*template.Template, error) {
	var buf bytes.Buffer

	io.WriteString(&buf, `{{define "status"}}`)
	io.WriteString(&buf, statusHTML)

	for i, sec := range sections {
		fmt.Fprintf(&buf, "<h1>%s</h1>\n", html.EscapeString(sec.Banner))
		fmt.Fprintf(&buf, "{{$sec := index .Sections %d}}\n", i)
		fmt.Fprintf(&buf, `{{template "sec-%d" call $sec.F}}`+"\n", i)
	}
	fmt.Fprintf(&buf, `</html>`)
	io.WriteString(&buf, "{{end}}\n")

	for i, sec := range sections {
		fmt.Fprintf(&buf, `{{define "sec-%d"}}%s{{end}}\n`, i, sec.Fragment)
	}
	return template.New("").Funcs(sp.funcMap).Parse(buf.String())
}

func init() {
	var err error
	hostname, err = os.Hostname()
	if err != nil {
		log.Exitf("os.Hostname: %v", err)
	}
}
