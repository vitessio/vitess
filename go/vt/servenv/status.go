/*
Copyright 2020 The Vitess Authors.

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

package servenv

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/safehtml"
	"github.com/google/safehtml/template"
	"github.com/google/safehtml/template/uncheckedconversions"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/log"
)

// AddStatusPart adds a new section to status. fragment is used as a
// subtemplate of the template used to render /debug/status, and will
// be executed using the value of invoking f at the time of the
// /debug/status request. fragment is parsed and executed with the
// html/template package. Functions registered with AddStatusFuncs
// may be used in the template.
func AddStatusPart(banner, fragment string, f func() any) {
	globalStatus.addStatusPart(banner, fragment, f)
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
	binaryName       = filepath.Base(os.Args[0])
	hostname         string
	serverStart      = time.Now()
	blockProfileRate = 0

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

<script>
function refreshTablesHaveClassRefreshRequired() {
  var xhr = new XMLHttpRequest();
  xhr.open("GET", "/debug/status", true);
  xhr.onreadystatechange = function() {
    if (this.readyState === XMLHttpRequest.DONE && this.status === 200) {
  	  var data = this.responseText;
  	  var parser = new DOMParser();
  	  var htmlDoc = parser.parseFromString(data, "text/html");
  	  var tables = document.getElementsByClassName("refreshRequired");
  	  var counter = 0;
  	  for (var i = 0; i < tables.length; i++) {
  	    var newTable = htmlDoc.querySelectorAll("table.refreshRequired")[counter];
  	    if (newTable) {
  	  	tables[i].innerHTML = newTable.innerHTML;
  	    }
  	    counter++;
  	  }
    }
  };
  xhr.send();
}
if (` + strconv.Itoa(tableRefreshInterval) + ` !== 0) {
	setInterval(refreshTablesHaveClassRefreshRequired, ` + strconv.Itoa(tableRefreshInterval) + `);
}
</script>
<div>
<div class=lefthand>
Started: {{.StartTime}}<br>
</div>
<div class=righthand>
Running on {{.Hostname}}<br>
View: <a href=/debug/vars>variables</a>,
     <a href=/debug/pprof>debugging profiles</a>
</div>
<br>
<div class=righthand>
Toggle (careful!): <a href=/debug/blockprofilerate>block profiling</a>,
     <a href=/debug/mutexprofilefraction>mutex profiling</a>
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
	F        func() any
}

func newStatusPage(name string) *statusPage {
	sp := &statusPage{
		funcMap: make(template.FuncMap),
	}
	sp.tmpl = template.Must(sp.reparse(nil))
	if name == "" {
		HTTPHandleFunc(StatusURLPath(), sp.statusHandler)
		// Debug profiles are only supported for the top level status page.
		registerDebugBlockProfileRate()
		registerDebugMutexProfileFraction()
	} else {
		pat, _ := url.JoinPath("/", name, StatusURLPath())
		HTTPHandleFunc(pat, sp.statusHandler)
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

func (sp *statusPage) addStatusPart(banner, fragment string, f func() any) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	secs := append(sp.sections, section{
		Banner:   banner,
		Fragment: fragment,
		F:        f,
	})

	var err error
	sp.tmpl, err = sp.reparse(secs)
	if err != nil {
		secs[len(secs)-1] = section{
			Banner:   banner,
			Fragment: "<code>bad status template: {{.}}</code>",
			F:        func() any { return err },
		}
	}
	sp.tmpl, _ = sp.reparse(secs)
	sp.sections = secs
}

func (sp *statusPage) addStatusSection(banner string, f func() string) {
	sp.addStatusPart(banner, `{{.}}`, func() any { return f() })
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
	var buf strings.Builder

	io.WriteString(&buf, `{{define "status"}}`)
	io.WriteString(&buf, statusHTML)

	for i, sec := range sections {
		fmt.Fprintf(&buf, "<h1>%s</h1>\n", safehtml.HTMLEscaped(sec.Banner))
		fmt.Fprintf(&buf, "{{$sec := index .Sections %d}}\n", i)
		fmt.Fprintf(&buf, `{{template "sec-%d" call $sec.F}}`+"\n", i)
	}
	fmt.Fprintf(&buf, `</html>`)
	io.WriteString(&buf, "{{end}}\n")

	for i, sec := range sections {
		fmt.Fprintf(&buf, `{{define "sec-%d"}}%s{{end}}\n`, i, sec.Fragment)
	}
	return template.New("").Funcs(sp.funcMap).ParseFromTrustedTemplate(uncheckedconversions.TrustedTemplateFromStringKnownToSatisfyTypeContract(buf.String()))
}

// Toggle the block profile rate to/from 100%, unless specific rate is passed in
func registerDebugBlockProfileRate() {
	HTTPHandleFunc("/debug/blockprofilerate", func(w http.ResponseWriter, r *http.Request) {
		if err := acl.CheckAccessHTTP(r, acl.DEBUGGING); err != nil {
			acl.SendError(w, err)
			return
		}

		rate, err := strconv.Atoi(r.FormValue("rate"))
		message := "block profiling enabled"
		if rate < 0 || err != nil {
			// We can't get the current profiling rate
			// from runtime, so we depend on a global var
			if blockProfileRate == 0 {
				rate = 1
			} else {
				rate = 0
				message = "block profiling disabled"
			}
		} else {
			message = fmt.Sprintf("Block profiling rate set to %d", rate)
		}
		blockProfileRate = rate
		runtime.SetBlockProfileRate(rate)
		log.Infof("Set block profile rate to: %d", rate)
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, message)
	})
}

// Toggle the mutex profiling fraction to/from 100%, unless specific fraction is passed in
func registerDebugMutexProfileFraction() {
	HTTPHandleFunc("/debug/mutexprofilefraction", func(w http.ResponseWriter, r *http.Request) {
		if err := acl.CheckAccessHTTP(r, acl.DEBUGGING); err != nil {
			acl.SendError(w, err)
			return
		}

		currentFraction := runtime.SetMutexProfileFraction(-1)
		fraction, err := strconv.Atoi(r.FormValue("fraction"))
		message := "mutex profiling enabled"
		if fraction < 0 || err != nil {
			if currentFraction == 0 {
				fraction = 1
			} else {
				fraction = 0
				message = "mutex profiling disabled"
			}
		} else {
			message = fmt.Sprintf("Mutex profiling set to fraction %d", fraction)
		}
		runtime.SetMutexProfileFraction(fraction)
		log.Infof("Set mutex profiling fraction to: %d", fraction)
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, message)
	})
}

func init() {
	var err error
	hostname, err = os.Hostname()
	if err != nil {
		log.Exitf("os.Hostname: %v", err)
	}
}
