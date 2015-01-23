package servenv

import (
	"bytes"
	"fmt"
	"html"
	"html/template"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/acl"
)

var (
	binaryName  = filepath.Base(os.Args[0])
	hostname    string
	serverStart = time.Now()

	statusMu       sync.RWMutex
	statusSections []section
	statusTmpl     = template.Must(reparse(nil))
	statusFuncMap  = make(template.FuncMap)
)

type section struct {
	Banner   string
	Fragment string
	F        func() interface{}
}

// AddStatusFuncs merges the provided functions into the set of
// functions used to render /debug/status. Call this before AddStatusPart
// if your template requires custom functions.
func AddStatusFuncs(fmap template.FuncMap) {
	statusMu.Lock()
	defer statusMu.Unlock()

	for name, fun := range fmap {
		if !strings.HasPrefix(name, "github_com_youtube_vitess_") {
			panic("status func registered without proper prefix, need github_com_youtube_vitess_:" + name)
		}
		if _, ok := statusFuncMap[name]; ok {
			panic("duplicate status func registered: " + name)
		}
		statusFuncMap[name] = fun
	}
}

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

func reparse(sections []section) (*template.Template, error) {
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
	return template.New("").Funcs(statusFuncMap).Parse(buf.String())
}

// AddStatusPart adds a new section to status. frag is used as a
// subtemplate of the template used to render /debug/status, and will
// be executed using the value of invoking f at the time of the
// /debug/status request. frag is parsed and executed with the
// html/template package. Functions registered with AddStatusFuncs
// may be used in the template.
func AddStatusPart(banner, frag string, f func() interface{}) {
	statusMu.Lock()
	defer statusMu.Unlock()

	secs := append(statusSections, section{
		Banner:   banner,
		Fragment: frag,
		F:        f,
	})

	var err error
	statusTmpl, err = reparse(secs)
	if err != nil {
		secs[len(secs)-1] = section{
			Banner:   banner,
			Fragment: "<code>bad status template: {{.}}</code>",
			F:        func() interface{} { return err },
		}
	}
	statusTmpl, _ = reparse(secs)
	statusSections = secs
}

// AddStatusSection registers a function that generates extra
// information for /debug/status. If banner is not empty, it will be
// used as a header before the information. If more complex output
// than a simple string is required use AddStatusPart instead.
func AddStatusSection(banner string, f func() string) {
	AddStatusPart(banner, `{{.}}`, func() interface{} { return f() })
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.DEBUGGING); err != nil {
		acl.SendError(w, err)
		return
	}
	statusMu.Lock()
	defer statusMu.Unlock()

	data := struct {
		Sections   []section
		BinaryName string
		Hostname   string
		StartTime  string
	}{
		Sections:   statusSections,
		BinaryName: binaryName,
		Hostname:   hostname,
		StartTime:  serverStart.Format(time.RFC1123),
	}

	if err := statusTmpl.ExecuteTemplate(w, "status", data); err != nil {
		log.Errorf("servenv: couldn't execute template: %v", err)
	}
}

// StatusURLPath returns the path to the status page.
func StatusURLPath() string {
	return "/debug/status"
}

func init() {
	var err error
	hostname, err = os.Hostname()
	if err != nil {
		log.Fatalf("os.Hostname: %v", err)
	}
	http.HandleFunc("/debug/status", statusHandler)
}
