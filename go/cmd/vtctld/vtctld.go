package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"path"
	"reflect"
	"strings"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/wrangler"
	"code.google.com/p/vitess/go/zk"
)

var (
	port        = flag.Int("port", 8080, "port for the server")
	templateDir = flag.String("templates", "", "directory containing templates")
	debug       = flag.Bool("debug", false, "recompile templates for every request")
)

// FHtmlize writes data to w as debug HTML (using definition lists).
func FHtmlize(w io.Writer, data interface{}) {
	v := reflect.Indirect(reflect.ValueOf(data))
	typ := v.Type()
	switch typ.Kind() {
	case reflect.Struct:
		fmt.Fprintf(w, "<dl class=\"%s\">", typ.Name())
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if field.PkgPath != "" {
				continue
			}
			fmt.Fprintf(w, "<dt>%v</dt>", field.Name)
			fmt.Fprint(w, "<dd>")
			FHtmlize(w, v.Field(i).Interface())
			fmt.Fprint(w, "</dd>")
		}
		fmt.Fprintf(w, "</dl>")
	case reflect.Slice:
		fmt.Fprint(w, "<ul>")
		for i := 0; i < v.Len(); i++ {
			FHtmlize(w, v.Index(i).Interface())
		}
		fmt.Fprint(w, "</ul>")
	case reflect.Map:
		fmt.Fprintf(w, "<dl class=\"map\">")
		for _, k := range v.MapKeys() {
			fmt.Fprint(w, "<dt>")
			FHtmlize(w, k.Interface())
			fmt.Fprint(w, "</dt>")
			fmt.Fprint(w, "<dd>")
			FHtmlize(w, v.MapIndex(k).Interface())
			fmt.Fprint(w, "</dd>")

		}
		fmt.Fprintf(w, "</dl>")
	case reflect.String:
		quoted := fmt.Sprintf("%q", v.Interface())
		printed := quoted[1 : len(quoted)-1]
		if printed == "" {
			printed = "&nbsp;"
		}
		fmt.Fprintf(w, "%s", printed)
	default:
		printed := fmt.Sprintf("%v", v.Interface())
		if printed == "" {
			printed = "&nbsp;"
		}
		fmt.Fprint(w, printed)
	}
}

// Htmlize returns a debug HTML representation of data.
func Htmlize(data interface{}) string {
	b := new(bytes.Buffer)
	FHtmlize(b, data)
	return b.String()
}

var funcMap = template.FuncMap{
	"htmlize": func(o interface{}) template.HTML {
		return template.HTML(Htmlize(o))
	},
	"hasprefix": strings.HasPrefix,
	"intequal": func(left, right int) bool {
		return left == right
	},
	"breadcrumbs": func(zkPath string) template.HTML {
		parts := strings.Split(zkPath, "/")
		paths := make([]string, len(parts))
		for i, part := range parts {
			if i == 0 {
				paths[i] = "/"
				continue
			}
			paths[i] = path.Join(paths[i-1], part)
		}
		b := new(bytes.Buffer)
		for i, part := range parts[1 : len(parts)-1] {
			fmt.Fprintf(b, "/ <a href=\"%v\">%v</a>&nbsp;", paths[i+1], part)
		}
		fmt.Fprintf(b, "/ "+parts[len(parts)-1])
		return template.HTML(b.String())
	},
}

var dummyTemplate = template.Must(template.New("dummy").Funcs(funcMap).Parse(`
<!DOCTYPE HTML>
<html>
<head>
<style>
    html {
      font-family: monospace;
    }
    dd {
      margin-left: 2em;
    }
</style>
</head>
<body>
  {{ htmlize . }}
</body>
</html>
`))

type TemplateLoader struct {
	Directory string
	usesDummy bool
	template  *template.Template
}

func (loader *TemplateLoader) compile() (*template.Template, error) {
	return template.New("main").Funcs(funcMap).ParseGlob(path.Join(loader.Directory, "[a-z]*"))
}

func (loader *TemplateLoader) makeErrorTemplate(errorMessage string) *template.Template {
	return template.Must(template.New("error").Parse(fmt.Sprintf("Error in template: %s", errorMessage)))
}

// NewTemplateLoader returns a template loader with templates from
// directory. If directory is "", fallbackTemplate will be used
// (regardless of the wanted template name). If debug is true,
// templates will be recompiled each time.
func NewTemplateLoader(directory string, fallbackTemplate *template.Template, debug bool) *TemplateLoader {
	loader := &TemplateLoader{Directory: directory}
	if directory == "" {
		loader.usesDummy = true
		loader.template = fallbackTemplate
		return loader
	}
	if !debug {
		tmpl, err := loader.compile()
		if err != nil {
			panic(err)
		}
		loader.template = tmpl
	}
	return loader
}

func (loader *TemplateLoader) Lookup(name string) (*template.Template, error) {
	if loader.usesDummy {
		return loader.template, nil
	}
	var err error
	source := loader.template
	if loader.template == nil {
		source, err = loader.compile()
		if err != nil {
			return nil, err
		}
	}
	tmpl := source.Lookup(name)
	if tmpl == nil {
		err := fmt.Errorf("template %v not available", name)
		return nil, err
	}
	return tmpl, nil
}

// ServeTemplate executes the named template passing data into it. If
// the format GET parameter is equal to "json", serves data as JSON
// instead.
func (tl *TemplateLoader) ServeTemplate(templateName string, data interface{}, w http.ResponseWriter, r *http.Request) {
	switch r.URL.Query().Get("format") {
	case "json":
		j, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			httpError(w, "JSON error%s", err)
			return
		}
		w.Write(j)
	default:
		tmpl, err := tl.Lookup(templateName)
		if err != nil {
			httpError(w, "error in template loader: %v", err)
			return
		}
		if err := tmpl.Execute(w, data); err != nil {
			httpError(w, "error executing template", err)
		}
	}
}

func httpError(w http.ResponseWriter, format string, err error) {
	relog.Error(format, err)
	http.Error(w, fmt.Sprintf(format, err), http.StatusInternalServerError)
}

type DbTopologyResult struct {
	Topology *wrangler.Topology
	Error    string
}

type ZkResult struct {
	Path     string
	Data     string
	Children []string
	Error    string
}

func main() {
	flag.Parse()

	templateLoader := NewTemplateLoader(*templateDir, dummyTemplate, *debug)
	zconn := zk.NewMetaConn(false)
	defer zconn.Close()
	wr := wrangler.NewWrangler(zconn, 30*time.Second, 30*time.Second)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		tmpl, err := templateLoader.Lookup("index.html")
		if err != nil {
			httpError(w, "error in template loader: %v", err)
			return
		}
		if err := tmpl.Execute(w, nil); err != nil {
			httpError(w, "error executing template", err)
		}

	})
	http.HandleFunc("/dbtopo", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			httpError(w, "cannot parse form: %s", err)
			return
		}
		result := DbTopologyResult{}
		topology, err := wr.DbTopology()
		if err != nil {
			result.Error = err.Error()
		} else {
			result.Topology = topology
		}
		templateLoader.ServeTemplate("dbtopo.html", result, w, r)
	})
	http.HandleFunc("/zk/", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			httpError(w, "cannot parse form: %s", err)
			return
		}
		zkPath := r.URL.Path[strings.Index(r.URL.Path, "/zk"):]

		if cleanPath := path.Clean(zkPath); zkPath != cleanPath && zkPath != cleanPath+"/" {
			relog.Info("redirecting to %v", cleanPath)
			http.Redirect(w, r, cleanPath, http.StatusTemporaryRedirect)
			return
		}

		if strings.HasSuffix(zkPath, "/") {
			zkPath = zkPath[:len(zkPath)-1]
		}

		result := ZkResult{Path: zkPath}

		if zkPath == "/zk" {
			cells, err := zk.ResolveWildcards(zconn, []string{"/zk/*"})
			if err != nil {
				httpError(w, "zk error: %v", err)
				return
			}
			for i, cell := range cells {
				cells[i] = cell[4:] // cut off "/zk/"
			}
			result.Children = cells
		} else {

			if data, _, err := zconn.Get(zkPath); err != nil {
				result.Error = err.Error()
			} else {
				result.Data = data
				if children, _, err := zconn.Children(zkPath); err != nil {
					result.Error = err.Error()
				} else {
					result.Children = children
				}

			}

		}
		templateLoader.ServeTemplate("zk.html", result, w, r)
	})
	relog.Fatal("%s", http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}
