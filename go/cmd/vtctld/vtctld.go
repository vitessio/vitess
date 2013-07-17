package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"strings"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/topo"
	"code.google.com/p/vitess/go/vt/wrangler"
	"code.google.com/p/vitess/go/vt/zktopo" // FIXME(alainjobart) to be removed
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
			fmt.Fprintf(b, "/<a href=\"%v\">%v</a>", paths[i+1], part)
		}
		fmt.Fprintf(b, "/"+parts[len(parts)-1])
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

type ActionResult struct {
	Name       string
	Parameters string
	Output     string
	Error      bool
}

func (ar *ActionResult) error(text string) {
	ar.Error = true
	ar.Output = text
}

// actionMethod is a function that performs some action on a Zookeeper
// node. It should return a message for the user or an empty string in
// case there's nothing interesting to be communicated.
type actionMethod func(wr *wrangler.Wrangler, zkPath string, r *http.Request) (output string, err error)

// ActionRepository is a repository of actions that can be performed
// on a Zookeeper node.
type ActionRepository struct {
	actions        map[string]actionMethod
	actionsForPath map[string]map[string]actionMethod
	wrangler       *wrangler.Wrangler
}

func NewActionRepository(wr *wrangler.Wrangler) *ActionRepository {
	return &ActionRepository{
		actions:        make(map[string]actionMethod),
		actionsForPath: make(map[string]map[string]actionMethod),
		wrangler:       wr}

}

// Register registers a named action that will be possible to apply on
// Zookeeper nodes whose path matches matchingPath.
func (ar *ActionRepository) Register(matchingPath string, name string, method actionMethod) {
	ar.actions[name] = method
	_, ok := ar.actionsForPath[matchingPath]
	if !ok {
		ar.actionsForPath[matchingPath] = make(map[string]actionMethod)
	}
	ar.actionsForPath[matchingPath][name] = method
}

func (ar *ActionRepository) Apply(actionName string, zkPath string, r *http.Request) *ActionResult {
	result := &ActionResult{Name: actionName, Parameters: zkPath}

	action, ok := ar.actions[actionName]
	if !ok {
		result.error("Unknown action")
		return result
	}
	ar.wrangler.ResetActionTimeout(wrangler.DefaultActionTimeout)
	output, err := action(ar.wrangler, zkPath, r)
	if err != nil {
		result.error(err.Error())
		return result
	}
	result.Output = output
	return result
}

// PopulateAvailableActions populates result with actions that can be
// performed on its node.
func (ar ActionRepository) PopulateAvailableActions(result *ZkResult) {
	for matchingPath, actions := range ar.actionsForPath {
		if m, _ := path.Match(matchingPath, result.Path); m {
			for name, _ := range actions {
				values := url.Values{}
				values.Set("action", name)
				values.Set("zkpath", result.Path)
				result.Actions[name] = template.URL("/actions?" + values.Encode())
			}
		}
	}
}

type DbTopologyResult struct {
	Topology *wrangler.Topology
	Error    string
}

type ZkResult struct {
	Path     string
	Data     string
	Children []string
	Actions  map[string]template.URL
	Error    string
}

func NewZkResult(zkPath string) *ZkResult {
	return &ZkResult{Actions: make(map[string]template.URL), Path: zkPath}
}

func main() {
	flag.Parse()

	templateLoader := NewTemplateLoader(*templateDir, dummyTemplate, *debug)

	ts := topo.GetServer()
	defer topo.CloseServers()

	wr := wrangler.New(ts, 30*time.Second, 30*time.Second)

	actionRepo := NewActionRepository(wr)

	const (
		keyspacePath = "/zk/global/vt/keyspaces/*"
		shardPath    = "/zk/global/vt/keyspaces/*/shards/*"
		tabletPath   = "/zk/*/vt/tablets/*"
	)

	actionRepo.Register(keyspacePath, "ValidateKeyspace",
		func(wr *wrangler.Wrangler, zkPath string, r *http.Request) (string, error) {
			return "", wr.ValidateKeyspace(path.Base(zkPath), false)
		})

	actionRepo.Register(keyspacePath, "ValidateSchemaKeyspace",
		func(wr *wrangler.Wrangler, zkPath string, r *http.Request) (string, error) {
			return "", wr.ValidateSchemaKeyspace(path.Base(zkPath), false)
		})

	actionRepo.Register(keyspacePath, "ValidateVersionKeyspace",
		func(wr *wrangler.Wrangler, zkPath string, r *http.Request) (string, error) {
			return "", wr.ValidateVersionKeyspace(path.Base(zkPath))
		})

	actionRepo.Register(keyspacePath, "ValidatePermissionsKeyspace",
		func(wr *wrangler.Wrangler, zkPath string, r *http.Request) (string, error) {
			return "", wr.ValidatePermissionsKeyspace(path.Base(zkPath))
		})

	actionRepo.Register(shardPath, "ValidateShard",
		func(wr *wrangler.Wrangler, zkPath string, r *http.Request) (string, error) {
			zkPathParts := strings.Split(zkPath, "/")
			return "", wr.ValidateShard(zkPathParts[5], zkPathParts[7], false)
		})

	actionRepo.Register(shardPath, "ValidateSchemaShard",
		func(wr *wrangler.Wrangler, zkPath string, r *http.Request) (string, error) {
			zkPathParts := strings.Split(zkPath, "/")
			return "", wr.ValidateSchemaShard(zkPathParts[5], zkPathParts[7], false)
		})

	actionRepo.Register(shardPath, "ValidateVersionShard",
		func(wr *wrangler.Wrangler, zkPath string, r *http.Request) (string, error) {
			zkPathParts := strings.Split(zkPath, "/")
			return "", wr.ValidateVersionShard(zkPathParts[5], zkPathParts[7])
		})

	actionRepo.Register(shardPath, "ValidatePermissionsShard",
		func(wr *wrangler.Wrangler, zkPath string, r *http.Request) (string, error) {
			zkPathParts := strings.Split(zkPath, "/")
			return "", wr.ValidatePermissionsShard(zkPathParts[5], zkPathParts[7])
		})

	actionRepo.Register(tabletPath, "RpcPing",
		func(wr *wrangler.Wrangler, zkPath string, r *http.Request) (string, error) {
			zkPathParts := strings.Split(zkPath, "/")
			alias, err := topo.ParseTabletAliasString(zkPathParts[2] + "-" + zkPathParts[5])
			if err != nil {
				return "", err
			}
			return "", wr.ActionInitiator().RpcPing(alias, 10*time.Second)
		})

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
	http.HandleFunc("/actions", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			httpError(w, "cannot parse form: %s", err)
			return
		}
		action := r.FormValue("action")
		if action == "" {
			http.Error(w, "no action provided", http.StatusBadRequest)
			return
		}

		zkPath := r.FormValue("zkpath")
		if zkPath == "" {
			http.Error(w, "no zookeeper path provided", http.StatusBadRequest)
			return
		}
		result := actionRepo.Apply(action, zkPath, r)

		templateLoader.ServeTemplate("action.html", result, w, r)
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
		zkTopoServ := topo.GetServerByName("zookeeper")
		if zkTopoServ == nil {
			http.Error(w, "can only look at zk with zktopo.Server", http.StatusInternalServerError)
			return
		}
		zconn := zkTopoServ.(*zktopo.Server).GetZConn()

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

		result := NewZkResult(zkPath)

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
				actionRepo.PopulateAvailableActions(result)
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
