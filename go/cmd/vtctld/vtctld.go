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

	log "github.com/golang/glog"
	_ "github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
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

// Plugins need to overwrite:
//   keyspace(keyspace)
//   shard(keyspace, shard)
// if they want to create links on these guys
var funcMap = template.FuncMap{
	"htmlize": func(o interface{}) template.HTML {
		return template.HTML(Htmlize(o))
	},
	"hasprefix": strings.HasPrefix,
	"intequal": func(left, right int) bool {
		return left == right
	},
	"breadcrumbs": func(fullPath string) template.HTML {
		parts := strings.Split(fullPath, "/")
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
	"keyspace": func(keyspace string) template.HTML {
		return template.HTML(keyspace)
	},
	"shard": func(keyspace, shard string) template.HTML {
		return template.HTML(shard)
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
	log.Errorf(format, err)
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

// action{Keyspace,Shard,Tablet}Method is a function that performs
// some action on a Topology object. It should return a message for
// the user or an empty string in case there's nothing interesting to
// be communicated.
type actionKeyspaceMethod func(wr *wrangler.Wrangler, keyspace string, r *http.Request) (output string, err error)

type actionShardMethod func(wr *wrangler.Wrangler, keyspace, shard string, r *http.Request) (output string, err error)

type actionTabletMethod func(wr *wrangler.Wrangler, tabletAlias topo.TabletAlias, r *http.Request) (output string, err error)

// ActionRepository is a repository of actions that can be performed
// on a {Keyspace,Shard,Tablet}.
type ActionRepository struct {
	keyspaceActions map[string]actionKeyspaceMethod
	shardActions    map[string]actionShardMethod
	tabletActions   map[string]actionTabletMethod
	wr              *wrangler.Wrangler
}

func NewActionRepository(wr *wrangler.Wrangler) *ActionRepository {
	return &ActionRepository{
		keyspaceActions: make(map[string]actionKeyspaceMethod),
		shardActions:    make(map[string]actionShardMethod),
		tabletActions:   make(map[string]actionTabletMethod),
		wr:              wr}

}

func (ar *ActionRepository) RegisterKeyspaceAction(name string, method actionKeyspaceMethod) {
	ar.keyspaceActions[name] = method
}

func (ar *ActionRepository) RegisterShardAction(name string, method actionShardMethod) {
	ar.shardActions[name] = method
}

func (ar *ActionRepository) RegisterTabletAction(name string, method actionTabletMethod) {
	ar.tabletActions[name] = method
}

func (ar *ActionRepository) ApplyKeyspaceAction(actionName, keyspace string, r *http.Request) *ActionResult {
	result := &ActionResult{Name: actionName, Parameters: keyspace}

	action, ok := ar.keyspaceActions[actionName]
	if !ok {
		result.error("Unknown keyspace action")
		return result
	}
	ar.wr.ResetActionTimeout(wrangler.DefaultActionTimeout)
	output, err := action(ar.wr, keyspace, r)
	if err != nil {
		result.error(err.Error())
		return result
	}
	result.Output = output
	return result
}

func (ar *ActionRepository) ApplyShardAction(actionName, keyspace, shard string, r *http.Request) *ActionResult {
	result := &ActionResult{Name: actionName, Parameters: keyspace + "/" + shard}

	action, ok := ar.shardActions[actionName]
	if !ok {
		result.error("Unknown shard action")
		return result
	}
	ar.wr.ResetActionTimeout(wrangler.DefaultActionTimeout)
	output, err := action(ar.wr, keyspace, shard, r)
	if err != nil {
		result.error(err.Error())
		return result
	}
	result.Output = output
	return result
}

func (ar *ActionRepository) ApplyTabletAction(actionName string, tabletAlias topo.TabletAlias, r *http.Request) *ActionResult {
	result := &ActionResult{Name: actionName, Parameters: tabletAlias.String()}

	action, ok := ar.tabletActions[actionName]
	if !ok {
		result.error("Unknown tablet action")
		return result
	}
	ar.wr.ResetActionTimeout(wrangler.DefaultActionTimeout)
	output, err := action(ar.wr, tabletAlias, r)
	if err != nil {
		result.error(err.Error())
		return result
	}
	result.Output = output
	return result
}

// Populate{Keyspace,Shard,Tablet}Actions populates result with
// actions that can be performed on its node.
func (ar ActionRepository) PopulateKeyspaceActions(actions map[string]template.URL, keyspace string) {
	for name, _ := range ar.keyspaceActions {
		values := url.Values{}
		values.Set("action", name)
		values.Set("keyspace", keyspace)
		actions[name] = template.URL("/keyspace_actions?" + values.Encode())
	}
}

func (ar ActionRepository) PopulateShardActions(actions map[string]template.URL, keyspace, shard string) {
	for name, _ := range ar.shardActions {
		values := url.Values{}
		values.Set("action", name)
		values.Set("keyspace", keyspace)
		values.Set("shard", shard)
		actions[name] = template.URL("/shard_actions?" + values.Encode())
	}
}

func (ar ActionRepository) PopulateTabletActions(actions map[string]template.URL, alias string) {
	for name, _ := range ar.tabletActions {
		values := url.Values{}
		values.Set("action", name)
		values.Set("alias", alias)
		actions[name] = template.URL("/tablet_actions?" + values.Encode())
	}
}

type DbTopologyResult struct {
	Topology *wrangler.Topology
	Error    string
}

type IndexContent struct {
	// maps a name to a linked URL
	ToplevelLinks map[string]string
}

// used at runtime by plug-ins
var templateLoader *TemplateLoader
var actionRepo *ActionRepository
var indexContent = IndexContent{
	ToplevelLinks: map[string]string{
		"DbTopology Tool": "/dbtopo",
	},
}

func main() {
	flag.Parse()
	servenv.Init()
	defer servenv.Close()
	templateLoader = NewTemplateLoader(*templateDir, dummyTemplate, *debug)

	ts := topo.GetServer()
	defer topo.CloseServers()

	wr := wrangler.New(ts, 30*time.Second, 30*time.Second)

	actionRepo = NewActionRepository(wr)

	// keyspace actions
	actionRepo.RegisterKeyspaceAction("ValidateKeyspace",
		func(wr *wrangler.Wrangler, keyspace string, r *http.Request) (string, error) {
			return "", wr.ValidateKeyspace(keyspace, false)
		})

	actionRepo.RegisterKeyspaceAction("ValidateSchemaKeyspace",
		func(wr *wrangler.Wrangler, keyspace string, r *http.Request) (string, error) {
			return "", wr.ValidateSchemaKeyspace(keyspace, false)
		})

	actionRepo.RegisterKeyspaceAction("ValidateVersionKeyspace",
		func(wr *wrangler.Wrangler, keyspace string, r *http.Request) (string, error) {
			return "", wr.ValidateVersionKeyspace(keyspace)
		})

	actionRepo.RegisterKeyspaceAction("ValidatePermissionsKeyspace",
		func(wr *wrangler.Wrangler, keyspace string, r *http.Request) (string, error) {
			return "", wr.ValidatePermissionsKeyspace(keyspace)
		})

	// shard actions
	actionRepo.RegisterShardAction("ValidateShard",
		func(wr *wrangler.Wrangler, keyspace, shard string, r *http.Request) (string, error) {
			return "", wr.ValidateShard(keyspace, shard, false)
		})

	actionRepo.RegisterShardAction("ValidateSchemaShard",
		func(wr *wrangler.Wrangler, keyspace, shard string, r *http.Request) (string, error) {
			return "", wr.ValidateSchemaShard(keyspace, shard, false)
		})

	actionRepo.RegisterShardAction("ValidateVersionShard",
		func(wr *wrangler.Wrangler, keyspace, shard string, r *http.Request) (string, error) {
			return "", wr.ValidateVersionShard(keyspace, shard)
		})

	actionRepo.RegisterShardAction("ValidatePermissionsShard",
		func(wr *wrangler.Wrangler, keyspace, shard string, r *http.Request) (string, error) {
			return "", wr.ValidatePermissionsShard(keyspace, shard)
		})

	// tablet actions
	actionRepo.RegisterTabletAction("RpcPing",
		func(wr *wrangler.Wrangler, tabletAlias topo.TabletAlias, r *http.Request) (string, error) {
			return "", wr.ActionInitiator().RpcPing(tabletAlias, 10*time.Second)
		})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		templateLoader.ServeTemplate("index.html", indexContent, w, r)
	})
	http.HandleFunc("/keyspace_actions", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			httpError(w, "cannot parse form: %s", err)
			return
		}
		action := r.FormValue("action")
		if action == "" {
			http.Error(w, "no action provided", http.StatusBadRequest)
			return
		}

		keyspace := r.FormValue("keyspace")
		if keyspace == "" {
			http.Error(w, "no keyspace provided", http.StatusBadRequest)
			return
		}
		result := actionRepo.ApplyKeyspaceAction(action, keyspace, r)

		templateLoader.ServeTemplate("action.html", result, w, r)
	})
	http.HandleFunc("/shard_actions", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			httpError(w, "cannot parse form: %s", err)
			return
		}
		action := r.FormValue("action")
		if action == "" {
			http.Error(w, "no action provided", http.StatusBadRequest)
			return
		}

		keyspace := r.FormValue("keyspace")
		if keyspace == "" {
			http.Error(w, "no keyspace provided", http.StatusBadRequest)
			return
		}
		shard := r.FormValue("shard")
		if shard == "" {
			http.Error(w, "no shard provided", http.StatusBadRequest)
			return
		}
		result := actionRepo.ApplyShardAction(action, keyspace, shard, r)

		templateLoader.ServeTemplate("action.html", result, w, r)
	})
	http.HandleFunc("/tablet_actions", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			httpError(w, "cannot parse form: %s", err)
			return
		}
		action := r.FormValue("action")
		if action == "" {
			http.Error(w, "no action provided", http.StatusBadRequest)
			return
		}

		alias := r.FormValue("alias")
		if alias == "" {
			http.Error(w, "no alias provided", http.StatusBadRequest)
			return
		}
		tabletAlias, err := topo.ParseTabletAliasString(alias)
		if err != nil {
			http.Error(w, "bad alias provided", http.StatusBadRequest)
			return
		}
		result := actionRepo.ApplyTabletAction(action, tabletAlias, r)

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
	servenv.Run(*port)
}
