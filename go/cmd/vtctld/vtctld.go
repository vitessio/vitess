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

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
)

var (
	templateDir = flag.String("templates", "", "directory containing templates")
	debug       = flag.Bool("debug", false, "recompile templates for every request")
)

func init() {
	servenv.RegisterDefaultFlags()
}

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

func link(text, href string) string {
	return fmt.Sprintf("<a href=%q>%v</a>", href, text)
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
		switch len(explorers) {
		case 0:
			return template.HTML(keyspace)
		case 1:
			for _, explorer := range explorers {
				return template.HTML(link(keyspace, explorer.GetKeyspacePath(keyspace)))
			}
		default:
			b := new(bytes.Buffer)
			fmt.Fprintf(b, "%v", keyspace)
			for name, explorer := range explorers {
				fmt.Fprintf(b, "&nbsp;[%v]", link(name, explorer.GetKeyspacePath(keyspace)))
			}
			return template.HTML(b.String())
		}
		panic("unreachable")
	},
	"srv_keyspace": func(cell, keyspace string) template.HTML {
		switch len(explorers) {
		case 0:
			return template.HTML(keyspace)
		case 1:
			for _, explorer := range explorers {
				return template.HTML(link(keyspace, explorer.GetSrvKeyspacePath(cell, keyspace)))
			}
		default:
			b := new(bytes.Buffer)
			fmt.Fprintf(b, "%v", keyspace)
			for name, explorer := range explorers {
				fmt.Fprintf(b, "&nbsp;[%v]", link(name, explorer.GetSrvKeyspacePath(cell, keyspace)))
			}
			return template.HTML(b.String())
		}
		panic("unreachable")
	},
	"shard": func(keyspace, shard string) template.HTML {
		switch len(explorers) {
		case 0:
			return template.HTML(shard)
		case 1:
			for _, explorer := range explorers {
				return template.HTML(link(shard, explorer.GetShardPath(keyspace, shard)))
			}
		default:
			b := new(bytes.Buffer)
			fmt.Fprintf(b, "%v", shard)
			for name, explorer := range explorers {
				fmt.Fprintf(b, `&nbsp;<span class="topo-link">[%v]</span>`, link(name, explorer.GetShardPath(keyspace, shard)))
			}
			return template.HTML(b.String())
		}
		panic("unreachable")
	},
	"srv_shard": func(cell, keyspace, shard string) template.HTML {
		switch len(explorers) {
		case 0:
			return template.HTML(shard)
		case 1:
			for _, explorer := range explorers {
				return template.HTML(link(shard, explorer.GetSrvShardPath(cell, keyspace, shard)))
			}
		default:
			b := new(bytes.Buffer)
			fmt.Fprintf(b, "%v", shard)
			for name, explorer := range explorers {
				fmt.Fprintf(b, `&nbsp;<span class="topo-link">[%v]</span>`, link(name, explorer.GetSrvShardPath(cell, keyspace, shard)))
			}
			return template.HTML(b.String())
		}
		panic("unreachable")
	},
	"tablet": func(alias topo.TabletAlias, shortname string) template.HTML {
		switch len(explorers) {
		case 0:
			return template.HTML(shortname)
		case 1:
			for _, explorer := range explorers {
				return template.HTML(link(shortname, explorer.GetTabletPath(alias)))
			}
		default:
			b := new(bytes.Buffer)
			fmt.Fprintf(b, "%v", shortname)
			for name, explorer := range explorers {
				fmt.Fprintf(b, `&nbsp;<span class="topo-link">[%v]</span>`, link(name, explorer.GetTabletPath(alias)))
			}
			return template.HTML(b.String())
		}
		panic("unreachable")
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
			httpError(w, "error executing template: %v", err)
		}
	}
}

func httpError(w http.ResponseWriter, format string, err error) {
	log.Errorf(format, err)
	http.Error(w, fmt.Sprintf(format, err), http.StatusInternalServerError)
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
		"Serving Graph":   "/serving_graph",
	},
}
var ts topo.Server

func main() {
	flag.Parse()
	servenv.Init()
	defer servenv.Close()
	templateLoader = NewTemplateLoader(*templateDir, dummyTemplate, *debug)

	ts = topo.GetServer()
	defer topo.CloseServers()

	wr := wrangler.New(logutil.NewConsoleLogger(), ts, 30*time.Second, 30*time.Second)

	actionRepo = NewActionRepository()

	// keyspace actions
	actionRepo.RegisterKeyspaceAction("ValidateKeyspace",
		func(wr *wrangler.Wrangler, keyspace string, r *http.Request) (string, error) {
			return "", wr.ValidateKeyspace(keyspace, false)
		})

	actionRepo.RegisterKeyspaceAction("ValidateSchemaKeyspace",
		func(wr *wrangler.Wrangler, keyspace string, r *http.Request) (string, error) {
			return "", wr.ValidateSchemaKeyspace(keyspace, nil, false)
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
			return "", wr.ValidateSchemaShard(keyspace, shard, nil, false)
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
	actionRepo.RegisterTabletAction("RpcPing", "",
		func(wr *wrangler.Wrangler, tabletAlias topo.TabletAlias, r *http.Request) (string, error) {
			return "", wr.ActionInitiator().RpcPing(tabletAlias, 10*time.Second)
		})

	actionRepo.RegisterTabletAction("ScrapTablet", acl.ADMIN,
		func(wr *wrangler.Wrangler, tabletAlias topo.TabletAlias, r *http.Request) (string, error) {
			// refuse to scrap tablets that are not spare
			ti, err := wr.TopoServer().GetTablet(tabletAlias)
			if err != nil {
				return "", err
			}
			if ti.Type != topo.TYPE_SPARE {
				return "", fmt.Errorf("Can only scrap spare tablets")
			}
			actionPath, err := wr.Scrap(tabletAlias, false, false)
			if err != nil {
				return "", err
			}
			return "", wr.WaitForCompletion(actionPath)
		})

	actionRepo.RegisterTabletAction("ScrapTabletForce", acl.ADMIN,
		func(wr *wrangler.Wrangler, tabletAlias topo.TabletAlias, r *http.Request) (string, error) {
			// refuse to scrap tablets that are not spare
			ti, err := wr.TopoServer().GetTablet(tabletAlias)
			if err != nil {
				return "", err
			}
			if ti.Type != topo.TYPE_SPARE {
				return "", fmt.Errorf("Can only scrap spare tablets")
			}
			_, err = wr.Scrap(tabletAlias, true, false)
			return "", err
		})

	actionRepo.RegisterTabletAction("DeleteTablet", acl.ADMIN,
		func(wr *wrangler.Wrangler, tabletAlias topo.TabletAlias, r *http.Request) (string, error) {
			return "", wr.DeleteTablet(tabletAlias)
		})

	// toplevel index
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		templateLoader.ServeTemplate("index.html", indexContent, w, r)
	})

	// keyspace actions
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

	// shard actions
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

	// tablet actions
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

	// topology server
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

	// serving graph
	http.HandleFunc("/serving_graph/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")

		cell := parts[len(parts)-1]
		if cell == "" {
			cells, err := ts.GetKnownCells()
			if err != nil {
				httpError(w, "cannot get known cells: %v", err)
				return
			} else {
				templateLoader.ServeTemplate("serving_graph_cells.html", cells, w, r)
			}
			return
		}

		servingGraph := wr.ServingGraph(cell)
		templateLoader.ServeTemplate("serving_graph.html", servingGraph, w, r)
	})

	// redirects for explorers
	http.HandleFunc("/explorers/redirect", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			httpError(w, "cannot parse form: %s", err)
			return
		}

		var explorer Explorer
		switch len(explorers) {
		case 0:
			http.Error(w, "no explorer configured", http.StatusInternalServerError)
			return
		case 1:
			for _, ex := range explorers {
				explorer = ex
			}
		default:
			explorerName := r.FormValue("explorer")
			var ok bool
			explorer, ok = explorers[explorerName]
			if !ok {
				http.Error(w, "bad explorer name", http.StatusBadRequest)
				return
			}
		}

		var target string
		switch r.FormValue("type") {
		case "keyspace":
			keyspace := r.FormValue("keyspace")
			if keyspace == "" {
				http.Error(w, "keyspace is obligatory for this redirect", http.StatusBadRequest)
				return
			}
			target = explorer.GetKeyspacePath(keyspace)

		case "shard":
			keyspace, shard := r.FormValue("keyspace"), r.FormValue("shard")
			if keyspace == "" || shard == "" {
				http.Error(w, "keyspace and shard are obligatory for this redirect", http.StatusBadRequest)
				return
			}
			target = explorer.GetShardPath(keyspace, shard)

		case "srv_keyspace":
			cell := r.FormValue("cell")
			if cell == "" {
				http.Error(w, "cell is obligatory for this redirect", http.StatusBadRequest)
				return
			}
			keyspace := r.FormValue("keyspace")
			if keyspace == "" {
				http.Error(w, "keyspace is obligatory for this redirect", http.StatusBadRequest)
				return
			}
			target = explorer.GetSrvKeyspacePath(cell, keyspace)

		case "srv_shard":
			cell := r.FormValue("cell")
			if cell == "" {
				http.Error(w, "cell is obligatory for this redirect", http.StatusBadRequest)
				return
			}
			keyspace := r.FormValue("keyspace")
			if keyspace == "" {
				http.Error(w, "keyspace is obligatory for this redirect", http.StatusBadRequest)
				return
			}
			shard := r.FormValue("shard")
			if shard == "" {
				http.Error(w, "shard is obligatory for this redirect", http.StatusBadRequest)
				return
			}
			target = explorer.GetSrvShardPath(cell, keyspace, shard)

		case "srv_type":
			cell := r.FormValue("cell")
			if cell == "" {
				http.Error(w, "cell is obligatory for this redirect", http.StatusBadRequest)
				return
			}
			keyspace := r.FormValue("keyspace")
			if keyspace == "" {
				http.Error(w, "keyspace is obligatory for this redirect", http.StatusBadRequest)
				return
			}
			shard := r.FormValue("shard")
			if shard == "" {
				http.Error(w, "shard is obligatory for this redirect", http.StatusBadRequest)
				return
			}
			tabletType := r.FormValue("tablet_type")
			if tabletType == "" {
				http.Error(w, "tablet_type is obligatory for this redirect", http.StatusBadRequest)
				return
			}
			target = explorer.GetSrvTypePath(cell, keyspace, shard, topo.TabletType(tabletType))

		case "tablet":
			aliasName := r.FormValue("alias")
			if aliasName == "" {
				http.Error(w, "keyspace is obligatory for this redirect", http.StatusBadRequest)
				return
			}
			alias, err := topo.ParseTabletAliasString(aliasName)
			if err != nil {
				http.Error(w, "bad tablet alias", http.StatusBadRequest)
				return
			}
			target = explorer.GetTabletPath(alias)

		case "replication":
			cell := r.FormValue("cell")
			if cell == "" {
				http.Error(w, "cell is obligatory for this redirect", http.StatusBadRequest)
				return
			}
			keyspace := r.FormValue("keyspace")
			if keyspace == "" {
				http.Error(w, "keyspace is obligatory for this redirect", http.StatusBadRequest)
				return
			}
			shard := r.FormValue("shard")
			if shard == "" {
				http.Error(w, "shard is obligatory for this redirect", http.StatusBadRequest)
				return
			}
			target = explorer.GetReplicationSlaves(cell, keyspace, shard)

		default:
			http.Error(w, "bad redirect type", http.StatusBadRequest)
			return
		}
		http.Redirect(w, r, target, http.StatusFound)
	})
	servenv.RunDefault()
}
