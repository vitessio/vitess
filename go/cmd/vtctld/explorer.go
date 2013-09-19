package main

import (
	"net/http"
	"path"
	"strings"

	log "github.com/golang/glog"
)

// Explorer allows exploring a topology server.
type Explorer interface {
	// HandlePath returns a result (suitable to be passed to a
	// template) appropriate for url, using actionRepo to populate
	// the actions in result.
	HandlePath(actionRepo *ActionRepository, url string) interface{}
}

// HandleExplorer serves explorer under url, using a template named
// templateName.
func HandleExplorer(url, templateName string, explorer Explorer) {
	http.HandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			httpError(w, "cannot parse form: %s", err)
			return
		}
		topoPath := r.URL.Path[strings.Index(r.URL.Path, url):]
		if cleanPath := path.Clean(topoPath); topoPath != cleanPath && topoPath != cleanPath+"/" {
			log.Infof("redirecting to %v", cleanPath)
			http.Redirect(w, r, cleanPath, http.StatusTemporaryRedirect)
			return
		}

		if strings.HasSuffix(topoPath, "/") {
			topoPath = topoPath[:len(topoPath)-1]
		}
		result := explorer.HandlePath(actionRepo, topoPath)
		templateLoader.ServeTemplate(templateName, result, w, r)
	})
}
