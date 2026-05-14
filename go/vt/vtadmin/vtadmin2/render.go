/*
Copyright 2026 The Vitess Authors.

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

package vtadmin2

import (
	"html/template"
	"io/fs"
	"net/http"
	"path/filepath"
	"strings"
)

type (
	templateSet struct {
		templates map[string]*template.Template
	}

	PageData struct {
		Title         string
		Active        string
		ReadOnly      bool
		CSRFToken     string
		Flash         *Flash
		Data          any
		DocumentTitle string
	}

	Flash struct {
		Kind    string
		Message string
	}
)

func parseTemplates() (*templateSet, error) {
	pages, err := fs.Glob(assets, "templates/*.html")
	if err != nil {
		return nil, err
	}

	templates := make(map[string]*template.Template, len(pages))
	for _, page := range pages {
		name := filepath.Base(page)
		if name == "layout.html" {
			continue
		}

		tmpl, err := template.New("").Funcs(template.FuncMap{
			"clusterID":        clusterID,
			"keyspaceName":     keyspaceName,
			"schemaTableCount": schemaTableCount,
			"sortedShardNames": sortedShardNames,
			"tabletAlias":      tabletAlias,
		}).ParseFS(assets, "templates/layout.html", page)
		if err != nil {
			return nil, err
		}
		templates[name] = tmpl
	}

	return &templateSet{templates: templates}, nil
}

func staticFS() fs.FS {
	static, err := fs.Sub(assets, "static")
	if err != nil {
		panic(err)
	}
	return static
}

func (s *Server) render(w http.ResponseWriter, r *http.Request, status int, name string, data PageData) {
	if data.DocumentTitle == "" {
		data.DocumentTitle = s.opts.DocumentTitle
	}
	data.ReadOnly = s.opts.ReadOnly
	if data.CSRFToken == "" {
		data.CSRFToken = csrfToken(w, r)
	}
	if flash := flashFromRequest(w, r); flash != nil {
		data.Flash = flash
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	tmpl := s.templates.templates[strings.TrimPrefix(name, "templates/")]
	if tmpl == nil {
		http.Error(w, "template not found: "+name, http.StatusInternalServerError)
		return
	}
	if status != http.StatusOK {
		w.WriteHeader(status)
	}
	if err := tmpl.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) renderError(w http.ResponseWriter, r *http.Request, status int, title string, err error) {
	s.render(w, r, status, "index.html", PageData{
		Title: title,
		Flash: &Flash{
			Kind:    "error",
			Message: err.Error(),
		},
	})
}

func (s *Server) renderReadOnly(w http.ResponseWriter, r *http.Request) {
	s.render(w, r, http.StatusForbidden, "index.html", PageData{
		Title: "Read-only",
		Flash: &Flash{
			Kind:    "error",
			Message: "vtadmin2 is running in read-only mode",
		},
	})
}
