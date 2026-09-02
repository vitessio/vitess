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
	"bytes"
	jsonv2 "encoding/json/v2"
	"html/template"
	"io/fs"
	"net/http"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
)

type (
	templateSet struct {
		templates map[string]*template.Template
	}

	PageData struct {
		Title          string
		Active         string
		ReadOnly       bool
		CSRFToken      string
		NeedsCSRF      bool
		Flash          *Flash
		Data           any
		DocumentTitle  string
		Theme          string
		RefreshSeconds int
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
			"clusterID":          clusterID,
			"externalURL":        externalURL,
			"keyspaceName":       keyspaceName,
			"pathEscape":         pathEscape,
			"protoJSON":          protoJSON,
			"schemaTableCount":   schemaTableCount,
			"shardActionPath":    shardActionPath,
			"keyspaceActionPath": keyspaceActionPath,
			"sortedShardNames":   sortedShardNames,
			"tabletAlias":        tabletAlias,
			"urlQueryEscape":     urlQueryEscape,
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

// refreshablePagePrefixes are the volatile pages for which auto-refresh via
// ?refresh=N is honored. Refreshing expensive pages (e.g. topology) or every
// page by default would hammer the API.
var refreshablePagePrefixes = []string{
	"/workflow",
	"/migrations",
	"/transactions",
}

// refreshSecondsFromQuery parses the ?refresh=N query parameter on pages
// where auto-refresh makes sense, bounded to [10, 60] seconds so aggressive
// polling cannot hammer the API.
func refreshSecondsFromQuery(r *http.Request) int {
	refresh := queryValue(r, "refresh")
	if refresh == "" {
		return 0
	}
	n, err := strconv.Atoi(refresh)
	if err != nil || n <= 0 {
		return 0
	}
	if n > 60 {
		n = 60
	}
	if n < 10 {
		n = 10
	}
	for _, prefix := range refreshablePagePrefixes {
		if strings.HasPrefix(r.URL.Path, prefix) {
			return n
		}
	}
	return 0
}

func (s *Server) render(w http.ResponseWriter, r *http.Request, status int, name string, data PageData) {
	if data.DocumentTitle == "" {
		data.DocumentTitle = s.opts.DocumentTitle
	}
	data.ReadOnly = s.opts.ReadOnly
	data.RefreshSeconds = refreshSecondsFromQuery(r)
	// Respect the user's saved theme, defaulting to system preference.
	data.Theme = "system"
	if cookie, err := r.Cookie(themeCookieName); err == nil && validThemes[cookie.Value] {
		data.Theme = cookie.Value
	}
	if data.NeedsCSRF && data.CSRFToken == "" {
		data.CSRFToken = s.csrfToken(w, r)
	}
	if data.Flash == nil {
		if flash := s.flashFromRequest(w, r); flash != nil {
			data.Flash = flash
		}
	} else {
		s.clearFlash(w, r)
	}

	tmpl := s.templates.templates[strings.TrimPrefix(name, "templates/")]
	if tmpl == nil {
		http.Error(w, "template not found: "+name, http.StatusInternalServerError)
		return
	}

	// Render into a buffer first so template failures produce a clean 500
	// rather than a partial page followed by an error.
	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, name, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if s.opts.EnableDebugJSON && queryValue(r, "format") == "json" {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(protoJSONAny(data.Data)))
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}

// protoJSONAny serializes a proto message, or a slice of proto messages, to
// JSON using protoJSON per element. Non-proto values fall back to
// encoding/json/v2.
func protoJSONAny(v any) string {
	if msg, ok := v.(proto.Message); ok {
		return protoJSON(msg)
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		b, err := jsonv2.Marshal(v)
		if err != nil {
			return "null"
		}
		return string(b)
	}

	parts := make([]string, 0, rv.Len())
	for i := range rv.Len() {
		elem := rv.Index(i)
		if msg, ok := elem.Interface().(proto.Message); ok {
			parts = append(parts, protoJSON(msg))
			continue
		}
		b, err := jsonv2.Marshal(elem.Interface())
		if err != nil {
			parts = append(parts, "null")
			continue
		}
		parts = append(parts, string(b))
	}
	return "[" + strings.Join(parts, ",") + "]"
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
