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
	"net/http"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	vthandlers "vitess.io/vitess/go/vt/vtadmin/http/handlers"
	"vitess.io/vitess/go/vt/vtadmin/rbac"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	Options struct {
		Addr            string
		ReadOnly        bool
		DocumentTitle   string
		EnableDebugJSON bool
		Authenticator   rbac.Authenticator
	}

	Server struct {
		api       vtadminpb.VTAdminServer
		opts      Options
		templates *templateSet
		router    *http.ServeMux
		handler   http.Handler
	}
)

func NewServer(api vtadminpb.VTAdminServer, opts Options) (*Server, error) {
	if api == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "vtadmin2 requires a VTAdmin server")
	}

	if opts.DocumentTitle == "" {
		opts.DocumentTitle = "VTAdmin2"
	}

	tmpl, err := parseTemplates()
	if err != nil {
		return nil, err
	}

	s := &Server{
		api:       api,
		opts:      opts,
		templates: tmpl,
		router:    http.NewServeMux(),
	}
	s.routes()
	s.handler = s.router
	return s, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.handleFunc("GET /{$}", s.redirectRoot)
	s.handleFunc("GET /clusters", s.clusters)
	s.handleFunc("GET /vtgates", s.gates)
	s.handleFunc("GET /vtctlds", s.vtctlds)
	s.handleFunc("GET /keyspaces", s.keyspaces)
	s.handleFunc("GET /keyspaces/create", s.createKeyspaceForm)
	s.handleFunc("POST /keyspaces/create", s.createKeyspace)
	s.handleFunc("GET /keyspace/{cluster_id}/{name}", s.keyspace)
	s.handleFunc("GET /topology", s.topologyPath)
	s.handleFunc("GET /shards", s.shards)
	s.handleFunc("GET /tablets", s.tablets)
	s.handleFunc("GET /tablet/{cluster_id}/{alias}", s.tabletDetail)
	s.handleFunc("GET /tablet/{cluster_id}/{alias}/full_status", s.tabletFullStatus)
	s.handleFunc("GET /schemas", s.schemas)
	s.handleFunc("GET /vschemas", s.vschemas)
	s.handleFunc("GET /vschema/{cluster_id}/{keyspace}", s.vschema)
	s.handleFunc("GET /srvkeyspaces", s.srvKeyspaces)
	s.handleFunc("GET /srvvschemas", s.srvVSchemas)
	s.handleFunc("GET /cells", s.cells)
	s.handleFunc("GET /cells_aliases", s.cellsAliases)
	s.handleFunc("GET /backups", s.backups)
	s.handleFunc("GET /workflows", s.workflows)
	s.handleFunc("GET /migrations", s.schemaMigrations)
	s.handleFunc("GET /transactions", s.transactions)
	s.handleFunc("GET /vtexplain", s.vtExplain)
	s.handleFunc("GET /vexplain", s.vExplain)
	s.handleFunc("GET /transaction/{cluster_id}/{dtid}/info", s.transactionInfo)
	s.handleFunc("GET /workflow/{cluster_id}/{keyspace}/{name}", s.workflow)
	s.handleFunc("GET /workflow/{cluster_id}/{keyspace}/{name}/status", s.workflowStatus)
	s.handleFunc("GET /vdiff/{cluster_id}/show", s.vdiffShow)
	s.router.HandleFunc("/static", http.NotFound)
	s.router.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS()))))
}

func (s *Server) handleFunc(pattern string, handler http.HandlerFunc) {
	s.handle(pattern, handler)
}

func (s *Server) handle(pattern string, handler http.Handler) {
	if s.opts.Authenticator != nil {
		handler = vthandlers.NewAuthenticationHandler(s.opts.Authenticator)(handler)
	}
	s.router.Handle(pattern, handler)
}

func (s *Server) redirectRoot(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/clusters", http.StatusSeeOther)
}
