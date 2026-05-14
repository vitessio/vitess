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
	s.router.HandleFunc("HEAD /{$}", methodNotAllowed)
	s.handleFunc("GET /clusters", s.clusters)
	s.router.HandleFunc("HEAD /clusters", methodNotAllowed)
	s.handleFunc("GET /gates", s.gates)
	s.router.HandleFunc("HEAD /gates", methodNotAllowed)
	s.handleFunc("GET /vtctlds", s.vtctlds)
	s.router.HandleFunc("HEAD /vtctlds", methodNotAllowed)
	s.handleFunc("GET /keyspaces", s.keyspaces)
	s.router.HandleFunc("HEAD /keyspaces", methodNotAllowed)
	s.handleFunc("GET /keyspaces/create", s.createKeyspaceForm)
	s.router.HandleFunc("HEAD /keyspaces/create", methodNotAllowed)
	s.handleFunc("POST /keyspaces/create", s.createKeyspace)
	s.handleFunc("GET /keyspace/{cluster_id}/{name}", s.keyspace)
	s.router.HandleFunc("HEAD /keyspace/{cluster_id}/{name}", methodNotAllowed)
	s.handleFunc("GET /topology", s.topologyPath)
	s.router.HandleFunc("HEAD /topology", methodNotAllowed)
	s.handleFunc("GET /shards", s.shards)
	s.router.HandleFunc("HEAD /shards", methodNotAllowed)
	s.handleFunc("GET /tablets", s.tablets)
	s.router.HandleFunc("HEAD /tablets", methodNotAllowed)
	s.handleFunc("GET /tablet/{cluster_id}/{alias}", s.tabletDetail)
	s.router.HandleFunc("HEAD /tablet/{cluster_id}/{alias}", methodNotAllowed)
	s.handleFunc("GET /tablet/{cluster_id}/{alias}/full_status", s.tabletFullStatus)
	s.router.HandleFunc("HEAD /tablet/{cluster_id}/{alias}/full_status", methodNotAllowed)
	s.handleFunc("GET /schemas", s.schemas)
	s.router.HandleFunc("HEAD /schemas", methodNotAllowed)
	s.handleFunc("GET /vschemas", s.vschemas)
	s.router.HandleFunc("HEAD /vschemas", methodNotAllowed)
	s.handleFunc("GET /vschema/{cluster_id}/{keyspace}", s.vschema)
	s.router.HandleFunc("HEAD /vschema/{cluster_id}/{keyspace}", methodNotAllowed)
	s.handleFunc("GET /srvkeyspaces", s.srvKeyspaces)
	s.router.HandleFunc("HEAD /srvkeyspaces", methodNotAllowed)
	s.handleFunc("GET /srvvschemas", s.srvVSchemas)
	s.router.HandleFunc("HEAD /srvvschemas", methodNotAllowed)
	s.handleFunc("GET /cells", s.cells)
	s.router.HandleFunc("HEAD /cells", methodNotAllowed)
	s.handleFunc("GET /cells_aliases", s.cellsAliases)
	s.router.HandleFunc("HEAD /cells_aliases", methodNotAllowed)
	s.handleFunc("GET /backups", s.backups)
	s.router.HandleFunc("HEAD /backups", methodNotAllowed)
	s.handleFunc("GET /workflows", s.workflows)
	s.router.HandleFunc("HEAD /workflows", methodNotAllowed)
	s.handleFunc("GET /migrations", s.schemaMigrations)
	s.router.HandleFunc("HEAD /migrations", methodNotAllowed)
	s.handleFunc("GET /transactions", s.transactions)
	s.router.HandleFunc("HEAD /transactions", methodNotAllowed)
	s.handleFunc("GET /vtexplain", s.vtExplain)
	s.router.HandleFunc("HEAD /vtexplain", methodNotAllowed)
	s.handleFunc("GET /vexplain", s.vExplain)
	s.router.HandleFunc("HEAD /vexplain", methodNotAllowed)
	s.handleFunc("GET /transaction/{cluster_id}/{dtid}/info", s.transactionInfo)
	s.router.HandleFunc("HEAD /transaction/{cluster_id}/{dtid}/info", methodNotAllowed)
	s.handleFunc("GET /workflow/{cluster_id}/{keyspace}/{name}", s.workflow)
	s.router.HandleFunc("HEAD /workflow/{cluster_id}/{keyspace}/{name}", methodNotAllowed)
	s.handleFunc("GET /workflow/{cluster_id}/{keyspace}/{name}/status", s.workflowStatus)
	s.router.HandleFunc("HEAD /workflow/{cluster_id}/{keyspace}/{name}/status", methodNotAllowed)
	s.handleFunc("GET /vdiff/{cluster_id}/show", s.vdiffShow)
	s.router.HandleFunc("HEAD /vdiff/{cluster_id}/show", methodNotAllowed)
	s.router.HandleFunc("/static", http.NotFound)
	s.handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS()))))
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

func methodNotAllowed(w http.ResponseWriter, r *http.Request) {
	http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
}

func (s *Server) redirectRoot(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/clusters", http.StatusSeeOther)
}
