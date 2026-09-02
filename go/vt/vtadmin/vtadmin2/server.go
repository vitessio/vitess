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
		// TrustProxyProto marks cookies Secure when the request arrives without
		// direct TLS but carries X-Forwarded-Proto: https from a trusted
		// HTTPS-terminating proxy. Only enable this when the UI is behind such
		// a proxy; the header is spoofable otherwise.
		TrustProxyProto bool
	}

	Server struct {
		api       vtAdminAPI
		opts      Options
		templates *templateSet
		router    *http.ServeMux
		handler   http.Handler
	}
)

func NewServer(api vtAdminAPI, opts Options) (*Server, error) {
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
	s.handler = s.secureHeaders(s.router)
	return s, nil
}

// secureHeaders applies response hardening to every UI response. The UI
// renders CSRF tokens, so framing it from another origin would let an
// attacker clickjack authenticated operators.
func (s *Server) secureHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "frame-ancestors 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
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
	s.handleFunc("POST /keyspace/{cluster_id}/{name}/validate", s.keyspaceValidate)
	s.handleFunc("POST /keyspace/{cluster_id}/{name}/validate_schema", s.keyspaceValidateSchema)
	s.handleFunc("POST /keyspace/{cluster_id}/{name}/validate_version", s.keyspaceValidateVersion)
	s.handleFunc("POST /keyspace/{cluster_id}/{name}/rebuild_graph", s.keyspaceRebuildGraph)
	s.handleFunc("POST /keyspace/{cluster_id}/{name}/remove_cell", s.keyspaceRemoveCell)
	s.handleFunc("POST /keyspace/{cluster_id}/{name}/create_shard", s.keyspaceCreateShard)
	s.handleFunc("POST /keyspace/{cluster_id}/{name}/reload_schema", s.keyspaceReloadSchema)
	s.handleFunc("GET /keyspace/{cluster_id}/{name}/shard/{shard}", s.shardDetail)
	s.handleFunc("POST /keyspace/{cluster_id}/{name}/shard/{shard}/delete", s.shardDelete)
	s.handleFunc("POST /keyspace/{cluster_id}/{name}/shard/{shard}/reload-schema", s.shardReloadSchema)
	s.handleFunc("POST /keyspace/{cluster_id}/{name}/shard/{shard}/externally-promote", s.shardExternallyPromote)
	s.handleFunc("POST /keyspace/{cluster_id}/{name}/shard/{shard}/planned-failover", s.shardPlannedFailover)
	s.handleFunc("POST /keyspace/{cluster_id}/{name}/shard/{shard}/emergency-failover", s.shardEmergencyFailover)
	s.handleFunc("POST /keyspace/{cluster_id}/{name}/shard/{shard}/validate", s.shardValidate)
	s.handleFunc("POST /keyspace/{cluster_id}/{name}/shard/{shard}/validate-version", s.shardValidateVersion)
	s.handleFunc("GET /topology", s.topologyPath)
	s.handleFunc("GET /topology/{cluster_id}", s.topologyTree)
	s.handleFunc("GET /topologytree/{cluster_id}", s.topologyTree)
	s.handleFunc("GET /shards", s.shards)
	s.handleFunc("GET /tablets", s.tablets)
	s.handleFunc("GET /tablet/{cluster_id}/{alias}", s.tabletDetail)
	s.handleFunc("GET /tablet/{cluster_id}/{alias}/full_status", s.tabletFullStatus)
	s.handleFunc("POST /tablet/{cluster_id}/{alias}/start_replication", s.tabletStartReplication)
	s.handleFunc("POST /tablet/{cluster_id}/{alias}/stop_replication", s.tabletStopReplication)
	s.handleFunc("POST /tablet/{cluster_id}/{alias}/refresh_replication_source", s.tabletRefreshReplicationSource)
	s.handleFunc("POST /tablet/{cluster_id}/{alias}/set_read_only", s.tabletSetReadOnly)
	s.handleFunc("POST /tablet/{cluster_id}/{alias}/set_read_write", s.tabletSetReadWrite)
	s.handleFunc("POST /tablet/{cluster_id}/{alias}/delete", s.tabletDelete)
	s.handleFunc("POST /tablet/{cluster_id}/{alias}/ping", s.tabletPing)
	s.handleFunc("POST /tablet/{cluster_id}/{alias}/refresh_state", s.tabletRefreshState)
	s.handleFunc("POST /tablet/{cluster_id}/{alias}/health_check", s.tabletRunHealthCheck)
	s.handleFunc("GET /schemas", s.schemas)
	s.handleFunc("GET /schema/{cluster_id}/{keyspace}/{table}", s.schemaDetail)
	s.handleFunc("GET /vschemas", s.vschemas)
	s.handleFunc("GET /vschema/{cluster_id}/{keyspace}", s.vschema)
	s.handleFunc("GET /srvkeyspaces", s.srvKeyspaces)
	s.handleFunc("GET /srvvschemas", s.srvVSchemas)
	s.handleFunc("GET /cells", s.cells)
	s.handleFunc("GET /cells_aliases", s.cellsAliases)
	s.handleFunc("GET /backups", s.backups)
	s.handleFunc("GET /workflows", s.workflows)
	s.handleFunc("GET /workflows/movetables/create", s.createMoveTablesForm)
	s.handleFunc("POST /workflows/movetables/create", s.createMoveTables)
	s.handleFunc("GET /workflows/reshard/create", s.createReshardForm)
	s.handleFunc("POST /workflows/reshard/create", s.createReshard)
	s.handleFunc("GET /workflows/materialize/create", s.createMaterializeForm)
	s.handleFunc("POST /workflows/materialize/create", s.createMaterialize)
	s.handleFunc("GET /migrations/create", s.createMigrationForm)
	s.handleFunc("POST /migrations/create", s.createMigration)
	s.handleFunc("GET /migrations", s.schemaMigrations)
	s.handleFunc("GET /transactions", s.transactions)
	s.handleFunc("GET /vtexplain", s.vtExplain)
	s.handleFunc("GET /vexplain", s.vExplain)
	s.handleFunc("GET /settings", s.settingsForm)
	s.handleFunc("POST /settings", s.settingsSave)
	s.handleFunc("GET /transaction/{cluster_id}/{dtid}/info", s.transactionInfo)
	// SPA compat: the old UI served the detail page at /transaction/{c}/{dtid}.
	s.handleFunc("GET /transaction/{cluster_id}/{dtid}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/transaction/"+pathEscape(r.PathValue("cluster_id"))+"/"+pathEscape(r.PathValue("dtid"))+"/info", http.StatusMovedPermanently)
	})
	s.handleFunc("POST /transaction/{cluster_id}/{dtid}/conclude", s.transactionConclude)
	s.handleFunc("GET /workflow/{cluster_id}/{keyspace}/{name}", s.workflow)
	s.handleFunc("GET /workflow/{cluster_id}/{keyspace}/{name}/status", s.workflowStatus)
	s.handleFunc("POST /workflow/{cluster_id}/{keyspace}/{name}/start", s.workflowStart)
	s.handleFunc("POST /workflow/{cluster_id}/{keyspace}/{name}/stop", s.workflowStop)
	s.handleFunc("POST /workflow/{cluster_id}/{keyspace}/{name}/cancel", s.workflowCancel)
	s.handleFunc("POST /workflow/{cluster_id}/{keyspace}/{name}/complete", s.workflowComplete)
	s.handleFunc("POST /workflow/{cluster_id}/{keyspace}/{name}/switch_traffic", s.workflowSwitchTrafficForward)
	s.handleFunc("POST /workflow/{cluster_id}/{keyspace}/{name}/reverse_traffic", s.workflowSwitchTrafficReverse)
	s.handleFunc("POST /workflow/{cluster_id}/{keyspace}/{name}/vdiff", s.workflowVDiffCreate)
	s.handleFunc("GET /workflow/{cluster_id}/{keyspace}/{name}/stream/{tablet_cell}/{tablet_uid}/{stream_id}", s.streamDetail)
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
