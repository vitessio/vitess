/*
Copyright 2020 The Vitess Authors.

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

package vtadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"
	"net/url"
	stdsort "sort"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/patrickmn/go-cache"

	"vitess.io/vitess/go/sets"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
	"vitess.io/vitess/go/vt/vtadmin/cluster/dynamic"
	"vitess.io/vitess/go/vt/vtadmin/errors"
	"vitess.io/vitess/go/vt/vtadmin/grpcserver"
	vtadminhttp "vitess.io/vitess/go/vt/vtadmin/http"
	"vitess.io/vitess/go/vt/vtadmin/http/debug"
	"vitess.io/vitess/go/vt/vtadmin/http/experimental"
	vthandlers "vitess.io/vitess/go/vt/vtadmin/http/handlers"
	"vitess.io/vitess/go/vt/vtadmin/rbac"
	"vitess.io/vitess/go/vt/vtadmin/sort"
	"vitess.io/vitess/go/vt/vtadmin/vtadminproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtexplain"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// API is the main entrypoint for the vtadmin server. It implements
// vtadminpb.VTAdminServer.
type API struct {
	vtadminpb.UnimplementedVTAdminServer

	clusterMu    sync.Mutex // guards `clusters` and `clusterMap`
	clusters     []*cluster.Cluster
	clusterMap   map[string]*cluster.Cluster
	clusterCache *cache.Cache
	serv         *grpcserver.Server
	router       *mux.Router

	authz *rbac.Authorizer

	options Options

	// vtexplain is now global again due to stat exporters in the tablet layer
	// we're not super concerned because we will be deleting vtexplain Soon(TM).
	vtexplainLock sync.Mutex
}

// Options wraps the configuration options for different components of the
// vtadmin API.
type Options struct {
	GRPCOpts grpcserver.Options
	HTTPOpts vtadminhttp.Options
	RBAC     *rbac.Config
	// EnableDynamicClusters makes it so that clients can pass clusters dynamically
	// in a session-like way, either via HTTP cookies or gRPC metadata.
	EnableDynamicClusters bool
}

// NewAPI returns a new API, configured to service the given set of clusters,
// and configured with the given options.
func NewAPI(clusters []*cluster.Cluster, opts Options) *API {
	clusterMap := make(map[string]*cluster.Cluster, len(clusters))
	for _, cluster := range clusters {
		clusterMap[cluster.ID] = cluster
	}

	sort.ClustersBy(func(c1, c2 *cluster.Cluster) bool {
		return c1.ID < c2.ID
	}).Sort(clusters)

	var (
		authn rbac.Authenticator
		authz *rbac.Authorizer
	)
	if opts.RBAC != nil {
		authn = opts.RBAC.GetAuthenticator()
		authz = opts.RBAC.GetAuthorizer()

		if authn != nil {
			opts.GRPCOpts.StreamInterceptors = append(opts.GRPCOpts.StreamInterceptors, rbac.AuthenticationStreamInterceptor(authn))
			opts.GRPCOpts.UnaryInterceptors = append(opts.GRPCOpts.UnaryInterceptors, rbac.AuthenticationUnaryInterceptor(authn))
		}
	}

	if authz == nil {
		authz, _ = rbac.NewAuthorizer(&rbac.Config{
			Rules: []*struct {
				Resource string
				Actions  []string
				Subjects []string
				Clusters []string
			}{
				{
					Resource: "*",
					Actions:  []string{"*"},
					Subjects: []string{"*"},
					Clusters: []string{"*"},
				},
			},
		})
	}

	api := &API{
		clusters:   clusters,
		clusterMap: clusterMap,
		authz:      authz,
	}

	if opts.EnableDynamicClusters {
		api.clusterCache = cache.New(24*time.Hour, 24*time.Hour)
		api.clusterCache.OnEvicted(api.EjectDynamicCluster)

		opts.GRPCOpts.StreamInterceptors = append(opts.GRPCOpts.StreamInterceptors, dynamic.StreamServerInterceptor(api))
		opts.GRPCOpts.UnaryInterceptors = append(opts.GRPCOpts.UnaryInterceptors, dynamic.UnaryServerInterceptor(api))
	}

	api.options = opts

	serv := grpcserver.New("vtadmin", opts.GRPCOpts)
	serv.Router().HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})

	router := serv.Router().PathPrefix("/api").Subrouter()
	router.PathPrefix("/").Handler(api).Methods("DELETE", "OPTIONS", "GET", "POST", "PUT")

	api.serv = serv
	api.router = router
	vtadminpb.RegisterVTAdminServer(api.serv.GRPCServer(), api)

	if !opts.HTTPOpts.DisableDebug {
		// Due to the way net/http/pprof insists on registering its handlers, we
		// have to put these on the root router, and not on the /debug prefixed
		// subrouter, which would make way more sense, but alas. Additional
		// debug routes should still go on the /debug subrouter, though.
		serv.Router().HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		serv.Router().HandleFunc("/debug/pprof/profile", pprof.Profile)
		serv.Router().HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		serv.Router().PathPrefix("/debug/pprof").HandlerFunc(pprof.Index)

		dapi := &debugAPI{api}
		debugRouter := serv.Router().PathPrefix("/debug").Subrouter()
		debugRouter.HandleFunc("/env", debug.Env)
		debugRouter.HandleFunc("/cluster/{cluster_id}", debug.Cluster(dapi))
		debugRouter.HandleFunc("/clusters", debug.Clusters(dapi))
	}

	// Middlewares are executed in order of addition. Our ordering (all
	// middlewares being optional) is:
	// 	1. CORS. CORS is a special case and is applied globally, the rest are applied only to the subrouter.
	//	2. Compression
	//	3. Tracing
	//	4. Authentication
	middlewares := []mux.MiddlewareFunc{}

	if len(opts.HTTPOpts.CORSOrigins) > 0 {
		serv.Router().Use(handlers.CORS(
			handlers.AllowCredentials(), handlers.AllowedOrigins(opts.HTTPOpts.CORSOrigins), handlers.AllowedMethods([]string{"GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS"})))
	}

	if !opts.HTTPOpts.DisableCompression {
		middlewares = append(middlewares, handlers.CompressHandler)
	}

	if opts.HTTPOpts.EnableTracing {
		middlewares = append(middlewares, vthandlers.TraceHandler)
	}

	if authn != nil {
		middlewares = append(middlewares, vthandlers.NewAuthenticationHandler(authn))
	}

	router.Use(middlewares...)

	return api
}

// Close closes all the clusters in an API concurrently. Its primary function is
// to gracefully shutdown cache background goroutines to avoid data races in
// tests, but needs to be exported to be called by those tests. It does not have
// any production use case.
func (api *API) Close() error {
	var (
		wg  sync.WaitGroup
		rec concurrency.AllErrorRecorder
	)

	for _, c := range api.clusters {
		wg.Add(1)
		go func(c *cluster.Cluster) {
			defer wg.Done()
			rec.RecordError(c.Close())
		}(c)
	}

	wg.Wait()
	return rec.Error()
}

// ListenAndServe starts serving this API on the configured Addr (see
// grpcserver.Options) until shutdown or irrecoverable error occurs.
func (api *API) ListenAndServe() error {
	return api.serv.ListenAndServe()
}

// ServeHTTP serves all routes matching path "/api" (see above)
// It first processes cookies, and acts accordingly
// Primarily, it sets up a dynamic API if HttpOpts.EnableDynamicClusters is set
// to true.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !api.options.EnableDynamicClusters {
		api.Handler().ServeHTTP(w, r)
		return
	}

	var dynamicAPI dynamic.API = api

	if clusterCookie, err := r.Cookie("cluster"); err == nil {
		urlDecoded, err := url.QueryUnescape(clusterCookie.Value)
		if err == nil {
			c, id, err := dynamic.ClusterFromString(r.Context(), urlDecoded)
			if id != "" {
				if err != nil {
					log.Warningf("failed to extract valid cluster from cookie; attempting to use existing cluster with id=%s; error: %s", id, err)
				}

				dynamicAPI = api.WithCluster(c, id)
			} else {
				log.Warningf("failed to unmarshal dynamic cluster spec from cookie; falling back to static API; error: %s", err)
			}
		}
	}

	dynamicAPI.Handler().ServeHTTP(w, r)
}

// WithCluster returns a dynamic API with the given cluster. If `c` is non-nil,
// it is used as the selected cluster. If the cluster is nil, then a cluster
// with the given id is retrieved from the API and used in the dynamic API.
//
// Callers must ensure that:
// 1. If c is non-nil, c.ID == id.
// 2. id is non-empty.
//
// Note that using dynamic.ClusterFromString ensures both of these
// preconditions.
func (api *API) WithCluster(c *cluster.Cluster, id string) dynamic.API {
	api.clusterMu.Lock()
	defer api.clusterMu.Unlock()

	dynamicAPI := &API{
		router:  api.router,
		serv:    api.serv,
		authz:   api.authz,
		options: api.options,
	}

	if c != nil {
		existingCluster, exists := api.clusterMap[id]
		shouldAddCluster := !exists
		if exists {
			isEqual, err := existingCluster.Equal(c)
			if err != nil {
				log.Errorf("Error checking for existing cluster %s equality with new cluster %s: %v", existingCluster.ID, id, err)
			}
			shouldAddCluster = shouldAddCluster || !isEqual
		}
		if shouldAddCluster {
			if existingCluster != nil {
				if err := existingCluster.Close(); err != nil {
					log.Errorf("%s; some connections and goroutines may linger", err.Error())
				}

				idx := stdsort.Search(len(api.clusters), func(i int) bool {
					return api.clusters[i].ID == existingCluster.ID
				})
				if idx >= 0 && idx < len(api.clusters) {
					api.clusters = append(api.clusters[:idx], api.clusters[idx+1:]...)
				}
			}

			api.clusterMap[id] = c
			api.clusters = append(api.clusters, c)
			sort.ClustersBy(func(c1, c2 *cluster.Cluster) bool {
				return c1.ID < c2.ID
			}).Sort(api.clusters)

			api.clusterCache.Set(id, c, cache.DefaultExpiration)
		} else {
			log.Infof("API already has cluster with id %s, using that instead", id)
		}
	}

	selectedCluster := api.clusterMap[id]
	dynamicAPI.clusters = []*cluster.Cluster{selectedCluster}
	dynamicAPI.clusterMap = map[string]*cluster.Cluster{id: selectedCluster}

	return dynamicAPI
}

// Handler handles all routes under "/api" (see above)
func (api *API) Handler() http.Handler {
	router := mux.NewRouter().PathPrefix("/api").Subrouter()

	router.Use(handlers.CORS(
		handlers.AllowCredentials(), handlers.AllowedOrigins(api.options.HTTPOpts.CORSOrigins), handlers.AllowedMethods([]string{"GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS"})))

	httpAPI := vtadminhttp.NewAPI(api, api.options.HTTPOpts)

	router.HandleFunc("/backups", httpAPI.Adapt(vtadminhttp.GetBackups)).Name("API.GetBackups")
	router.HandleFunc("/cells", httpAPI.Adapt(vtadminhttp.GetCellInfos)).Name("API.GetCellInfos")
	router.HandleFunc("/cells_aliases", httpAPI.Adapt(vtadminhttp.GetCellsAliases)).Name("API.GetCellsAliases")
	router.HandleFunc("/clusters", httpAPI.Adapt(vtadminhttp.GetClusters)).Name("API.GetClusters")
	router.HandleFunc("/cluster/{cluster_id}/topology", httpAPI.Adapt(vtadminhttp.GetTopologyPath)).Name("API.GetTopologyPath")
	router.HandleFunc("/cluster/{cluster_id}/validate", httpAPI.Adapt(vtadminhttp.Validate)).Name("API.Validate").Methods("PUT", "OPTIONS")
	router.HandleFunc("/gates", httpAPI.Adapt(vtadminhttp.GetGates)).Name("API.GetGates")
	router.HandleFunc("/keyspace/{cluster_id}", httpAPI.Adapt(vtadminhttp.CreateKeyspace)).Name("API.CreateKeyspace").Methods("POST")
	router.HandleFunc("/keyspace/{cluster_id}/{name}", httpAPI.Adapt(vtadminhttp.DeleteKeyspace)).Name("API.DeleteKeyspace").Methods("DELETE")
	router.HandleFunc("/keyspace/{cluster_id}/{name}", httpAPI.Adapt(vtadminhttp.GetKeyspace)).Name("API.GetKeyspace")
	router.HandleFunc("/keyspace/{cluster_id}/{name}/rebuild_keyspace_graph", httpAPI.Adapt(vtadminhttp.RebuildKeyspaceGraph)).Name("API.RebuildKeyspaceGraph").Methods("PUT", "OPTIONS")
	router.HandleFunc("/keyspace/{cluster_id}/{name}/remove_keyspace_cell", httpAPI.Adapt(vtadminhttp.RemoveKeyspaceCell)).Name("API.RemoveKeyspaceCell").Methods("PUT", "OPTIONS")
	router.HandleFunc("/keyspace/{cluster_id}/{name}/validate", httpAPI.Adapt(vtadminhttp.ValidateKeyspace)).Name("API.ValidateKeyspace").Methods("PUT", "OPTIONS")
	router.HandleFunc("/keyspace/{cluster_id}/{name}/validate/schema", httpAPI.Adapt(vtadminhttp.ValidateSchemaKeyspace)).Name("API.ValidateSchemaKeyspace").Methods("PUT", "OPTIONS")
	router.HandleFunc("/keyspace/{cluster_id}/{name}/validate/version", httpAPI.Adapt(vtadminhttp.ValidateVersionKeyspace)).Name("API.ValidateVersionKeyspace").Methods("PUT", "OPTIONS")
	router.HandleFunc("/keyspaces", httpAPI.Adapt(vtadminhttp.GetKeyspaces)).Name("API.GetKeyspaces")
	router.HandleFunc("/schema/{table}", httpAPI.Adapt(vtadminhttp.FindSchema)).Name("API.FindSchema")
	router.HandleFunc("/schema/{cluster_id}/{keyspace}/{table}", httpAPI.Adapt(vtadminhttp.GetSchema)).Name("API.GetSchema")
	router.HandleFunc("/schemas", httpAPI.Adapt(vtadminhttp.GetSchemas)).Name("API.GetSchemas")
	router.HandleFunc("/schemas/reload", httpAPI.Adapt(vtadminhttp.ReloadSchemas)).Name("API.ReloadSchemas").Methods("PUT", "OPTIONS")
	router.HandleFunc("/shard/{cluster_id}/{keyspace}/{shard}/emergency_failover", httpAPI.Adapt(vtadminhttp.EmergencyFailoverShard)).Name("API.EmergencyFailoverShard").Methods("POST")
	router.HandleFunc("/shard/{cluster_id}/{keyspace}/{shard}/planned_failover", httpAPI.Adapt(vtadminhttp.PlannedFailoverShard)).Name("API.PlannedFailoverShard").Methods("POST")
	router.HandleFunc("/shard/{cluster_id}/{keyspace}/{shard}/reload_schema_shard", httpAPI.Adapt(vtadminhttp.ReloadSchemaShard)).Name("API.ReloadSchemaShard").Methods("PUT", "OPTIONS")
	router.HandleFunc("/shard/{cluster_id}/{keyspace}/{shard}/validate", httpAPI.Adapt(vtadminhttp.ValidateShard)).Name("API.ValidateShard").Methods("PUT", "OPTIONS")
	router.HandleFunc("/shard/{cluster_id}/{keyspace}/{shard}/validate_version", httpAPI.Adapt(vtadminhttp.ValidateVersionShard)).Name("API.ValidateVersionShard").Methods("PUT", "OPTIONS")
	router.HandleFunc("/shard_replication_positions", httpAPI.Adapt(vtadminhttp.GetShardReplicationPositions)).Name("API.GetShardReplicationPositions")
	router.HandleFunc("/shards/{cluster_id}", httpAPI.Adapt(vtadminhttp.CreateShard)).Name("API.CreateShard").Methods("POST")
	router.HandleFunc("/shards/{cluster_id}", httpAPI.Adapt(vtadminhttp.DeleteShards)).Name("API.DeleteShards").Methods("DELETE")
	router.HandleFunc("/srvkeyspaces", httpAPI.Adapt(vtadminhttp.GetSrvKeyspaces)).Name("API.GetSrvKeyspaces").Methods("GET")
	router.HandleFunc("/srvkeyspace/{cluster_id}/{name}", httpAPI.Adapt(vtadminhttp.GetSrvKeyspace)).Name("API.GetSrvKeyspace").Methods("GET")
	router.HandleFunc("/srvvschema/{cluster_id}/{cell}", httpAPI.Adapt(vtadminhttp.GetSrvVSchema)).Name("API.GetSrvVSchema")
	router.HandleFunc("/srvvschemas", httpAPI.Adapt(vtadminhttp.GetSrvVSchemas)).Name("API.GetSrvVSchemas")
	router.HandleFunc("/tablets", httpAPI.Adapt(vtadminhttp.GetTablets)).Name("API.GetTablets")
	router.HandleFunc("/tablet/{tablet}", httpAPI.Adapt(vtadminhttp.GetTablet)).Name("API.GetTablet").Methods("GET")
	router.HandleFunc("/tablet/{tablet}", httpAPI.Adapt(vtadminhttp.DeleteTablet)).Name("API.DeleteTablet").Methods("DELETE", "OPTIONS")
	router.HandleFunc("/tablet/{tablet}/full_status", httpAPI.Adapt(vtadminhttp.GetFullStatus)).Name("API.GetFullStatus").Methods("GET")
	router.HandleFunc("/tablet/{tablet}/healthcheck", httpAPI.Adapt(vtadminhttp.RunHealthCheck)).Name("API.RunHealthCheck")
	router.HandleFunc("/tablet/{tablet}/ping", httpAPI.Adapt(vtadminhttp.PingTablet)).Name("API.PingTablet")
	router.HandleFunc("/tablet/{tablet}/refresh", httpAPI.Adapt(vtadminhttp.RefreshState)).Name("API.RefreshState").Methods("PUT", "OPTIONS")
	router.HandleFunc("/tablet/{tablet}/refresh_replication_source", httpAPI.Adapt(vtadminhttp.RefreshTabletReplicationSource)).Name("API.RefreshTabletReplicationSource").Methods("PUT", "OPTIONS")
	router.HandleFunc("/tablet/{tablet}/reload_schema", httpAPI.Adapt(vtadminhttp.ReloadTabletSchema)).Name("API.ReloadTabletSchema").Methods("PUT", "OPTIONS")
	router.HandleFunc("/tablet/{tablet}/set_read_only", httpAPI.Adapt(vtadminhttp.SetReadOnly)).Name("API.SetReadOnly").Methods("PUT", "OPTIONS")
	router.HandleFunc("/tablet/{tablet}/set_read_write", httpAPI.Adapt(vtadminhttp.SetReadWrite)).Name("API.SetReadWrite").Methods("PUT", "OPTIONS")
	router.HandleFunc("/tablet/{tablet}/start_replication", httpAPI.Adapt(vtadminhttp.StartReplication)).Name("API.StartReplication").Methods("PUT", "OPTIONS")
	router.HandleFunc("/tablet/{tablet}/stop_replication", httpAPI.Adapt(vtadminhttp.StopReplication)).Name("API.StopReplication").Methods("PUT", "OPTIONS")
	router.HandleFunc("/tablet/{tablet}/externally_promoted", httpAPI.Adapt(vtadminhttp.TabletExternallyPromoted)).Name("API.TabletExternallyPromoted").Methods("POST")
	router.HandleFunc("/vschema/{cluster_id}/{keyspace}", httpAPI.Adapt(vtadminhttp.GetVSchema)).Name("API.GetVSchema")
	router.HandleFunc("/vschemas", httpAPI.Adapt(vtadminhttp.GetVSchemas)).Name("API.GetVSchemas")
	router.HandleFunc("/vtctlds", httpAPI.Adapt(vtadminhttp.GetVtctlds)).Name("API.GetVtctlds")
	router.HandleFunc("/vtexplain", httpAPI.Adapt(vtadminhttp.VTExplain)).Name("API.VTExplain")
	router.HandleFunc("/workflow/{cluster_id}/{keyspace}/{name}", httpAPI.Adapt(vtadminhttp.GetWorkflow)).Name("API.GetWorkflow")
	router.HandleFunc("/workflows", httpAPI.Adapt(vtadminhttp.GetWorkflows)).Name("API.GetWorkflows")

	experimentalRouter := router.PathPrefix("/experimental").Subrouter()
	experimentalRouter.HandleFunc("/tablet/{tablet}/debug/vars", httpAPI.Adapt(experimental.TabletDebugVarsPassthrough)).Name("API.TabletDebugVarsPassthrough")
	experimentalRouter.HandleFunc("/whoami", httpAPI.Adapt(experimental.WhoAmI))

	return router
}

func (api *API) EjectDynamicCluster(key string, value any) {
	api.clusterMu.Lock()
	defer api.clusterMu.Unlock()

	// Delete dynamic clusters from clusterMap when they are expired from clusterCache
	c, ok := api.clusterMap[key]
	if ok {
		delete(api.clusterMap, key)
		if err := c.Close(); err != nil {
			log.Errorf("%s; some connections and goroutines may linger", err.Error())
		}
	}

	// Maintain order of clusters when removing dynamic cluster
	clusterIndex := stdsort.Search(len(api.clusters), func(i int) bool { return api.clusters[i].ID == key })
	if clusterIndex >= len(api.clusters) || clusterIndex < 0 {
		log.Errorf("Cannot remove cluster %s from api.clusters. Cluster index %d is out of range for clusters slice of %d length.", key, clusterIndex, len(api.clusters))
	}

	api.clusters = append(api.clusters[:clusterIndex], api.clusters[clusterIndex+1:]...)
}

// CreateKeyspace is part of the vtadminpb.VTAdminServer interface.
func (api *API) CreateKeyspace(ctx context.Context, req *vtadminpb.CreateKeyspaceRequest) (*vtadminpb.CreateKeyspaceResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.CreateKeyspace")
	defer span.Finish()

	span.Annotate("cluster_id", req.ClusterId)

	if !api.authz.IsAuthorized(ctx, req.ClusterId, rbac.KeyspaceResource, rbac.CreateAction) {
		return nil, fmt.Errorf("%w: cannot create keyspace in %s", errors.ErrUnauthorized, req.ClusterId)
	}

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	ks, err := c.CreateKeyspace(ctx, req.Options)
	if err != nil {
		return nil, err
	}

	return &vtadminpb.CreateKeyspaceResponse{
		Keyspace: ks,
	}, nil
}

// CreateShard is part of the vtadminpb.VTAdminServer interface.
func (api *API) CreateShard(ctx context.Context, req *vtadminpb.CreateShardRequest) (*vtctldatapb.CreateShardResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.CreateShard")
	defer span.Finish()

	span.Annotate("cluster_id", req.ClusterId)

	if !api.authz.IsAuthorized(ctx, req.ClusterId, rbac.ShardResource, rbac.CreateAction) {
		return nil, fmt.Errorf("%w: cannot create shard in %s", errors.ErrUnauthorized, req.ClusterId)
	}

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	return c.CreateShard(ctx, req.Options)
}

// DeleteKeyspace is part of the vtadminpb.VTAdminServer interface.
func (api *API) DeleteKeyspace(ctx context.Context, req *vtadminpb.DeleteKeyspaceRequest) (*vtctldatapb.DeleteKeyspaceResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.DeleteKeyspace")
	defer span.Finish()

	span.Annotate("cluster_id", req.ClusterId)

	if !api.authz.IsAuthorized(ctx, req.ClusterId, rbac.KeyspaceResource, rbac.DeleteAction) {
		return nil, fmt.Errorf("%w: cannot delete keyspace in %s", errors.ErrUnauthorized, req.ClusterId)
	}

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	return c.DeleteKeyspace(ctx, req.Options)
}

// DeleteShards is part of the vtadminpb.VTAdminServer interface.
func (api *API) DeleteShards(ctx context.Context, req *vtadminpb.DeleteShardsRequest) (*vtctldatapb.DeleteShardsResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.DeleteShards")
	defer span.Finish()

	span.Annotate("cluster_id", req.ClusterId)

	if !api.authz.IsAuthorized(ctx, req.ClusterId, rbac.ShardResource, rbac.DeleteAction) {
		return nil, fmt.Errorf("%w: cannot delete shards in %s", errors.ErrUnauthorized, req.ClusterId)
	}

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	return c.DeleteShards(ctx, req.Options)
}

// DeleteTablet is part of the vtadminpb.VTAdminServer interface.
func (api *API) DeleteTablet(ctx context.Context, req *vtadminpb.DeleteTabletRequest) (*vtadminpb.DeleteTabletResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.DeleteTablet")
	defer span.Finish()

	tablet, c, err := api.getTabletForAction(ctx, span, rbac.DeleteAction, req.Alias, req.ClusterIds)
	if err != nil {
		return nil, err
	}

	if _, err := c.DeleteTablets(ctx, &vtctldatapb.DeleteTabletsRequest{
		AllowPrimary:  req.AllowPrimary,
		TabletAliases: []*topodatapb.TabletAlias{tablet.Tablet.Alias},
	}); err != nil {
		return nil, fmt.Errorf("failed to delete tablet: %w", err)
	}

	return &vtadminpb.DeleteTabletResponse{
		Status:  "ok",
		Cluster: c.ToProto(),
	}, nil
}

// EmergencyFailoverShard is part of the vtadminpb.VTAdminServer interface.
func (api *API) EmergencyFailoverShard(ctx context.Context, req *vtadminpb.EmergencyFailoverShardRequest) (*vtadminpb.EmergencyFailoverShardResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.EmergencyFailoverShard")
	defer span.Finish()

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.ShardResource, rbac.EmergencyFailoverShardAction) {
		return nil, nil
	}

	return c.EmergencyFailoverShard(ctx, req.Options)
}

// FindSchema is part of the vtadminpb.VTAdminServer interface.
func (api *API) FindSchema(ctx context.Context, req *vtadminpb.FindSchemaRequest) (*vtadminpb.Schema, error) {
	span, _ := trace.NewSpan(ctx, "API.FindSchema")
	defer span.Finish()

	span.Annotate("table", req.Table)

	clusters, clusterIDs := api.getClustersForRequest(req.ClusterIds)

	var (
		m       sync.Mutex
		wg      sync.WaitGroup
		rec     concurrency.AllErrorRecorder
		results []*vtadminpb.Schema
	)

	for _, c := range clusters {
		if !api.authz.IsAuthorized(ctx, c.ID, rbac.SchemaResource, rbac.GetAction) {
			continue
		}

		wg.Add(1)

		go func(c *cluster.Cluster) {
			defer wg.Done()

			schemas, err := c.GetSchemas(ctx, cluster.GetSchemaOptions{
				TableSizeOptions: req.TableSizeOptions,
			})
			if err != nil {
				rec.RecordError(err)
				return
			}

			for _, schema := range schemas {
				for _, td := range schema.TableDefinitions {
					if td.Name == req.Table {
						m.Lock()
						results = append(results, schema)
						m.Unlock()

						return
					}
				}
			}

			log.Infof("cluster %s has no tables named %s", c.ID, req.Table)
		}(c)
	}

	wg.Wait()

	if rec.HasErrors() {
		return nil, rec.Error()
	}

	switch len(results) {
	case 0:
		return nil, &errors.NoSuchSchema{
			Clusters: clusterIDs,
			Table:    req.Table,
		}
	case 1:
		return results[0], nil
	default:
		return nil, fmt.Errorf("%w: %d schemas found with table named %s", errors.ErrAmbiguousSchema, len(results), req.Table)
	}
}

// GetBackups is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetBackups(ctx context.Context, req *vtadminpb.GetBackupsRequest) (*vtadminpb.GetBackupsResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetBackups")
	defer span.Finish()

	clusters, _ := api.getClustersForRequest(req.ClusterIds)

	var (
		m       sync.Mutex
		wg      sync.WaitGroup
		rec     concurrency.AllErrorRecorder
		backups []*vtadminpb.ClusterBackup
	)

	if req.RequestOptions == nil {
		req.RequestOptions = &vtctldatapb.GetBackupsRequest{}
	}

	for _, c := range clusters {
		if !api.authz.IsAuthorized(ctx, c.ID, rbac.BackupResource, rbac.GetAction) {
			continue
		}

		wg.Add(1)

		go func(c *cluster.Cluster) {
			defer wg.Done()

			bs, err := c.GetBackups(ctx, req)
			if err != nil {
				rec.RecordError(err)
				return
			}

			m.Lock()
			defer m.Unlock()

			backups = append(backups, bs...)
		}(c)
	}

	wg.Wait()

	if rec.HasErrors() {
		return nil, rec.Error()
	}

	return &vtadminpb.GetBackupsResponse{
		Backups: backups,
	}, nil
}

// GetCellInfos is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetCellInfos(ctx context.Context, req *vtadminpb.GetCellInfosRequest) (*vtadminpb.GetCellInfosResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetCellInfos")
	defer span.Finish()

	clusters, _ := api.getClustersForRequest(req.ClusterIds)

	var (
		m         sync.Mutex
		wg        sync.WaitGroup
		rec       concurrency.AllErrorRecorder
		cellInfos []*vtadminpb.ClusterCellInfo
	)

	for _, c := range clusters {
		if !api.authz.IsAuthorized(ctx, c.ID, rbac.CellInfoResource, rbac.GetAction) {
			continue
		}

		wg.Add(1)
		go func(c *cluster.Cluster) {
			defer wg.Done()

			clusterCellInfos, err := c.GetCellInfos(ctx, req)
			if err != nil {
				rec.RecordError(fmt.Errorf("failed to GetCellInfos for cluster %s: %w", c.ID, err))
				return
			}

			m.Lock()
			defer m.Unlock()
			cellInfos = append(cellInfos, clusterCellInfos...)
		}(c)
	}

	wg.Wait()
	if rec.HasErrors() {
		return nil, rec.Error()
	}

	return &vtadminpb.GetCellInfosResponse{
		CellInfos: cellInfos,
	}, nil
}

// GetCellsAliases is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetCellsAliases(ctx context.Context, req *vtadminpb.GetCellsAliasesRequest) (*vtadminpb.GetCellsAliasesResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetCellsAliases")
	defer span.Finish()

	clusters, _ := api.getClustersForRequest(req.ClusterIds)

	var (
		m       sync.Mutex
		wg      sync.WaitGroup
		rec     concurrency.AllErrorRecorder
		aliases []*vtadminpb.ClusterCellsAliases
	)

	for _, c := range clusters {
		if !api.authz.IsAuthorized(ctx, c.ID, rbac.CellsAliasResource, rbac.GetAction) {
			continue
		}

		wg.Add(1)
		go func(c *cluster.Cluster) {
			defer wg.Done()

			clusterAliases, err := c.GetCellsAliases(ctx)
			if err != nil {
				rec.RecordError(fmt.Errorf("failed to GetCellsAliases for cluster %s: %w", c.ID, err))
				return
			}

			m.Lock()
			defer m.Unlock()
			aliases = append(aliases, clusterAliases)
		}(c)
	}

	wg.Wait()
	if rec.HasErrors() {
		return nil, rec.Error()
	}

	return &vtadminpb.GetCellsAliasesResponse{
		Aliases: aliases,
	}, nil
}

// GetClusters is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetClusters(ctx context.Context, req *vtadminpb.GetClustersRequest) (*vtadminpb.GetClustersResponse, error) {
	span, _ := trace.NewSpan(ctx, "API.GetClusters")
	defer span.Finish()

	clusters, _ := api.getClustersForRequest(nil)

	vcs := make([]*vtadminpb.Cluster, 0, len(clusters))

	for _, c := range clusters {
		if !api.authz.IsAuthorized(ctx, c.ID, rbac.ClusterResource, rbac.GetAction) {
			continue
		}

		vcs = append(vcs, &vtadminpb.Cluster{
			Id:   c.ID,
			Name: c.Name,
		})
	}

	return &vtadminpb.GetClustersResponse{
		Clusters: vcs,
	}, nil
}

// GetFullStatus is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetFullStatus(ctx context.Context, req *vtadminpb.GetFullStatusRequest) (*vtctldatapb.GetFullStatusResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetFullStatus")
	defer span.Finish()

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.TabletFullStatusResource, rbac.GetAction) {
		return nil, nil
	}

	return c.Vtctld.GetFullStatus(ctx, &vtctldatapb.GetFullStatusRequest{
		TabletAlias: req.Alias,
	})
}

// GetGates is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetGates(ctx context.Context, req *vtadminpb.GetGatesRequest) (*vtadminpb.GetGatesResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetGates")
	defer span.Finish()

	clusters, _ := api.getClustersForRequest(req.ClusterIds)

	var (
		gates []*vtadminpb.VTGate
		wg    sync.WaitGroup
		er    concurrency.AllErrorRecorder
		m     sync.Mutex
	)

	for _, c := range clusters {
		if !api.authz.IsAuthorized(ctx, c.ID, rbac.VTGateResource, rbac.GetAction) {
			continue
		}

		wg.Add(1)

		go func(c *cluster.Cluster) {
			defer wg.Done()

			gs, err := c.GetGates(ctx)
			if err != nil {
				er.RecordError(err)
				return
			}

			m.Lock()
			defer m.Unlock()

			gates = append(gates, gs...)
		}(c)
	}

	wg.Wait()

	if er.HasErrors() {
		return nil, er.Error()
	}

	return &vtadminpb.GetGatesResponse{
		Gates: gates,
	}, nil
}

// GetKeyspace is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetKeyspace(ctx context.Context, req *vtadminpb.GetKeyspaceRequest) (*vtadminpb.Keyspace, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetKeyspace")
	defer span.Finish()

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.KeyspaceResource, rbac.GetAction) {
		return nil, nil
	}

	return c.GetKeyspace(ctx, req.Keyspace)
}

// GetKeyspaces is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetKeyspaces(ctx context.Context, req *vtadminpb.GetKeyspacesRequest) (*vtadminpb.GetKeyspacesResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetKeyspaces")
	defer span.Finish()

	clusters, _ := api.getClustersForRequest(req.ClusterIds)

	var (
		keyspaces []*vtadminpb.Keyspace
		wg        sync.WaitGroup
		er        concurrency.AllErrorRecorder
		m         sync.Mutex
	)

	for _, c := range clusters {
		if !api.authz.IsAuthorized(ctx, c.ID, rbac.KeyspaceResource, rbac.GetAction) {
			continue
		}

		wg.Add(1)

		go func(c *cluster.Cluster) {
			defer wg.Done()

			kss, err := c.GetKeyspaces(ctx)
			if err != nil {
				er.RecordError(err)
				return
			}

			m.Lock()
			keyspaces = append(keyspaces, kss...)
			m.Unlock()
		}(c)
	}

	wg.Wait()

	if er.HasErrors() {
		return nil, er.Error()
	}

	return &vtadminpb.GetKeyspacesResponse{
		Keyspaces: keyspaces,
	}, nil
}

// GetSchema is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetSchema(ctx context.Context, req *vtadminpb.GetSchemaRequest) (*vtadminpb.Schema, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetSchema")
	defer span.Finish()

	span.Annotate("cluster_id", req.ClusterId)
	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("table", req.Table)
	vtadminproto.AnnotateSpanWithGetSchemaTableSizeOptions(req.TableSizeOptions, span)

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.SchemaResource, rbac.GetAction) {
		return nil, nil
	}

	schema, err := c.GetSchema(ctx, req.Keyspace, cluster.GetSchemaOptions{
		BaseRequest: &vtctldatapb.GetSchemaRequest{
			Tables: []string{req.Table},
		},
		TableSizeOptions: req.TableSizeOptions,
	})
	if err != nil {
		return nil, err
	}

	if schema == nil || len(schema.TableDefinitions) == 0 {
		return nil, &errors.NoSuchSchema{
			Clusters: []string{req.ClusterId},
			Table:    req.Table,
		}
	}

	return schema, nil
}

// GetSchemas is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetSchemas(ctx context.Context, req *vtadminpb.GetSchemasRequest) (*vtadminpb.GetSchemasResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetSchemas")
	defer span.Finish()

	clusters, _ := api.getClustersForRequest(req.ClusterIds)

	var (
		schemas []*vtadminpb.Schema
		wg      sync.WaitGroup
		er      concurrency.AllErrorRecorder
		m       sync.Mutex
	)

	for _, c := range clusters {
		if !api.authz.IsAuthorized(ctx, c.ID, rbac.SchemaResource, rbac.GetAction) {
			continue
		}

		wg.Add(1)

		// Get schemas for the cluster
		go func(c *cluster.Cluster) {
			defer wg.Done()

			ss, err := c.GetSchemas(ctx, cluster.GetSchemaOptions{
				TableSizeOptions: req.TableSizeOptions,
			})
			if err != nil {
				er.RecordError(err)
				return
			}

			m.Lock()
			schemas = append(schemas, ss...)
			m.Unlock()
		}(c)
	}

	wg.Wait()

	if er.HasErrors() {
		return nil, er.Error()
	}

	stdsort.Slice(schemas, func(i, j int) bool {
		return schemas[i].Cluster.Id < schemas[j].Cluster.Id
	})

	return &vtadminpb.GetSchemasResponse{
		Schemas: schemas,
	}, nil
}

// GetShardReplicationPositions is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetShardReplicationPositions(ctx context.Context, req *vtadminpb.GetShardReplicationPositionsRequest) (*vtadminpb.GetShardReplicationPositionsResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetShardReplicationPositions")
	defer span.Finish()

	clusters, _ := api.getClustersForRequest(req.ClusterIds)

	var (
		m         sync.Mutex
		wg        sync.WaitGroup
		rec       concurrency.AllErrorRecorder
		positions []*vtadminpb.ClusterShardReplicationPosition
	)

	for _, c := range clusters {
		if !api.authz.IsAuthorized(ctx, c.ID, rbac.ShardReplicationPositionResource, rbac.GetAction) {
			continue
		}

		wg.Add(1)

		go func(c *cluster.Cluster) {
			defer wg.Done()

			clusterPositions, err := c.GetShardReplicationPositions(ctx, req)
			if err != nil {
				rec.RecordError(err)
				return
			}

			m.Lock()
			defer m.Unlock()
			positions = append(positions, clusterPositions...)
		}(c)
	}

	wg.Wait()

	if rec.HasErrors() {
		return nil, rec.Error()
	}

	return &vtadminpb.GetShardReplicationPositionsResponse{
		ReplicationPositions: positions,
	}, nil
}

// GetSrvKeyspace is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetSrvKeyspace(ctx context.Context, req *vtadminpb.GetSrvKeyspaceRequest) (*vtctldatapb.GetSrvKeyspacesResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetSrvKeyspace")
	defer span.Finish()

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.SrvKeyspaceResource, rbac.GetAction) {
		return nil, nil
	}

	return c.Vtctld.GetSrvKeyspaces(ctx, &vtctldatapb.GetSrvKeyspacesRequest{
		Keyspace: req.Keyspace,
		Cells:    req.Cells,
	})
}

// GetSrvKeyspaces is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetSrvKeyspaces(ctx context.Context, req *vtadminpb.GetSrvKeyspacesRequest) (*vtadminpb.GetSrvKeyspacesResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetSrvKeyspaces")
	defer span.Finish()

	clusters, _ := api.getClustersForRequest(req.ClusterIds)

	var (
		sks = make(map[string]*vtctldatapb.GetSrvKeyspacesResponse)
		wg  sync.WaitGroup
		er  concurrency.AllErrorRecorder
		m   sync.Mutex
	)

	for _, c := range clusters {
		if !api.authz.IsAuthorized(ctx, c.ID, rbac.SrvKeyspaceResource, rbac.GetAction) {
			continue
		}

		wg.Add(1)

		go func(c *cluster.Cluster) {
			defer wg.Done()

			span, ctx := trace.NewSpan(ctx, "Cluster.GetSrvKeyspaces")
			defer span.Finish()

			sk, err := c.GetSrvKeyspaces(ctx, req.Cells)

			if err != nil {
				er.RecordError(err)
				return
			}

			m.Lock()
			for key, value := range sk {
				sks[key] = value
			}
			m.Unlock()
		}(c)
	}

	wg.Wait()

	if er.HasErrors() {
		return nil, er.Error()
	}

	return &vtadminpb.GetSrvKeyspacesResponse{
		SrvKeyspaces: sks,
	}, nil
}

// GetSrvVSchema is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetSrvVSchema(ctx context.Context, req *vtadminpb.GetSrvVSchemaRequest) (*vtadminpb.SrvVSchema, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetSrvVSchema")
	defer span.Finish()

	span.Annotate("cluster_id", req.ClusterId)
	span.Annotate("cell", req.Cell)

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.SrvVSchemaResource, rbac.GetAction) {
		return nil, nil
	}

	return c.GetSrvVSchema(ctx, req.Cell)
}

// GetSrvVSchemas is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetSrvVSchemas(ctx context.Context, req *vtadminpb.GetSrvVSchemasRequest) (*vtadminpb.GetSrvVSchemasResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetSrvVSchemas")
	defer span.Finish()

	clusters, _ := api.getClustersForRequest(req.ClusterIds)

	var (
		svs []*vtadminpb.SrvVSchema
		wg  sync.WaitGroup
		er  concurrency.AllErrorRecorder
		m   sync.Mutex
	)

	for _, c := range clusters {
		if !api.authz.IsAuthorized(ctx, c.ID, rbac.SrvVSchemaResource, rbac.GetAction) {
			continue
		}

		wg.Add(1)

		go func(c *cluster.Cluster) {
			defer wg.Done()

			s, err := c.GetSrvVSchemas(ctx, req.Cells)

			if err != nil {
				er.RecordError(err)
				return
			}

			m.Lock()
			svs = append(svs, s...)
			m.Unlock()
		}(c)
	}

	wg.Wait()

	if er.HasErrors() {
		return nil, er.Error()
	}

	return &vtadminpb.GetSrvVSchemasResponse{
		SrvVSchemas: svs,
	}, nil
}

// GetTablet is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetTablet(ctx context.Context, req *vtadminpb.GetTabletRequest) (*vtadminpb.Tablet, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetTablet")
	defer span.Finish()

	t, _, err := api.getTabletForAction(ctx, span, rbac.GetAction, req.Alias, req.ClusterIds)
	return t, err
}

// GetTablets is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetTablets(ctx context.Context, req *vtadminpb.GetTabletsRequest) (*vtadminpb.GetTabletsResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetTablets")
	defer span.Finish()

	clusters, _ := api.getClustersForRequest(req.ClusterIds)

	var (
		tablets []*vtadminpb.Tablet
		wg      sync.WaitGroup
		er      concurrency.AllErrorRecorder
		m       sync.Mutex
	)

	for _, c := range clusters {
		if !api.authz.IsAuthorized(ctx, c.ID, rbac.TabletResource, rbac.GetAction) {
			continue
		}

		wg.Add(1)

		go func(c *cluster.Cluster) {
			defer wg.Done()

			ts, err := c.GetTablets(ctx)
			if err != nil {
				er.RecordError(fmt.Errorf("GetTablets(cluster = %s): %w", c.ID, err))
				return
			}

			m.Lock()
			tablets = append(tablets, ts...)
			m.Unlock()
		}(c)
	}

	wg.Wait()

	if er.HasErrors() {
		return nil, er.Error()
	}

	return &vtadminpb.GetTabletsResponse{
		Tablets: tablets,
	}, nil
}

// GetTopologyPath is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetTopologyPath(ctx context.Context, req *vtadminpb.GetTopologyPathRequest) (*vtctldatapb.GetTopologyPathResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetTopologyPath")
	defer span.Finish()

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	cluster.AnnotateSpan(c, span)

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.TopologyResource, rbac.GetAction) {
		return nil, nil
	}

	return c.Vtctld.GetTopologyPath(ctx, &vtctldatapb.GetTopologyPathRequest{Path: req.Path})
}

// GetVSchema is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetVSchema(ctx context.Context, req *vtadminpb.GetVSchemaRequest) (*vtadminpb.VSchema, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetVSchema")
	defer span.Finish()

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	cluster.AnnotateSpan(c, span)

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.VSchemaResource, rbac.GetAction) {
		return nil, nil
	}

	return c.GetVSchema(ctx, req.Keyspace)
}

// GetVSchemas is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetVSchemas(ctx context.Context, req *vtadminpb.GetVSchemasRequest) (*vtadminpb.GetVSchemasResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetVSchemas")
	defer span.Finish()

	clusters, _ := api.getClustersForRequest(req.ClusterIds)

	var (
		m        sync.Mutex
		wg       sync.WaitGroup
		rec      concurrency.AllErrorRecorder
		vschemas []*vtadminpb.VSchema
	)

	if len(clusters) == 0 {
		if len(req.ClusterIds) > 0 {
			return nil, fmt.Errorf("%w: %s", errors.ErrUnsupportedCluster, strings.Join(req.ClusterIds, ", "))
		}

		return &vtadminpb.GetVSchemasResponse{VSchemas: []*vtadminpb.VSchema{}}, nil
	}

	for _, c := range clusters {
		if !api.authz.IsAuthorized(ctx, c.ID, rbac.VSchemaResource, rbac.GetAction) {
			continue
		}

		wg.Add(1)

		go func(c *cluster.Cluster) {
			defer wg.Done()

			span, ctx := trace.NewSpan(ctx, "Cluster.GetVSchemas")
			defer span.Finish()

			cluster.AnnotateSpan(c, span)

			getKeyspacesSpan, getKeyspacesCtx := trace.NewSpan(ctx, "Cluster.GetKeyspaces")
			cluster.AnnotateSpan(c, getKeyspacesSpan)

			keyspaces, err := c.Vtctld.GetKeyspaces(getKeyspacesCtx, &vtctldatapb.GetKeyspacesRequest{})
			if err != nil {
				rec.RecordError(fmt.Errorf("GetKeyspaces(cluster = %s): %w", c.ID, err))
				getKeyspacesSpan.Finish()
				return
			}

			getKeyspacesSpan.Finish()

			var (
				clusterM        sync.Mutex
				clusterWG       sync.WaitGroup
				clusterRec      concurrency.AllErrorRecorder
				clusterVSchemas = make([]*vtadminpb.VSchema, 0, len(keyspaces.Keyspaces))
			)

			for _, keyspace := range keyspaces.Keyspaces {
				clusterWG.Add(1)

				go func(keyspace *vtctldatapb.Keyspace) {
					defer clusterWG.Done()
					vschema, err := c.GetVSchema(ctx, keyspace.Name)
					if err != nil {
						clusterRec.RecordError(fmt.Errorf("GetVSchema(keyspace = %s): %w", keyspace.Name, err))
						return
					}

					clusterM.Lock()
					clusterVSchemas = append(clusterVSchemas, vschema)
					clusterM.Unlock()
				}(keyspace)
			}

			clusterWG.Wait()

			if clusterRec.HasErrors() {
				rec.RecordError(fmt.Errorf("GetVSchemas(cluster = %s): %w", c.ID, clusterRec.Error()))
				return
			}

			m.Lock()
			vschemas = append(vschemas, clusterVSchemas...)
			m.Unlock()
		}(c)
	}

	wg.Wait()

	if rec.HasErrors() {
		return nil, rec.Error()
	}

	return &vtadminpb.GetVSchemasResponse{
		VSchemas: vschemas,
	}, nil
}

// GetVtctlds is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetVtctlds(ctx context.Context, req *vtadminpb.GetVtctldsRequest) (*vtadminpb.GetVtctldsResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetVtctlds")
	defer span.Finish()

	clusters, _ := api.getClustersForRequest(req.ClusterIds)

	var (
		m       sync.Mutex
		wg      sync.WaitGroup
		rec     concurrency.AllErrorRecorder
		vtctlds []*vtadminpb.Vtctld
	)

	for _, c := range clusters {
		if !api.authz.IsAuthorized(ctx, c.ID, rbac.VtctldResource, rbac.GetAction) {
			continue
		}

		wg.Add(1)
		go func(c *cluster.Cluster) {
			defer wg.Done()

			vs, err := c.GetVtctlds(ctx)
			if err != nil {
				rec.RecordError(err)
				return
			}

			m.Lock()
			defer m.Unlock()

			vtctlds = append(vtctlds, vs...)
		}(c)
	}

	wg.Wait()
	if rec.HasErrors() {
		return nil, rec.Error()
	}

	return &vtadminpb.GetVtctldsResponse{
		Vtctlds: vtctlds,
	}, nil
}

// GetWorkflow is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetWorkflow(ctx context.Context, req *vtadminpb.GetWorkflowRequest) (*vtadminpb.Workflow, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetWorkflow")
	defer span.Finish()

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	cluster.AnnotateSpan(c, span)
	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("workflow_name", req.Name)
	span.Annotate("active_only", req.ActiveOnly)

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.WorkflowResource, rbac.GetAction) {
		return nil, nil
	}

	return c.GetWorkflow(ctx, req.Keyspace, req.Name, cluster.GetWorkflowOptions{
		ActiveOnly: req.ActiveOnly,
	})
}

// GetWorkflows is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetWorkflows(ctx context.Context, req *vtadminpb.GetWorkflowsRequest) (*vtadminpb.GetWorkflowsResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetWorkflows")
	defer span.Finish()

	clusters, _ := api.getClustersForRequest(req.ClusterIds)

	var (
		m       sync.Mutex
		wg      sync.WaitGroup
		rec     concurrency.AllErrorRecorder
		results = map[string]*vtadminpb.ClusterWorkflows{}
	)

	for _, c := range clusters {
		if !api.authz.IsAuthorized(ctx, c.ID, rbac.WorkflowResource, rbac.GetAction) {
			continue
		}

		wg.Add(1)

		go func(c *cluster.Cluster) {
			defer wg.Done()

			workflows, err := c.GetWorkflows(ctx, req.Keyspaces, cluster.GetWorkflowsOptions{
				ActiveOnly:      req.ActiveOnly,
				IgnoreKeyspaces: sets.New[string](req.IgnoreKeyspaces...),
			})
			if err != nil {
				rec.RecordError(err)

				return
			}

			m.Lock()
			results[c.ID] = workflows
			m.Unlock()
		}(c)
	}

	wg.Wait()

	if rec.HasErrors() {
		return nil, rec.Error()
	}

	return &vtadminpb.GetWorkflowsResponse{
		WorkflowsByCluster: results,
	}, nil
}

// PingTablet is part of the vtadminpb.VTAdminServer interface.
func (api *API) PingTablet(ctx context.Context, req *vtadminpb.PingTabletRequest) (*vtadminpb.PingTabletResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.PingTablet")
	defer span.Finish()

	tablet, c, err := api.getTabletForAction(ctx, span, rbac.PingAction, req.Alias, req.ClusterIds)
	if err != nil {
		return nil, err
	}

	cluster.AnnotateSpan(c, span)

	_, err = c.Vtctld.PingTablet(ctx, &vtctldatapb.PingTabletRequest{
		TabletAlias: tablet.Tablet.Alias,
	})

	if err != nil {
		return nil, err
	}

	return &vtadminpb.PingTabletResponse{
		Status:  "ok",
		Cluster: c.ToProto(),
	}, nil
}

// PlannedFailoverShard is part of the vtadminpb.VTAdminServer interface.
func (api *API) PlannedFailoverShard(ctx context.Context, req *vtadminpb.PlannedFailoverShardRequest) (*vtadminpb.PlannedFailoverShardResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.PlannedFailoverShard")
	defer span.Finish()

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.ShardResource, rbac.PlannedFailoverShardAction) {
		return nil, nil
	}

	return c.PlannedFailoverShard(ctx, req.Options)
}

// RebuildKeyspaceGraph is a part of the vtadminpb.VTAdminServer interface.
func (api *API) RebuildKeyspaceGraph(ctx context.Context, req *vtadminpb.RebuildKeyspaceGraphRequest) (*vtadminpb.RebuildKeyspaceGraphResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.RebuildKeyspaceGraph")
	defer span.Finish()

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.KeyspaceResource, rbac.PutAction) {
		return nil, nil
	}

	_, err = c.Vtctld.RebuildKeyspaceGraph(ctx, &vtctldatapb.RebuildKeyspaceGraphRequest{
		Keyspace:     req.Keyspace,
		AllowPartial: req.AllowPartial,
		Cells:        req.Cells,
	})

	if err != nil {
		return nil, err
	}

	return &vtadminpb.RebuildKeyspaceGraphResponse{
		Status: "ok",
	}, nil
}

// RefreshState is part of the vtadminpb.VTAdminServer interface.
func (api *API) RefreshState(ctx context.Context, req *vtadminpb.RefreshStateRequest) (*vtadminpb.RefreshStateResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.RefreshState")
	defer span.Finish()

	tablet, c, err := api.getTabletForAction(ctx, span, rbac.PutAction, req.Alias, req.ClusterIds)
	if err != nil {
		return nil, err
	}

	if err := c.RefreshState(ctx, tablet); err != nil {
		return nil, err
	}

	return &vtadminpb.RefreshStateResponse{
		Status:  "ok",
		Cluster: c.ToProto(),
	}, nil
}

// RefreshTabletReplicationSource is part of the vtadminpb.VTAdminServer interface.
func (api *API) RefreshTabletReplicationSource(ctx context.Context, req *vtadminpb.RefreshTabletReplicationSourceRequest) (*vtadminpb.RefreshTabletReplicationSourceResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.RefreshTabletReplicationSource")
	defer span.Finish()

	tablet, c, err := api.getTabletForAction(ctx, span, rbac.RefreshTabletReplicationSourceAction, req.Alias, req.ClusterIds)
	if err != nil {
		return nil, err
	}

	return c.RefreshTabletReplicationSource(ctx, tablet)
}

// ReloadSchemas is part of the vtadminpb.VTAdminServer interface.
func (api *API) ReloadSchemas(ctx context.Context, req *vtadminpb.ReloadSchemasRequest) (*vtadminpb.ReloadSchemasResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.ReloadSchemas")
	defer span.Finish()

	clusters, _ := api.getClustersForRequest(req.ClusterIds)

	var (
		m    sync.Mutex
		wg   sync.WaitGroup
		rec  concurrency.AllErrorRecorder
		resp vtadminpb.ReloadSchemasResponse
	)

	for _, c := range clusters {
		if !api.authz.IsAuthorized(ctx, c.ID, rbac.SchemaResource, rbac.ReloadAction) {
			continue
		}

		wg.Add(1)

		go func(c *cluster.Cluster) {
			defer wg.Done()

			cr, err := c.ReloadSchemas(ctx, req)
			if err != nil {
				rec.RecordError(fmt.Errorf("ReloadSchemas(cluster = %s) failed: %w", c.ID, err))
				return
			}

			m.Lock()
			defer m.Unlock()
			resp.KeyspaceResults = append(resp.KeyspaceResults, cr.KeyspaceResults...)
			resp.ShardResults = append(resp.ShardResults, cr.ShardResults...)
			resp.TabletResults = append(resp.TabletResults, cr.TabletResults...)
		}(c)
	}

	wg.Wait()
	if rec.HasErrors() {
		return nil, rec.Error()
	}

	return &resp, nil
}

// RemoveKeyspaceCell is a part of the vtadminpb.VTAdminServer interface.
func (api *API) RemoveKeyspaceCell(ctx context.Context, req *vtadminpb.RemoveKeyspaceCellRequest) (*vtadminpb.RemoveKeyspaceCellResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.RemoveKeyspaceCell")
	defer span.Finish()

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.KeyspaceResource, rbac.PutAction) {
		return nil, nil
	}

	_, err = c.Vtctld.RemoveKeyspaceCell(ctx, &vtctldatapb.RemoveKeyspaceCellRequest{
		Keyspace:  req.Keyspace,
		Cell:      req.Cell,
		Force:     req.Force,
		Recursive: req.Recursive,
	})

	if err != nil {
		return nil, err
	}

	return &vtadminpb.RemoveKeyspaceCellResponse{
		Status: "ok",
	}, nil
}

// ReloadSchemaShard is part of the vtadminpb.VTAdminServer interface.
func (api *API) ReloadSchemaShard(ctx context.Context, req *vtadminpb.ReloadSchemaShardRequest) (*vtadminpb.ReloadSchemaShardResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.ReloadSchemas")
	defer span.Finish()

	c, err := api.getClusterForRequest(req.ClusterId)

	if err != nil {
		return nil, err
	}

	res, err := c.Vtctld.ReloadSchemaShard(ctx, &vtctldatapb.ReloadSchemaShardRequest{
		WaitPosition:   req.WaitPosition,
		IncludePrimary: req.IncludePrimary,
		Concurrency:    req.Concurrency,
	})

	if err != nil {
		return nil, err
	}

	return &vtadminpb.ReloadSchemaShardResponse{
		Events: res.Events,
	}, nil
}

// RunHealthCheck is part of the vtadminpb.VTAdminServer interface.
func (api *API) RunHealthCheck(ctx context.Context, req *vtadminpb.RunHealthCheckRequest) (*vtadminpb.RunHealthCheckResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.RunHealthCheck")
	defer span.Finish()

	tablet, c, err := api.getTabletForAction(ctx, span, rbac.GetAction, req.Alias, req.ClusterIds)
	if err != nil {
		return nil, err
	}

	cluster.AnnotateSpan(c, span)

	_, err = c.Vtctld.RunHealthCheck(ctx, &vtctldatapb.RunHealthCheckRequest{
		TabletAlias: tablet.Tablet.Alias,
	})

	if err != nil {
		return nil, err
	}

	return &vtadminpb.RunHealthCheckResponse{
		Status:  "ok",
		Cluster: c.ToProto(),
	}, nil
}

// SetReadOnly is part of the vtadminpb.VTAdminServer interface.
func (api *API) SetReadOnly(ctx context.Context, req *vtadminpb.SetReadOnlyRequest) (*vtadminpb.SetReadOnlyResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.SetReadOnly")
	defer span.Finish()

	tablet, c, err := api.getTabletForAction(ctx, span, rbac.ManageTabletWritabilityAction, req.Alias, req.ClusterIds)
	if err != nil {
		return nil, err
	}

	err = c.SetWritable(ctx, &vtctldatapb.SetWritableRequest{
		TabletAlias: tablet.Tablet.Alias,
		Writable:    false,
	})
	if err != nil {
		return nil, fmt.Errorf("Error setting tablet to read-only: %w", err)
	}

	return &vtadminpb.SetReadOnlyResponse{}, nil
}

// SetReadWrite is part of the vtadminpb.VTAdminServer interface.
func (api *API) SetReadWrite(ctx context.Context, req *vtadminpb.SetReadWriteRequest) (*vtadminpb.SetReadWriteResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.SetReadWrite")
	defer span.Finish()

	tablet, c, err := api.getTabletForAction(ctx, span, rbac.ManageTabletWritabilityAction, req.Alias, req.ClusterIds)
	if err != nil {
		return nil, err
	}

	err = c.SetWritable(ctx, &vtctldatapb.SetWritableRequest{
		TabletAlias: tablet.Tablet.Alias,
		Writable:    true,
	})
	if err != nil {
		return nil, fmt.Errorf("Error setting tablet to read-write: %w", err)
	}

	return &vtadminpb.SetReadWriteResponse{}, nil
}

// StartReplication is part of the vtadminpb.VTAdminServer interface.
func (api *API) StartReplication(ctx context.Context, req *vtadminpb.StartReplicationRequest) (*vtadminpb.StartReplicationResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.StartReplication")
	defer span.Finish()

	tablet, c, err := api.getTabletForAction(ctx, span, rbac.ManageTabletReplicationAction, req.Alias, req.ClusterIds)
	if err != nil {
		return nil, err
	}

	start := true
	if err := c.ToggleTabletReplication(ctx, tablet, start); err != nil {
		return nil, err
	}

	return &vtadminpb.StartReplicationResponse{
		Status:  "ok",
		Cluster: c.ToProto(),
	}, nil
}

// StopReplication is part of the vtadminpb.VTAdminServer interface.
func (api *API) StopReplication(ctx context.Context, req *vtadminpb.StopReplicationRequest) (*vtadminpb.StopReplicationResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.StopReplication")
	defer span.Finish()

	tablet, c, err := api.getTabletForAction(ctx, span, rbac.ManageTabletReplicationAction, req.Alias, req.ClusterIds)
	if err != nil {
		return nil, err
	}

	start := true
	if err := c.ToggleTabletReplication(ctx, tablet, !start); err != nil {
		return nil, err
	}

	return &vtadminpb.StopReplicationResponse{
		Status:  "ok",
		Cluster: c.ToProto(),
	}, nil
}

// TabletExternallyPromoted is part of the vtadminpb.VTAdminServer interface.
func (api *API) TabletExternallyPromoted(ctx context.Context, req *vtadminpb.TabletExternallyPromotedRequest) (*vtadminpb.TabletExternallyPromotedResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.TabletExternallyPromoted")
	defer span.Finish()

	tablet, c, err := api.getTabletForShardAction(ctx, span, rbac.TabletExternallyPromotedAction, req.Alias, req.ClusterIds)
	if err != nil {
		return nil, err
	}

	return c.TabletExternallyPromoted(ctx, tablet)
}

// Validate is part of the vtadminpb.VTAdminServer interface.
func (api *API) Validate(ctx context.Context, req *vtadminpb.ValidateRequest) (*vtctldatapb.ValidateResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.Validate")
	defer span.Finish()

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.ClusterResource, rbac.PutAction) {
		return nil, nil
	}

	res, err := c.Vtctld.Validate(ctx, &vtctldatapb.ValidateRequest{
		PingTablets: req.PingTablets,
	})

	if err != nil {
		return nil, err
	}

	return res, nil
}

// ValidateKeyspace is part of the vtadminpb.VTAdminServer interface.
func (api *API) ValidateKeyspace(ctx context.Context, req *vtadminpb.ValidateKeyspaceRequest) (*vtctldatapb.ValidateKeyspaceResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.ValidateKeyspace")
	defer span.Finish()

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.KeyspaceResource, rbac.PutAction) {
		return nil, nil
	}

	res, err := c.Vtctld.ValidateKeyspace(ctx, &vtctldatapb.ValidateKeyspaceRequest{
		Keyspace:    req.Keyspace,
		PingTablets: req.PingTablets,
	})

	if err != nil {
		return nil, err
	}

	return res, nil
}

// ValidateSchemaKeyspace is part of the vtadminpb.VTAdminServer interface.
func (api *API) ValidateSchemaKeyspace(ctx context.Context, req *vtadminpb.ValidateSchemaKeyspaceRequest) (*vtctldatapb.ValidateSchemaKeyspaceResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.ValidateSchemaKeyspace")
	defer span.Finish()

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.KeyspaceResource, rbac.PutAction) {
		return nil, nil
	}

	res, err := c.Vtctld.ValidateSchemaKeyspace(ctx, &vtctldatapb.ValidateSchemaKeyspaceRequest{
		Keyspace: req.Keyspace,
	})

	if err != nil {
		return nil, err
	}

	return res, nil
}

// ValidateShard is part of the vtadminpb.VTAdminServer interface.
func (api *API) ValidateShard(ctx context.Context, req *vtadminpb.ValidateShardRequest) (*vtctldatapb.ValidateShardResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.ValidateShard")
	defer span.Finish()

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.ShardResource, rbac.PutAction) {
		return nil, nil
	}

	res, err := c.Vtctld.ValidateShard(ctx, &vtctldatapb.ValidateShardRequest{
		Keyspace:    req.Keyspace,
		Shard:       req.Shard,
		PingTablets: req.PingTablets,
	})

	if err != nil {
		return nil, err
	}

	return res, nil
}

// ValidateVersionKeyspace is part of the vtadminpb.VTAdminServer interface.
func (api *API) ValidateVersionKeyspace(ctx context.Context, req *vtadminpb.ValidateVersionKeyspaceRequest) (*vtctldatapb.ValidateVersionKeyspaceResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.ValidateVersionKeyspace")
	defer span.Finish()

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.KeyspaceResource, rbac.PutAction) {
		return nil, nil
	}

	res, err := c.Vtctld.ValidateVersionKeyspace(ctx, &vtctldatapb.ValidateVersionKeyspaceRequest{
		Keyspace: req.Keyspace,
	})

	if err != nil {
		return nil, err
	}

	return res, nil
}

// ValidateVersionShard is part of the vtadminpb.VTAdminServer interface.
func (api *API) ValidateVersionShard(ctx context.Context, req *vtadminpb.ValidateVersionShardRequest) (*vtctldatapb.ValidateVersionShardResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.ValidateVersionShard")
	defer span.Finish()

	c, err := api.getClusterForRequest(req.ClusterId)
	if err != nil {
		return nil, err
	}

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.ShardResource, rbac.PutAction) {
		return nil, nil
	}

	res, err := c.Vtctld.ValidateVersionShard(ctx, &vtctldatapb.ValidateVersionShardRequest{
		Keyspace: req.Keyspace,
		Shard:    req.Shard,
	})

	if err != nil {
		return nil, err
	}

	return res, nil
}

// VTExplain is part of the vtadminpb.VTAdminServer interface.
func (api *API) VTExplain(ctx context.Context, req *vtadminpb.VTExplainRequest) (*vtadminpb.VTExplainResponse, error) {
	// TODO (andrew): https://github.com/vitessio/vitess/issues/12161.
	log.Warningf("VTAdminServer.VTExplain is deprecated; please use a vexplain query instead. For more details, see https://vitess.io/docs/user-guides/sql/vexplain/.")

	span, ctx := trace.NewSpan(ctx, "API.VTExplain")
	defer span.Finish()

	if req.Cluster == "" {
		return nil, fmt.Errorf("%w: cluster ID is required", errors.ErrInvalidRequest)
	}

	if req.Keyspace == "" {
		return nil, fmt.Errorf("%w: keyspace name is required", errors.ErrInvalidRequest)
	}

	if req.Sql == "" {
		return nil, fmt.Errorf("%w: SQL query is required", errors.ErrInvalidRequest)
	}

	c, err := api.getClusterForRequest(req.Cluster)
	if err != nil {
		return nil, err
	}

	span.Annotate("keyspace", req.Keyspace)
	cluster.AnnotateSpan(c, span)

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.VTExplainResource, rbac.GetAction) {
		return nil, nil
	}

	lockWaitStart := time.Now()

	api.vtexplainLock.Lock()
	defer api.vtexplainLock.Unlock()

	lockWaitTime := time.Since(lockWaitStart)
	log.Infof("vtexplain lock wait time: %s", lockWaitTime)

	span.Annotate("vtexplain_lock_wait_time", lockWaitTime.String())

	tablet, err := c.FindTablet(ctx, func(t *vtadminpb.Tablet) bool {
		return t.Tablet.Keyspace == req.Keyspace && topo.IsInServingGraph(t.Tablet.Type) && t.Tablet.Type != topodatapb.TabletType_PRIMARY && t.State == vtadminpb.Tablet_SERVING
	})
	if err != nil {
		return nil, fmt.Errorf("cannot find serving, non-primary tablet in keyspace=%s: %w", req.Keyspace, err)
	}

	span.Annotate("tablet_alias", topoproto.TabletAliasString(tablet.Tablet.Alias))

	var (
		wg sync.WaitGroup
		er concurrency.AllErrorRecorder

		// Writes to these three variables are, in the strictest sense, unsafe.
		// However, there is one goroutine responsible for writing each of these
		// values (so, no concurrent writes), and reads are blocked on the call to
		// wg.Wait(), so we guarantee that all writes have finished before attempting
		// to read anything.
		srvVSchema string
		schema     string
		shardMap   string
	)

	wg.Add(3)

	// GetSchema
	go func(c *cluster.Cluster) {
		defer wg.Done()

		res, err := c.GetSchema(ctx, req.Keyspace, cluster.GetSchemaOptions{})
		if err != nil {
			er.RecordError(fmt.Errorf("GetSchema(%s): %w", topoproto.TabletAliasString(tablet.Tablet.Alias), err))
			return
		}

		schemas := make([]string, len(res.TableDefinitions))
		for i, td := range res.TableDefinitions {
			schemas[i] = td.Schema
		}

		schema = strings.Join(schemas, ";")
	}(c)

	// GetSrvVSchema
	go func(c *cluster.Cluster) {
		defer wg.Done()

		span, ctx := trace.NewSpan(ctx, "Cluster.GetSrvVSchema")
		defer span.Finish()

		span.Annotate("cell", tablet.Tablet.Alias.Cell)
		cluster.AnnotateSpan(c, span)

		res, err := c.Vtctld.GetSrvVSchema(ctx, &vtctldatapb.GetSrvVSchemaRequest{
			Cell: tablet.Tablet.Alias.Cell,
		})

		if err != nil {
			er.RecordError(fmt.Errorf("GetSrvVSchema(%s): %w", tablet.Tablet.Alias.Cell, err))
			return
		}

		ksvs, ok := res.SrvVSchema.Keyspaces[req.Keyspace]
		if !ok {
			er.RecordError(fmt.Errorf("%w: keyspace %s", errors.ErrNoSrvVSchema, req.Keyspace))
			return
		}

		ksvsb, err := json.Marshal(&ksvs)
		if err != nil {
			er.RecordError(err)
			return
		}

		srvVSchema = fmt.Sprintf(`{"%s": %s}`, req.Keyspace, string(ksvsb))
	}(c)

	// FindAllShardsInKeyspace
	go func(c *cluster.Cluster) {
		defer wg.Done()

		shards, err := c.FindAllShardsInKeyspace(ctx, req.Keyspace, cluster.FindAllShardsInKeyspaceOptions{})
		if err != nil {
			er.RecordError(err)
			return
		}

		vtsm := make(map[string]*topodatapb.Shard)
		for _, s := range shards {
			vtsm[s.Name] = s.Shard
		}

		vtsb, err := json.Marshal(&vtsm)
		if err != nil {
			er.RecordError(err)
			return
		}

		shardMap = fmt.Sprintf(`{"%s": %s}`, req.Keyspace, string(vtsb))
	}(c)

	wg.Wait()

	if er.HasErrors() {
		return nil, er.Error()
	}

	vte, err := vtexplain.Init(srvVSchema, schema, shardMap, &vtexplain.Options{ReplicationMode: "ROW"})
	if err != nil {
		return nil, fmt.Errorf("error initilaizing vtexplain: %w", err)
	}
	defer vte.Stop()

	plans, err := vte.Run(req.Sql)
	if err != nil {
		return nil, fmt.Errorf("error running vtexplain: %w", err)
	}

	response, err := vte.ExplainsAsText(plans)
	if err != nil {
		return nil, fmt.Errorf("error converting vtexplain to text output: %w", err)
	}

	return &vtadminpb.VTExplainResponse{
		Response: response,
	}, nil
}

func (api *API) getClusterForRequest(id string) (*cluster.Cluster, error) {
	api.clusterMu.Lock()
	defer api.clusterMu.Unlock()

	c, ok := api.clusterMap[id]
	if !ok {
		return nil, fmt.Errorf("%w: no cluster with id %s", errors.ErrUnsupportedCluster, id)
	}

	return c, nil
}

func (api *API) getClustersForRequest(ids []string) ([]*cluster.Cluster, []string) {
	api.clusterMu.Lock()
	defer api.clusterMu.Unlock()

	if len(ids) == 0 {
		clusterIDs := make([]string, 0, len(api.clusters))

		for k := range api.clusterMap {
			clusterIDs = append(clusterIDs, k)
		}

		return api.clusters, clusterIDs
	}

	clusters := make([]*cluster.Cluster, 0, len(ids))

	for _, id := range ids {
		if c, ok := api.clusterMap[id]; ok {
			clusters = append(clusters, c)
		}
	}

	return clusters, ids
}

func (api *API) getTabletForAction(ctx context.Context, span trace.Span, action rbac.Action, alias *topodatapb.TabletAlias, clusterIDs []string) (*vtadminpb.Tablet, *cluster.Cluster, error) {
	return api.getTabletForResourceAndAction(ctx, span, rbac.TabletResource, action, alias, clusterIDs)
}

func (api *API) getTabletForShardAction(ctx context.Context, span trace.Span, action rbac.Action, alias *topodatapb.TabletAlias, clusterIDs []string) (*vtadminpb.Tablet, *cluster.Cluster, error) {
	return api.getTabletForResourceAndAction(ctx, span, rbac.ShardResource, action, alias, clusterIDs)
}

func (api *API) getTabletForResourceAndAction(
	ctx context.Context,
	span trace.Span,
	resource rbac.Resource,
	action rbac.Action,
	alias *topodatapb.TabletAlias,
	clusterIDs []string,
) (*vtadminpb.Tablet, *cluster.Cluster, error) {
	span.Annotate("tablet_alias", topoproto.TabletAliasString(alias))
	span.Annotate("tablet_cell", alias.Cell)
	span.Annotate("tablet_uid", alias.Uid)

	clusters, ids := api.getClustersForRequest(clusterIDs)

	var (
		m   sync.Mutex
		wg  sync.WaitGroup
		rec concurrency.AllErrorRecorder

		tablets []*vtadminpb.Tablet
	)

	for _, c := range clusters {
		if !api.authz.IsAuthorized(ctx, c.ID, resource, action) {
			continue
		}

		wg.Add(1)

		go func(c *cluster.Cluster) {
			defer wg.Done()

			ts, err := c.FindTablets(ctx, func(t *vtadminpb.Tablet) bool {
				return topoproto.TabletAliasEqual(t.Tablet.Alias, alias)
			}, -1)
			if err != nil {
				rec.RecordError(fmt.Errorf("FindTablets(cluster = %s): %w", c.ID, err))
				return
			}

			m.Lock()
			tablets = append(tablets, ts...)
			m.Unlock()
		}(c)
	}

	wg.Wait()

	if rec.HasErrors() {
		return nil, nil, rec.Error()
	}

	switch len(tablets) {
	case 0:
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "%s: %s, searched clusters = %v", errors.ErrNoTablet, alias, ids)
	case 1:
		t := tablets[0]
		for _, c := range clusters {
			if c.ID == t.Cluster.Id {
				return t, c, nil
			}
		}

		return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "impossible: found tablet from cluster %s but cannot find cluster with that id", t.Cluster.Id)
	}

	return nil, nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "%s: %s, searched clusters = %v", errors.ErrAmbiguousTablet, alias, ids)
}
