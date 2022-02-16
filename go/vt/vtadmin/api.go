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
	"encoding/base64"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/http"
	"net/http/pprof"
	stdsort "sort"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/patrickmn/go-cache"
	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
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

	clusters     []*cluster.Cluster
	clusterCache *cache.Cache
	clusterMap   map[string]*cluster.Cluster
	serv         *grpcserver.Server
	router       *mux.Router

	authz *rbac.Authorizer

	// See https://github.com/vitessio/vitess/issues/7723 for why this exists.
	vtexplainLock sync.Mutex
	options       Options
}

// Options wraps the configuration options for different components of the
// vtadmin API.
type Options struct {
	GRPCOpts grpcserver.Options
	HTTPOpts vtadminhttp.Options
	RBAC     *rbac.Config
}

type DynamicClusterJSON struct {
	ClusterName string `json:"name,omitempty"`
}

// NewAPI returns a new API, configured to service the given set of clusters,
// and configured with the given options.
//
// If opts.GRPCOpts.Services is nil, NewAPI will automatically add
// "vtadmin.VTAdminServer" to the list of services queryable in the healthcheck
// service. Callers can opt-out of this behavior by explicitly setting this
// value to the empty slice.
func NewAPI(clusters []*cluster.Cluster, opts Options) *API {
	clusterMap := make(map[string]*cluster.Cluster, len(clusters))
	for _, cluster := range clusters {
		clusterMap[cluster.ID] = cluster
	}

	sort.ClustersBy(func(c1, c2 *cluster.Cluster) bool {
		return c1.ID < c2.ID
	}).Sort(clusters)

	if opts.GRPCOpts.Services == nil {
		opts.GRPCOpts.Services = []string{"vtadmin.VTAdminServer"}
	}

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

	serv := grpcserver.New("vtadmin", opts.GRPCOpts)
	serv.Router().HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})

	router := serv.Router().PathPrefix("/api").Subrouter()
	api := &API{
		clusters:   clusters,
		clusterMap: clusterMap,
		router:     router,
		serv:       serv,
		authz:      authz,
		options:    opts,
	}
	router.PathPrefix("/").Handler(api).Methods("DELETE", "OPTIONS", "GET", "POST", "PUT")
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

	if opts.HTTPOpts.EnableDynamicClusters {
		api.clusterCache = cache.New(24*time.Hour, 24*time.Hour)
		api.clusterCache.OnEvicted(api.EjectDynamicCluster)
	}

	router.Use(middlewares...)

	return api
}

// ListenAndServe starts serving this API on the configured Addr (see
// grpcserver.Options) until shutdown or irrecoverable error occurs.
func (api *API) ListenAndServe() error {
	return api.serv.ListenAndServe()
}

// ServeHTTP serves all routes matching path "/api" (see above)
// It first processes cookies, and acts accordingly
// Primarily, it sets up a dynamic API if HttpOpts.EnableDynamicClusters is set to true
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !api.options.HTTPOpts.EnableDynamicClusters {
		api.Handler().ServeHTTP(w, r)
		return
	}
	dynamicAPI := &API{
		clusters:   api.clusters,
		clusterMap: api.clusterMap,
		router:     api.router,
		serv:       api.serv,
		authz:      api.authz,
		options:    api.options,
	}

	clusterCookie, err := r.Cookie("cluster")

	if err == nil {
		decoded, err := base64.StdEncoding.DecodeString(clusterCookie.Value)
		if err == nil {
			var clusterJSON DynamicClusterJSON
			err = json.Unmarshal(decoded, &clusterJSON)
			if err == nil {
				clusterID := clusterJSON.ClusterName
				c, err := cluster.Config{
					ID:            clusterID,
					Name:          clusterID,
					DiscoveryImpl: "dynamic",
					DiscoveryFlagsByImpl: cluster.FlagsByImpl{
						"dynamic": map[string]string{
							"discovery": string(decoded),
						},
					},
				}.Cluster()
				if err == nil {
					api.clusterMap[clusterID] = c
					api.clusters = append(api.clusters, c)
					err = api.clusterCache.Add(clusterID, c, 24*time.Hour)
					if err != nil {
						log.Infof("could not add dynamic cluster %s to cluster cache: %+v", clusterID, err)
					}
				}
				selectedCluster := api.clusterMap[clusterID]
				dynamicAPI.clusters = []*cluster.Cluster{selectedCluster}
				dynamicAPI.clusterMap = map[string]*cluster.Cluster{clusterID: selectedCluster}
			}
		}
	}

	defer dynamicAPI.Close()
	dynamicAPI.Handler().ServeHTTP(w, r)
}

func (api *API) Close() error {
	return nil
}

// Handler handles all routes under "/api" (see above)
func (api *API) Handler() http.Handler {
	router := mux.NewRouter().PathPrefix("/api").Subrouter()

	router.Use(handlers.CORS(
		handlers.AllowCredentials(), handlers.AllowedOrigins(api.options.HTTPOpts.CORSOrigins), handlers.AllowedMethods([]string{"GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS"})))

	httpAPI := vtadminhttp.NewAPI(api, api.options.HTTPOpts)

	router.HandleFunc("/backups", httpAPI.Adapt(vtadminhttp.GetBackups)).Name("API.GetBackups")
	router.HandleFunc("/clusters", httpAPI.Adapt(vtadminhttp.GetClusters)).Name("API.GetClusters")
	router.HandleFunc("/gates", httpAPI.Adapt(vtadminhttp.GetGates)).Name("API.GetGates")
	router.HandleFunc("/keyspace/{cluster_id}", httpAPI.Adapt(vtadminhttp.CreateKeyspace)).Name("API.CreateKeyspace").Methods("POST")
	router.HandleFunc("/keyspace/{cluster_id}/{name}", httpAPI.Adapt(vtadminhttp.DeleteKeyspace)).Name("API.DeleteKeyspace").Methods("DELETE")
	router.HandleFunc("/keyspace/{cluster_id}/{name}", httpAPI.Adapt(vtadminhttp.GetKeyspace)).Name("API.GetKeyspace")
	router.HandleFunc("/keyspace/{cluster_id}/{name}/validate", httpAPI.Adapt(vtadminhttp.ValidateKeyspace)).Name("API.ValidateKeyspace").Methods("PUT", "OPTIONS")
	router.HandleFunc("/keyspace/{cluster_id}/{name}/validate_schema", httpAPI.Adapt(vtadminhttp.ValidateSchemaKeyspace)).Name("API.ValidateSchemaKeyspace").Methods("PUT", "OPTIONS")
	router.HandleFunc("/keyspace/{cluster_id}/{name}/validate_version", httpAPI.Adapt(vtadminhttp.ValidateVersionKeyspace)).Name("API.ValidateVersionKeyspace").Methods("PUT", "OPTIONS")
	router.HandleFunc("/keyspaces", httpAPI.Adapt(vtadminhttp.GetKeyspaces)).Name("API.GetKeyspaces")
	router.HandleFunc("/schema/{table}", httpAPI.Adapt(vtadminhttp.FindSchema)).Name("API.FindSchema")
	router.HandleFunc("/schema/{cluster_id}/{keyspace}/{table}", httpAPI.Adapt(vtadminhttp.GetSchema)).Name("API.GetSchema")
	router.HandleFunc("/schemas", httpAPI.Adapt(vtadminhttp.GetSchemas)).Name("API.GetSchemas")
	router.HandleFunc("/shard_replication_positions", httpAPI.Adapt(vtadminhttp.GetShardReplicationPositions)).Name("API.GetShardReplicationPositions")
	router.HandleFunc("/shards/{cluster_id}", httpAPI.Adapt(vtadminhttp.CreateShard)).Name("API.CreateShard").Methods("POST")
	router.HandleFunc("/shards/{cluster_id}", httpAPI.Adapt(vtadminhttp.DeleteShards)).Name("API.DeleteShards").Methods("DELETE")
	router.HandleFunc("/srvvschema/{cluster_id}/{cell}", httpAPI.Adapt(vtadminhttp.GetSrvVSchema)).Name("API.GetSrvVSchema")
	router.HandleFunc("/srvvschemas", httpAPI.Adapt(vtadminhttp.GetSrvVSchemas)).Name("API.GetSrvVSchemas")
	router.HandleFunc("/tablets", httpAPI.Adapt(vtadminhttp.GetTablets)).Name("API.GetTablets")
	router.HandleFunc("/tablet/{tablet}", httpAPI.Adapt(vtadminhttp.GetTablet)).Name("API.GetTablet").Methods("GET")
	router.HandleFunc("/tablet/{tablet}", httpAPI.Adapt(vtadminhttp.DeleteTablet)).Name("API.DeleteTablet").Methods("DELETE", "OPTIONS")
	router.HandleFunc("/tablet/{tablet}/healthcheck", httpAPI.Adapt(vtadminhttp.RunHealthCheck)).Name("API.RunHealthCheck")
	router.HandleFunc("/tablet/{tablet}/ping", httpAPI.Adapt(vtadminhttp.PingTablet)).Name("API.PingTablet")
	router.HandleFunc("/tablet/{tablet}/refresh", httpAPI.Adapt(vtadminhttp.RefreshState)).Name("API.RefreshState").Methods("PUT", "OPTIONS")
	router.HandleFunc("/tablet/{tablet}/reparent", httpAPI.Adapt(vtadminhttp.ReparentTablet)).Name("API.ReparentTablet").Methods("PUT", "OPTIONS")
	router.HandleFunc("/tablet/{tablet}/set_read_only", httpAPI.Adapt(vtadminhttp.SetReadOnly)).Name("API.SetReadOnly").Methods("PUT", "OPTIONS")
	router.HandleFunc("/tablet/{tablet}/set_read_write", httpAPI.Adapt(vtadminhttp.SetReadWrite)).Name("API.SetReadWrite").Methods("PUT", "OPTIONS")
	router.HandleFunc("/tablet/{tablet}/start_replication", httpAPI.Adapt(vtadminhttp.StartReplication)).Name("API.StartReplication").Methods("PUT", "OPTIONS")
	router.HandleFunc("/tablet/{tablet}/stop_replication", httpAPI.Adapt(vtadminhttp.StopReplication)).Name("API.StopReplication").Methods("PUT", "OPTIONS")
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

func (api *API) EjectDynamicCluster(key string, value interface{}) {
	// Delete dynamic clusters from clusterMap when they are expired from clusterCache
	_, ok := api.clusterMap[key]
	if ok {
		delete(api.clusterMap, key)
	}
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

	if err := c.Vtctld.Dial(ctx); err != nil {
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

	if err := c.Vtctld.Dial(ctx); err != nil {
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

	if err := c.Vtctld.Dial(ctx); err != nil {
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

	if err := c.Vtctld.Dial(ctx); err != nil {
		return nil, err
	}

	return c.DeleteShards(ctx, req.Options)
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

			tablets, err := c.FindTablets(ctx, func(t *vtadminpb.Tablet) bool {
				// Filter out all the non-serving tablets once, to make the
				// later, per-keyspace filtering slightly faster (fewer
				// potentially-redundant iterations).
				return t.State == vtadminpb.Tablet_SERVING
			}, -1)
			if err != nil {
				err := fmt.Errorf("could not find any serving tablets for cluster %s: %w", c.ID, err)
				rec.RecordError(err)

				return
			}

			schemas, err := api.getSchemas(ctx, c, cluster.GetSchemaOptions{
				Tablets:          tablets,
				TableSizeOptions: req.TableSizeOptions,
			})
			if err != nil {
				err := fmt.Errorf("%w: while collecting schemas for cluster %s", err, c.ID)
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

// GetClusters is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetClusters(ctx context.Context, req *vtadminpb.GetClustersRequest) (*vtadminpb.GetClustersResponse, error) {
	span, _ := trace.NewSpan(ctx, "API.GetClusters")
	defer span.Finish()

	vcs := make([]*vtadminpb.Cluster, 0, len(api.clusters))

	for _, c := range api.clusters {
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

	c, ok := api.clusterMap[req.ClusterId]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errors.ErrUnsupportedCluster, req.ClusterId)
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

// ValidateKeyspace validates that all nodes reachable from the specified keyspace are consistent.
func (api *API) ValidateKeyspace(ctx context.Context, req *vtadminpb.ValidateKeyspaceRequest) (*vtctldatapb.ValidateKeyspaceResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.ValidateKeyspace")
	defer span.Finish()

	c, ok := api.clusterMap[req.ClusterId]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errors.ErrUnsupportedCluster, req.ClusterId)
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

// ValidateSchemaKeyspace validates that the schema on the primary tablet for shard 0 matches the schema on all of the other tablets in the keyspace
func (api *API) ValidateSchemaKeyspace(ctx context.Context, req *vtadminpb.ValidateSchemaKeyspaceRequest) (*vtctldatapb.ValidateSchemaKeyspaceResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.ValidateSchemaKeyspace")
	defer span.Finish()

	c, ok := api.clusterMap[req.ClusterId]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errors.ErrUnsupportedCluster, req.ClusterId)
	}

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.KeyspaceResource, rbac.PutAction) {
		return nil, nil
	}

	if err := c.Vtctld.Dial(ctx); err != nil {
		return nil, err
	}

	res, err := c.Vtctld.ValidateSchemaKeyspace(ctx, &vtctldatapb.ValidateSchemaKeyspaceRequest{
		Keyspace: req.Keyspace,
	})

	if err != nil {
		return nil, err
	}

	return res, nil
}

// ValidateVersionKeyspace validates that the version on the primary of shard 0 matches all of the other tablets in the keyspace.
func (api *API) ValidateVersionKeyspace(ctx context.Context, req *vtadminpb.ValidateVersionKeyspaceRequest) (*vtctldatapb.ValidateVersionKeyspaceResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.ValidateVersionKeyspace")
	defer span.Finish()

	c, ok := api.clusterMap[req.ClusterId]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errors.ErrUnsupportedCluster, req.ClusterId)
	}

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.KeyspaceResource, rbac.PutAction) {
		return nil, nil
	}

	if err := c.Vtctld.Dial(ctx); err != nil {
		return nil, err
	}

	res, err := c.Vtctld.ValidateVersionKeyspace(ctx, &vtctldatapb.ValidateVersionKeyspaceRequest{
		Keyspace: req.Keyspace,
	})

	if err != nil {
		return nil, err
	}

	return res, nil
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

			// Since tablets are per-cluster, we can fetch them once
			// and use them throughout the other waitgroups.
			tablets, err := c.GetTablets(ctx)
			if err != nil {
				er.RecordError(err)
				return
			}

			ss, err := api.getSchemas(ctx, c, cluster.GetSchemaOptions{
				Tablets:          tablets,
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

// getSchemas returns all of the schemas across all keyspaces in the given cluster.
func (api *API) getSchemas(ctx context.Context, c *cluster.Cluster, opts cluster.GetSchemaOptions) ([]*vtadminpb.Schema, error) {
	if err := c.Vtctld.Dial(ctx); err != nil {
		return nil, err
	}

	getKeyspacesSpan, getKeyspacesCtx := trace.NewSpan(ctx, "Cluster.GetKeyspaces")
	cluster.AnnotateSpan(c, getKeyspacesSpan)

	resp, err := c.Vtctld.GetKeyspaces(getKeyspacesCtx, &vtctldatapb.GetKeyspacesRequest{})
	if err != nil {
		getKeyspacesSpan.Finish()
		return nil, err
	}

	getKeyspacesSpan.Finish()

	var (
		schemas []*vtadminpb.Schema
		wg      sync.WaitGroup
		er      concurrency.AllErrorRecorder
		m       sync.Mutex
	)

	for _, ks := range resp.Keyspaces {
		wg.Add(1)

		// Get schemas for the cluster/keyspace
		go func(c *cluster.Cluster, ks *vtctldatapb.Keyspace) {
			defer wg.Done()

			ss, err := c.GetSchema(ctx, ks.Name, opts)
			if err != nil {
				// Ignore keyspaces without any serving tablets.
				if stderrors.Is(err, errors.ErrNoServingTablet) {
					log.Infof(err.Error())
					return
				}

				er.RecordError(err)
				return
			}

			// Ignore keyspaces without schemas
			if ss == nil {
				log.Infof("No schemas for %s", ks.Name)
				return
			}

			if len(ss.TableDefinitions) == 0 {
				log.Infof("No tables in schema for %s", ks.Name)
				return
			}

			m.Lock()
			schemas = append(schemas, ss)
			m.Unlock()
		}(c, ks)
	}

	wg.Wait()

	if er.HasErrors() {
		return nil, er.Error()
	}

	return schemas, nil
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

// GetSrvVSchema is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetSrvVSchema(ctx context.Context, req *vtadminpb.GetSrvVSchemaRequest) (*vtadminpb.SrvVSchema, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetSrvVSchema")
	defer span.Finish()

	span.Annotate("cluster_id", req.ClusterId)
	span.Annotate("cell", req.Cell)

	c, ok := api.clusterMap[req.ClusterId]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errors.ErrUnsupportedCluster, req.ClusterId)
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

	return api.getTabletForAction(ctx, span, rbac.GetAction, req.Alias, req.ClusterIds)
}

func (api *API) DeleteTablet(ctx context.Context, req *vtadminpb.DeleteTabletRequest) (*vtadminpb.DeleteTabletResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.DeleteTablet")
	defer span.Finish()

	tablet, err := api.getTabletForAction(ctx, span, rbac.DeleteAction, req.Alias, req.ClusterIds)
	if err != nil {
		return nil, err
	}

	c, ok := api.clusterMap[tablet.Cluster.Id]
	if !ok {
		return nil, fmt.Errorf("%w: no such cluster %s", errors.ErrUnsupportedCluster, tablet.Cluster.Id)
	}

	cluster.AnnotateSpan(c, span)

	if err := c.Vtctld.Dial(ctx); err != nil {
		return nil, err
	}

	_, err = c.Vtctld.DeleteTablets(ctx, &vtctldatapb.DeleteTabletsRequest{
		TabletAliases: []*topodatapb.TabletAlias{
			tablet.Tablet.Alias,
		},
	})

	if err != nil {
		return nil, fmt.Errorf("Error deleting tablet: %w", err)
	}

	return &vtadminpb.DeleteTabletResponse{Status: "ok"}, nil
}

func (api *API) ReparentTablet(ctx context.Context, req *vtadminpb.ReparentTabletRequest) (*vtadminpb.ReparentTabletResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.ReparentTablet")
	defer span.Finish()

	tablet, err := api.getTabletForAction(ctx, span, rbac.PutAction, req.Alias, req.ClusterIds)
	if err != nil {
		return nil, err
	}

	c, ok := api.clusterMap[tablet.Cluster.Id]
	if !ok {
		return nil, fmt.Errorf("%w: no such cluster %s", errors.ErrUnsupportedCluster, tablet.Cluster.Id)
	}

	cluster.AnnotateSpan(c, span)

	if err := c.Vtctld.Dial(ctx); err != nil {
		return nil, err
	}

	r, err := c.Vtctld.ReparentTablet(ctx, &vtctldatapb.ReparentTabletRequest{
		Tablet: tablet.Tablet.Alias,
	})

	if err != nil {
		return nil, fmt.Errorf("Error reparenting tablet: %w", err)
	}

	return &vtadminpb.ReparentTabletResponse{Keyspace: r.Keyspace, Primary: r.Primary.String(), Shard: r.Shard}, nil
}

// PingTablet is part of the vtadminpb.VTAdminServer interface.
func (api *API) RunHealthCheck(ctx context.Context, req *vtadminpb.RunHealthCheckRequest) (*vtadminpb.RunHealthCheckResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.RunHealthCheck")
	defer span.Finish()

	tablet, err := api.getTabletForAction(ctx, span, rbac.GetAction, req.Alias, req.ClusterIds)
	if err != nil {
		return nil, err
	}

	c, ok := api.clusterMap[tablet.Cluster.Id]
	if !ok {
		return nil, fmt.Errorf("%w: no such cluster %s", errors.ErrUnsupportedCluster, tablet.Cluster.Id)
	}

	cluster.AnnotateSpan(c, span)

	if err := c.Vtctld.Dial(ctx); err != nil {
		return nil, err
	}

	_, err = c.Vtctld.RunHealthCheck(ctx, &vtctldatapb.RunHealthCheckRequest{
		TabletAlias: tablet.Tablet.Alias,
	})

	if err != nil {
		return nil, fmt.Errorf("Error running health check on tablet: %w", err)
	}

	return &vtadminpb.RunHealthCheckResponse{Status: "ok"}, nil
}

// PingTablet is part of the vtadminpb.VTAdminServer interface.
func (api *API) PingTablet(ctx context.Context, req *vtadminpb.PingTabletRequest) (*vtadminpb.PingTabletResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.PingTablet")
	defer span.Finish()

	tablet, err := api.getTabletForAction(ctx, span, rbac.PingAction, req.Alias, req.ClusterIds)
	if err != nil {
		return nil, err
	}

	c, ok := api.clusterMap[tablet.Cluster.Id]
	if !ok {
		return nil, fmt.Errorf("%w: no such cluster %s", errors.ErrUnsupportedCluster, tablet.Cluster.Id)
	}

	cluster.AnnotateSpan(c, span)

	if err := c.Vtctld.Dial(ctx); err != nil {
		return nil, err
	}

	_, err = c.Vtctld.PingTablet(ctx, &vtctldatapb.PingTabletRequest{
		TabletAlias: tablet.Tablet.Alias,
	})

	if err != nil {
		return nil, fmt.Errorf("Error pinging cluster: %w", err)
	}

	return &vtadminpb.PingTabletResponse{Status: "ok"}, nil
}

// SetReadOnly sets the tablet to read only mode
func (api *API) SetReadOnly(ctx context.Context, req *vtadminpb.SetReadOnlyRequest) (*vtadminpb.SetReadOnlyResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.SetReadOnly")
	defer span.Finish()

	tablet, err := api.getTabletForAction(ctx, span, rbac.PutAction, req.Alias, req.ClusterIds)
	if err != nil {
		return nil, err
	}

	c, ok := api.clusterMap[tablet.Cluster.Id]
	if !ok {
		return nil, fmt.Errorf("%w: no such cluster %s", errors.ErrUnsupportedCluster, tablet.Cluster.Id)
	}

	cluster.AnnotateSpan(c, span)

	if err := c.Vtctld.Dial(ctx); err != nil {
		return nil, err
	}

	_, err = c.Vtctld.SetWritable(ctx, &vtctldatapb.SetWritableRequest{
		TabletAlias: tablet.Tablet.Alias,
		Writable:    false,
	})

	if err != nil {
		return nil, fmt.Errorf("Error setting tablet to read-only: %w", err)
	}

	return &vtadminpb.SetReadOnlyResponse{}, nil
}

// SetReadWrite sets the tablet to read-write mode
func (api *API) SetReadWrite(ctx context.Context, req *vtadminpb.SetReadWriteRequest) (*vtadminpb.SetReadWriteResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.SetReadWrite")
	defer span.Finish()

	tablet, err := api.getTabletForAction(ctx, span, rbac.PutAction, req.Alias, req.ClusterIds)
	if err != nil {
		return nil, err
	}

	c, ok := api.clusterMap[tablet.Cluster.Id]
	if !ok {
		return nil, fmt.Errorf("%w: no such cluster %s", errors.ErrUnsupportedCluster, tablet.Cluster.Id)
	}

	cluster.AnnotateSpan(c, span)

	if err := c.Vtctld.Dial(ctx); err != nil {
		return nil, err
	}

	_, err = c.Vtctld.SetWritable(ctx, &vtctldatapb.SetWritableRequest{
		TabletAlias: tablet.Tablet.Alias,
		Writable:    true,
	})

	if err != nil {
		return nil, fmt.Errorf("Error setting tablet to read-write: %w", err)
	}

	return &vtadminpb.SetReadWriteResponse{}, nil
}

// StartReplication starts replication on the specified tablet.
func (api *API) StartReplication(ctx context.Context, req *vtadminpb.StartReplicationRequest) (*vtadminpb.StartReplicationResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.StartReplication")
	defer span.Finish()

	tablet, err := api.getTabletForAction(ctx, span, rbac.PutAction, req.Alias, req.ClusterIds)
	if err != nil {
		return nil, err
	}

	c, ok := api.clusterMap[tablet.Cluster.Id]
	if !ok {
		return nil, fmt.Errorf("%w: no such cluster %s", errors.ErrUnsupportedCluster, tablet.Cluster.Id)
	}

	cluster.AnnotateSpan(c, span)

	if err := c.Vtctld.Dial(ctx); err != nil {
		return nil, err
	}

	_, err = c.Vtctld.StartReplication(ctx, &vtctldatapb.StartReplicationRequest{
		TabletAlias: tablet.Tablet.Alias,
	})

	if err != nil {
		return nil, fmt.Errorf("Error starting replication: %w", err)
	}

	return &vtadminpb.StartReplicationResponse{Status: "ok"}, nil
}

// StopReplication stops replication on the specified tablet.
func (api *API) StopReplication(ctx context.Context, req *vtadminpb.StopReplicationRequest) (*vtadminpb.StopReplicationResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.StopReplication")
	defer span.Finish()

	tablet, err := api.getTabletForAction(ctx, span, rbac.PutAction, req.Alias, req.ClusterIds)
	if err != nil {
		return nil, err
	}

	c, ok := api.clusterMap[tablet.Cluster.Id]
	if !ok {
		return nil, fmt.Errorf("%w: no such cluster %s", errors.ErrUnsupportedCluster, tablet.Cluster.Id)
	}

	cluster.AnnotateSpan(c, span)

	if err := c.Vtctld.Dial(ctx); err != nil {
		return nil, err
	}

	_, err = c.Vtctld.StopReplication(ctx, &vtctldatapb.StopReplicationRequest{
		TabletAlias: tablet.Tablet.Alias,
	})

	if err != nil {
		return nil, fmt.Errorf("Error stopping replication: %w", err)
	}

	return &vtadminpb.StopReplicationResponse{Status: "ok"}, nil
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

// GetVSchema is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetVSchema(ctx context.Context, req *vtadminpb.GetVSchemaRequest) (*vtadminpb.VSchema, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetVSchema")
	defer span.Finish()

	c, ok := api.clusterMap[req.ClusterId]
	if !ok {
		return nil, fmt.Errorf("%w: no such cluster %s", errors.ErrUnsupportedCluster, req.ClusterId)
	}

	cluster.AnnotateSpan(c, span)

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.VSchemaResource, rbac.GetAction) {
		return nil, nil
	}

	if err := c.Vtctld.Dial(ctx); err != nil {
		return nil, err
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

			if err := c.Vtctld.Dial(ctx); err != nil {
				rec.RecordError(fmt.Errorf("Vtctld.Dial(cluster = %s): %w", c.ID, err))
				return
			}

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

	c, ok := api.clusterMap[req.ClusterId]
	if !ok {
		return nil, fmt.Errorf("%w: no such cluster %s", errors.ErrUnsupportedCluster, req.ClusterId)
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
				IgnoreKeyspaces: sets.NewString(req.IgnoreKeyspaces...),
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

// RefreshState reloads the tablet record on the specified tablet.
func (api *API) RefreshState(ctx context.Context, req *vtadminpb.RefreshStateRequest) (*vtadminpb.RefreshStateResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.RefreshState")
	defer span.Finish()

	tablet, err := api.getTabletForAction(ctx, span, rbac.PutAction, req.Alias, req.ClusterIds)
	if err != nil {
		return nil, err
	}

	c, ok := api.clusterMap[tablet.Cluster.Id]
	if !ok {
		return nil, fmt.Errorf("%w: no such cluster %s", errors.ErrUnsupportedCluster, tablet.Cluster.Id)
	}

	cluster.AnnotateSpan(c, span)

	if err := c.Vtctld.Dial(ctx); err != nil {
		return nil, err
	}

	_, err = c.Vtctld.RefreshState(ctx, &vtctldatapb.RefreshStateRequest{
		TabletAlias: tablet.Tablet.Alias,
	})

	if err != nil {
		return nil, fmt.Errorf("Error pinging cluster: %w", err)
	}

	return &vtadminpb.RefreshStateResponse{Status: "ok"}, nil
}

// VTExplain is part of the vtadminpb.VTAdminServer interface.
func (api *API) VTExplain(ctx context.Context, req *vtadminpb.VTExplainRequest) (*vtadminpb.VTExplainResponse, error) {
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

	c, ok := api.clusterMap[req.Cluster]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errors.ErrUnsupportedCluster, req.Cluster)
	}

	span.Annotate("keyspace", req.Keyspace)
	cluster.AnnotateSpan(c, span)

	if !api.authz.IsAuthorized(ctx, c.ID, rbac.VTExplainResource, rbac.GetAction) {
		return nil, nil
	}

	tablet, err := c.FindTablet(ctx, func(t *vtadminpb.Tablet) bool {
		return t.Tablet.Keyspace == req.Keyspace && topo.IsInServingGraph(t.Tablet.Type) && t.Tablet.Type != topodatapb.TabletType_PRIMARY && t.State == vtadminpb.Tablet_SERVING
	})
	if err != nil {
		return nil, fmt.Errorf("cannot find serving, non-primary tablet in keyspace=%s: %w", req.Keyspace, err)
	}

	span.Annotate("tablet_alias", topoproto.TabletAliasString(tablet.Tablet.Alias))

	if err := c.Vtctld.Dial(ctx); err != nil {
		return nil, err
	}

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

		res, err := c.GetSchema(ctx, req.Keyspace, cluster.GetSchemaOptions{
			Tablets: []*vtadminpb.Tablet{tablet},
		})
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

		shards, err := c.FindAllShardsInKeyspace(ctx, req.Keyspace, cluster.FindAllShardsInKeyspaceOptions{
			SkipDial: true,
		})
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

	opts := &vtexplain.Options{ReplicationMode: "ROW"}

	lockWaitStart := time.Now()

	api.vtexplainLock.Lock()
	defer api.vtexplainLock.Unlock()

	lockWaitTime := time.Since(lockWaitStart)
	log.Infof("vtexplain lock wait time: %s", lockWaitTime)

	span.Annotate("vtexplain_lock_wait_time", lockWaitTime.String())

	if err := vtexplain.Init(srvVSchema, schema, shardMap, opts); err != nil {
		return nil, fmt.Errorf("error initilaizing vtexplain: %w", err)
	}

	defer vtexplain.Stop()

	plans, err := vtexplain.Run(req.Sql)
	if err != nil {
		return nil, fmt.Errorf("error running vtexplain: %w", err)
	}

	response, err := vtexplain.ExplainsAsText(plans)
	if err != nil {
		return nil, fmt.Errorf("error converting vtexplain to text output: %w", err)
	}
	return &vtadminpb.VTExplainResponse{
		Response: response,
	}, nil
}

func (api *API) getClusterForRequest(id string) (*cluster.Cluster, error) {
	c, ok := api.clusterMap[id]
	if !ok {
		return nil, fmt.Errorf("%w: no cluster with id %s", errors.ErrUnsupportedCluster, id)
	}

	return c, nil
}

func (api *API) getClustersForRequest(ids []string) ([]*cluster.Cluster, []string) {
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

func (api *API) getTabletForAction(ctx context.Context, span trace.Span, action rbac.Action, alias string, clusterIds []string) (*vtadminpb.Tablet, error) {
	span.Annotate("tablet_alias", alias)

	tabletAlias, err := topoproto.ParseTabletAlias(alias)
	if err != nil {
		return nil, fmt.Errorf("failed to parse tablet_alias %s: %w", alias, err)
	}

	span.Annotate("tablet_cell", tabletAlias.Cell)
	span.Annotate("tablet_uid", tabletAlias.Uid)

	clusters, ids := api.getClustersForRequest(clusterIds)

	var (
		tablets []*vtadminpb.Tablet
		wg      sync.WaitGroup
		er      concurrency.AllErrorRecorder
		m       sync.Mutex
	)

	for _, c := range clusters {
		if !api.authz.IsAuthorized(ctx, c.ID, rbac.TabletResource, action) {
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

			var found []*vtadminpb.Tablet

			for _, t := range ts {
				if t.Tablet.Alias.Cell == tabletAlias.Cell && t.Tablet.Alias.Uid == tabletAlias.Uid {
					found = append(found, t)
				}
			}

			m.Lock()
			tablets = append(tablets, found...)
			m.Unlock()
		}(c)
	}

	wg.Wait()

	if er.HasErrors() {
		return nil, er.Error()
	}

	switch len(tablets) {
	case 0:
		return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "%s: %s, searched clusters = %v", errors.ErrNoTablet, alias, ids)
	case 1:
		return tablets[0], nil
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "%s: %s, searched clusters = %v", errors.ErrAmbiguousTablet, alias, ids)
}
