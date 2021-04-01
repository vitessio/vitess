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
	stderrors "errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
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
	vthandlers "vitess.io/vitess/go/vt/vtadmin/http/handlers"
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
	clusters   []*cluster.Cluster
	clusterMap map[string]*cluster.Cluster
	serv       *grpcserver.Server
	router     *mux.Router

	// See https://github.com/vitessio/vitess/issues/7723 for why this exists.
	vtexplainLock sync.Mutex
}

// NewAPI returns a new API, configured to service the given set of clusters,
// and configured with the given gRPC and HTTP server options.
//
// If opts.Services is nil, NewAPI will automatically add
// "vtadmin.VTAdminServer" to the list of services queryable in the healthcheck
// service. Callers can opt-out of this behavior by explicitly setting this
// value to the empty slice.
func NewAPI(clusters []*cluster.Cluster, opts grpcserver.Options, httpOpts vtadminhttp.Options) *API {
	clusterMap := make(map[string]*cluster.Cluster, len(clusters))
	for _, cluster := range clusters {
		clusterMap[cluster.ID] = cluster
	}

	sort.ClustersBy(func(c1, c2 *cluster.Cluster) bool {
		return c1.ID < c2.ID
	}).Sort(clusters)

	if opts.Services == nil {
		opts.Services = []string{"vtadmin.VTAdminServer"}
	}

	serv := grpcserver.New("vtadmin", opts)
	serv.Router().HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})

	router := serv.Router().PathPrefix("/api").Subrouter()

	api := &API{
		clusters:   clusters,
		clusterMap: clusterMap,
		router:     router,
		serv:       serv,
	}

	vtadminpb.RegisterVTAdminServer(serv.GRPCServer(), api)

	httpAPI := vtadminhttp.NewAPI(api)

	router.HandleFunc("/clusters", httpAPI.Adapt(vtadminhttp.GetClusters)).Name("API.GetClusters")
	router.HandleFunc("/gates", httpAPI.Adapt(vtadminhttp.GetGates)).Name("API.GetGates")
	router.HandleFunc("/keyspaces", httpAPI.Adapt(vtadminhttp.GetKeyspaces)).Name("API.GetKeyspaces")
	router.HandleFunc("/schema/{table}", httpAPI.Adapt(vtadminhttp.FindSchema)).Name("API.FindSchema")
	router.HandleFunc("/schema/{cluster_id}/{keyspace}/{table}", httpAPI.Adapt(vtadminhttp.GetSchema)).Name("API.GetSchema")
	router.HandleFunc("/schemas", httpAPI.Adapt(vtadminhttp.GetSchemas)).Name("API.GetSchemas")
	router.HandleFunc("/tablets", httpAPI.Adapt(vtadminhttp.GetTablets)).Name("API.GetTablets")
	router.HandleFunc("/tablet/{tablet}", httpAPI.Adapt(vtadminhttp.GetTablet)).Name("API.GetTablet")
	router.HandleFunc("/vschema/{cluster_id}/{keyspace}", httpAPI.Adapt(vtadminhttp.GetVSchema)).Name("API.GetVSchema")
	router.HandleFunc("/vschemas", httpAPI.Adapt(vtadminhttp.GetVSchemas)).Name("API.GetVSchemas")
	router.HandleFunc("/vtexplain", httpAPI.Adapt(vtadminhttp.VTExplain)).Name("API.VTExplain")
	router.HandleFunc("/workflow/{cluster_id}/{keyspace}/{name}", httpAPI.Adapt(vtadminhttp.GetWorkflow)).Name("API.GetWorkflow")
	router.HandleFunc("/workflows", httpAPI.Adapt(vtadminhttp.GetWorkflows)).Name("API.GetWorkflows")

	// Middlewares are executed in order of addition. Our ordering (all
	// middlewares being optional) is:
	// 	1. CORS. CORS is a special case and is applied globally, the rest are applied only to the subrouter.
	//	2. Compression
	//	3. Tracing
	middlewares := []mux.MiddlewareFunc{}

	if len(httpOpts.CORSOrigins) > 0 {
		serv.Router().Use(handlers.CORS(
			handlers.AllowCredentials(), handlers.AllowedOrigins(httpOpts.CORSOrigins)))
	}

	if !httpOpts.DisableCompression {
		middlewares = append(middlewares, handlers.CompressHandler)
	}

	if httpOpts.EnableTracing {
		middlewares = append(middlewares, vthandlers.TraceHandler)
	}

	router.Use(middlewares...)

	return api
}

// ListenAndServe starts serving this API on the configured Addr (see
// grpcserver.Options) until shutdown or irrecoverable error occurs.
func (api *API) ListenAndServe() error {
	return api.serv.ListenAndServe()
}

// FindSchema is part of the vtadminpb.VTAdminServer interface.
func (api *API) FindSchema(ctx context.Context, req *vtadminpb.FindSchemaRequest) (*vtadminpb.Schema, error) {
	span, _ := trace.NewSpan(ctx, "API.FindSchema")
	defer span.Finish()

	span.Annotate("table", req.Table)

	clusters, _ := api.getClustersForRequest(req.ClusterIds)

	var (
		m       sync.Mutex
		wg      sync.WaitGroup
		rec     concurrency.AllErrorRecorder
		results []*vtadminpb.Schema
	)

	for _, c := range clusters {
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
		return nil, fmt.Errorf("%w: no schemas found with table named %s", errors.ErrNoSchema, req.Table)
	case 1:
		return results[0], nil
	default:
		return nil, fmt.Errorf("%w: %d schemas found with table named %s", errors.ErrAmbiguousSchema, len(results), req.Table)
	}
}

// GetClusters is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetClusters(ctx context.Context, req *vtadminpb.GetClustersRequest) (*vtadminpb.GetClustersResponse, error) {
	span, _ := trace.NewSpan(ctx, "API.GetClusters")
	defer span.Finish()

	vcs := make([]*vtadminpb.Cluster, 0, len(api.clusters))

	for _, c := range api.clusters {
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
		wg.Add(1)

		go func(c *cluster.Cluster) {
			defer wg.Done()

			gs, err := c.Discovery.DiscoverVTGates(ctx, []string{})
			if err != nil {
				er.RecordError(fmt.Errorf("DiscoverVTGates(cluster = %s): %w", c.ID, err))
				return
			}

			m.Lock()

			for _, g := range gs {
				gates = append(gates, &vtadminpb.VTGate{
					Cell: g.Cell,
					Cluster: &vtadminpb.Cluster{
						Id:   c.ID,
						Name: c.Name,
					},
					Hostname:  g.Hostname,
					Keyspaces: g.Keyspaces,
					Pool:      g.Pool,
				})
			}

			m.Unlock()
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
		wg.Add(1)

		go func(c *cluster.Cluster) {
			defer wg.Done()

			if err := c.Vtctld.Dial(ctx); err != nil {
				er.RecordError(err)
				return
			}

			getKeyspacesSpan, getKeyspacesCtx := trace.NewSpan(ctx, "Cluster.GetKeyspaces")
			cluster.AnnotateSpan(c, getKeyspacesSpan)

			resp, err := c.Vtctld.GetKeyspaces(getKeyspacesCtx, &vtctldatapb.GetKeyspacesRequest{})
			if err != nil {
				er.RecordError(fmt.Errorf("GetKeyspaces(cluster = %s): %w", c.ID, err))
				getKeyspacesSpan.Finish()
				return
			}

			getKeyspacesSpan.Finish()

			kss := make([]*vtadminpb.Keyspace, 0, len(resp.Keyspaces))

			var (
				kwg sync.WaitGroup
				km  sync.Mutex
			)

			for _, ks := range resp.Keyspaces {
				kwg.Add(1)

				// Find all shards for each keyspace in the cluster, in parallel
				go func(c *cluster.Cluster, ks *vtctldatapb.Keyspace) {
					defer kwg.Done()

					shards, err := c.FindAllShardsInKeyspace(ctx, ks.Name, cluster.FindAllShardsInKeyspaceOptions{
						SkipDial: true,
					})

					if err != nil {
						er.RecordError(err)
						return
					}

					km.Lock()
					kss = append(kss, &vtadminpb.Keyspace{
						Cluster:  c.ToProto(),
						Keyspace: ks,
						Shards:   shards,
					})
					km.Unlock()
				}(c, ks)
			}

			kwg.Wait()

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

	c, ok := api.clusterMap[req.ClusterId]
	if !ok {
		return nil, fmt.Errorf("%w: no cluster with id %s", errors.ErrUnsupportedCluster, req.ClusterId)
	}

	return c.GetSchema(ctx, req.Keyspace, cluster.GetSchemaOptions{
		BaseRequest: &vtctldatapb.GetSchemaRequest{
			Tables: []string{req.Table},
		},
		TableSizeOptions: req.TableSizeOptions,
	})
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

// GetTablet is part of the vtadminpb.VTAdminServer interface.
func (api *API) GetTablet(ctx context.Context, req *vtadminpb.GetTabletRequest) (*vtadminpb.Tablet, error) {
	span, ctx := trace.NewSpan(ctx, "API.GetTablet")
	defer span.Finish()

	span.Annotate("tablet_hostname", req.Hostname)

	clusters, ids := api.getClustersForRequest(req.ClusterIds)

	var (
		tablets []*vtadminpb.Tablet
		wg      sync.WaitGroup
		er      concurrency.AllErrorRecorder
		m       sync.Mutex
	)

	for _, c := range clusters {
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
				if t.Tablet.Hostname == req.Hostname {
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
		return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "%s: %s, searched clusters = %v", errors.ErrNoTablet, req.Hostname, ids)
	case 1:
		return tablets[0], nil
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "%s: %s, searched clusters = %v", errors.ErrAmbiguousTablet, req.Hostname, ids)
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

	tablet, err := c.FindTablet(ctx, func(t *vtadminpb.Tablet) bool {
		return t.Tablet.Keyspace == req.Keyspace && topo.IsInServingGraph(t.Tablet.Type) && t.Tablet.Type != topodatapb.TabletType_MASTER && t.State == vtadminpb.Tablet_SERVING
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

	response := vtexplain.ExplainsAsText(plans)
	return &vtadminpb.VTExplainResponse{
		Response: response,
	}, nil
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
