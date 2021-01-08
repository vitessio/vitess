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
	"net/http"
	"sync"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
	"vitess.io/vitess/go/vt/vtadmin/grpcserver"
	vtadminhttp "vitess.io/vitess/go/vt/vtadmin/http"
	vthandlers "vitess.io/vitess/go/vt/vtadmin/http/handlers"
	"vitess.io/vitess/go/vt/vtadmin/sort"
	"vitess.io/vitess/go/vt/vterrors"

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
}

// NewAPI returns a new API, configured to service the given set of clusters,
// and configured with the given gRPC and HTTP server options.
func NewAPI(clusters []*cluster.Cluster, opts grpcserver.Options, httpOpts vtadminhttp.Options) *API {
	clusterMap := make(map[string]*cluster.Cluster, len(clusters))
	for _, cluster := range clusters {
		clusterMap[cluster.ID] = cluster
	}

	sort.ClustersBy(func(c1, c2 *cluster.Cluster) bool {
		return c1.ID < c2.ID
	}).Sort(clusters)

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

	router.HandleFunc("/gates", httpAPI.Adapt(vtadminhttp.GetGates)).Name("API.GetGates")
	router.HandleFunc("/keyspaces", httpAPI.Adapt(vtadminhttp.GetKeyspaces)).Name("API.GetKeyspaces")
	router.HandleFunc("/tablets", httpAPI.Adapt(vtadminhttp.GetTablets)).Name("API.GetTablets")
	router.HandleFunc("/tablet/{tablet}", httpAPI.Adapt(vtadminhttp.GetTablet)).Name("API.GetTablet")

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

			g, err := c.Discovery.DiscoverVTGates(ctx, []string{})
			if err != nil {
				er.RecordError(err)
				return
			}

			m.Lock()
			gates = append(gates, g...)
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

			resp, err := c.Vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
			if err != nil {
				er.RecordError(err)
				return
			}

			m.Lock()
			for _, ks := range resp.Keyspaces {
				keyspaces = append(keyspaces, &vtadminpb.Keyspace{
					Cluster:  c.ToProto(),
					Keyspace: ks,
				})
			}
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

			ts, err := api.getTablets(ctx, c)
			if err != nil {
				er.RecordError(err)
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
		return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "%s: %s, searched clusters = %v", ErrNoTablet, req.Hostname, ids)
	case 1:
		return tablets[0], nil
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "%s: %s, searched clusters = %v", ErrAmbiguousTablet, req.Hostname, ids)
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

			ts, err := api.getTablets(ctx, c)
			if err != nil {
				er.RecordError(err)
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

func (api *API) getTablets(ctx context.Context, c *cluster.Cluster) ([]*vtadminpb.Tablet, error) {
	if err := c.DB.Dial(ctx, ""); err != nil {
		return nil, err
	}

	rows, err := c.DB.ShowTablets(ctx)
	if err != nil {
		return nil, err
	}

	return ParseTablets(rows, c)
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
