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

package http

import (
	"context"
	"net/http"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/vtadmin/cache"
	"vitess.io/vitess/go/vt/vtadmin/rbac"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// Options defines the set of configurations for an HTTP API server.
type Options struct {
	// CORSOrigins is the list of origins to allow via CORS. An empty or nil
	// slice disables CORS entirely.
	CORSOrigins []string
	// EnableTracing specifies whether to install a tracing middleware on the
	// API subrouter.
	EnableTracing bool
	// DisableCompression specifies whether to turn off gzip compression for API
	// endpoints. It is named as the negative (as opposed to EnableTracing) so
	// the zero value has compression enabled.
	DisableCompression bool
	// DisableDebug specifies whether to omit the /debug/pprof/* and /debug/env
	// routes.
	DisableDebug        bool
	ExperimentalOptions struct {
		TabletURLTmpl string
	}
}

// API is used to power HTTP endpoint wrappers to the VTAdminServer interface.
type API struct {
	server vtadminpb.VTAdminServer
	opts   Options
}

// NewAPI returns an HTTP API backed by the given VTAdminServer implementation.
func NewAPI(server vtadminpb.VTAdminServer, opts Options) *API {
	return &API{server: server, opts: opts}
}

// VTAdminHandler is an HTTP endpoint handler that takes, via injection,
// everything needed to implement a JSON API response.
type VTAdminHandler func(ctx context.Context, r Request, api *API) *JSONResponse

// Adapt converts a VTAdminHandler into an http.HandlerFunc. It deals with
// wrapping the request in a wrapper for some convenience functions and starts
// a new context, after extracting any potential spans that were set by an
// upstream middleware in the request context.
func (api *API) Adapt(handler VTAdminHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()

		span, _ := trace.FromContext(r.Context())
		if span != nil {
			ctx = trace.NewContext(ctx, span)
		}

		actor, _ := rbac.FromContext(r.Context())
		if actor != nil {
			ctx = rbac.NewContext(ctx, actor)
		}

		if cache.ShouldRefreshFromRequest(r) {
			ctx = cache.NewIncomingRefreshContext(ctx)
		}

		handler(ctx, Request{r}, api).Write(w)
	}
}

// Options returns a copy of the Options this API was configured with.
func (api *API) Options() Options {
	return api.opts
}

// Server returns the VTAdminServer wrapped by this API.
func (api *API) Server() vtadminpb.VTAdminServer {
	return api.server
}
