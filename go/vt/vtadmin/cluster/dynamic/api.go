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

package dynamic

import (
	"context"
	"net/http"

	"vitess.io/vitess/go/vt/vtadmin/cluster"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// API is the interface dynamic APIs must implement.
// It is implemented by vtadmin.API.
type API interface {
	vtadminpb.VTAdminServer
	// WithCluster returns an API that serves only the cluster with the given
	// id. If id names a statically configured cluster, that cluster is used
	// and cfg is ignored. Otherwise, cfg (parsed from spec) is built into a
	// cluster that only APIs returned for the same spec use.
	WithCluster(ctx context.Context, id string, cfg *cluster.Config, spec string) (API, error)
	Handler() http.Handler
}
