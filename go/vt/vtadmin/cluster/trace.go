/*
Copyright 2021 The Vitess Authors.

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

package cluster

import (
	"strings"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtadmin/vtadminproto"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// AnnotateSpan adds the cluster_id and cluster_name to a span.
func AnnotateSpan(c *Cluster, span trace.Span) {
	vtadminproto.AnnotateClusterSpan(c.ToProto(), span)
	// (TODO:@ajm188) add support for discovery impls to add annotations to a
	// span, like `discovery_impl` and any parameters that might be relevant.
}

// (TODO: @ajm188) perhaps we want a ./go/vt/vtctl/vtctlproto package for this?
func annotateGetSchemaRequest(req *vtctldatapb.GetSchemaRequest, span trace.Span) {
	if req.TabletAlias != nil {
		span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))
	}

	span.Annotate("exclude_tables", strings.Join(req.ExcludeTables, ","))
	span.Annotate("tables", strings.Join(req.Tables, ","))
	span.Annotate("include_views", req.IncludeViews)
	span.Annotate("table_names_only", req.TableNamesOnly)
	span.Annotate("table_sizes_only", req.TableSizesOnly)
}
