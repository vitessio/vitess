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
