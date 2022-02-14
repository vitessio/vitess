package vtadminproto

import (
	"vitess.io/vitess/go/trace"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// AnnotateClusterSpan adds the cluster_id and cluster_name to a span.
func AnnotateClusterSpan(c *vtadminpb.Cluster, span trace.Span) {
	span.Annotate("cluster_id", c.Id)
	span.Annotate("cluster_name", c.Name)
}

// AnnotateSpanWithGetSchemaTableSizeOptions adds the aggregate_table_sizes to a
// span. It is a noop if the size options object is nil.
func AnnotateSpanWithGetSchemaTableSizeOptions(opts *vtadminpb.GetSchemaTableSizeOptions, span trace.Span) {
	if opts == nil {
		opts = &vtadminpb.GetSchemaTableSizeOptions{}
	}

	span.Annotate("aggregate_table_sizes", opts.AggregateSizes)
	span.Annotate("include_non_serving_shards", opts.IncludeNonServingShards)
}
