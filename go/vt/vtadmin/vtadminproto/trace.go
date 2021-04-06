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
}
