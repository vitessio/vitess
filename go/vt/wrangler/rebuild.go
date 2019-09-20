/*
Copyright 2019 The Vitess Authors.

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

package wrangler

import (
	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/topotools"
)

// RebuildKeyspaceGraph rebuilds the serving graph data while locking out other changes.
func (wr *Wrangler) RebuildKeyspaceGraph(ctx context.Context, keyspace string, cells []string) error {
	return topotools.RebuildKeyspace(ctx, wr.logger, wr.ts, keyspace, cells)
}
