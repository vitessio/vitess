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

package topo

import (
	"context"
	"time"
)

// SetCellInfoCacheTTL sets how long ts's StaleTolerantView reuses a cell's CellInfo.
func (ts *Server) SetCellInfoCacheTTL(ttl time.Duration) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.cellInfoCacheTTL = ttl
}

// ExpireCellInfo makes the next StaleTolerantView read for cell re-read its CellInfo.
func (ts *Server) ExpireCellInfo(cell string) {
	ts.expireCellInfo(cell)
}

// CellReader returns the connection that ts's StaleTolerantView reads cell's records through.
func (ts *Server) CellReader(ctx context.Context, cell string) (any, error) {
	return ts.cellReaderFor(ctx, cell)
}
