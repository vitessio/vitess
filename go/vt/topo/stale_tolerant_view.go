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

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// cellReader reads a cell's local topo records. It cannot write or lock, so a connection that may be to a
// cell's old topo server cannot be used to change anything.
type cellReader interface {
	Get(ctx context.Context, filePath string) ([]byte, Version, error)
	List(ctx context.Context, filePathPrefix string) ([]KVInfo, error)
	ListDir(ctx context.Context, dirPath string, full bool) ([]DirEntry, error)
}

// cellReaderFunc returns a cellReader for a cell. Server.connReaderForCell and Server.cellReaderFor are the two
// implementations; the read functions shared by the Server and its StaleTolerantView take one as a parameter.
type cellReaderFunc func(ctx context.Context, cell string) (cellReader, error)

// connReaderForCell is ConnForCell as a cellReaderFunc.
func (ts *Server) connReaderForCell(ctx context.Context, cell string) (cellReader, error) {
	return ts.ConnForCell(ctx, cell)
}

// StaleTolerantView reads cell-local records through a connection chosen from a CellInfo up to
// --topo-cell-info-cache-ttl old, rather than reading the cell's CellInfo from the global topo on every call.
// After a cell is re-pointed or deleted by another process, its reads may come from the cell's old topo
// server for up to the TTL, so it is only for callers that can act on such data, like the tablet throttler
// choosing which tablets to probe. With the TTL at 0, it reads exactly like the Server.
type StaleTolerantView struct {
	ts *Server
}

// StaleTolerantView returns ts's StaleTolerantView.
func (ts *Server) StaleTolerantView() *StaleTolerantView {
	return &StaleTolerantView{ts: ts}
}

// GetTablet is Server.GetTablet, reading the tablet's cell location from the CellInfo cache.
func (v *StaleTolerantView) GetTablet(ctx context.Context, alias *topodatapb.TabletAlias) (*TabletInfo, error) {
	return getTablet(ctx, alias, v.ts.cellReaderFor)
}

// FindAllTabletAliasesInShard is Server.FindAllTabletAliasesInShard, reading each cell's location from the
// CellInfo cache.
func (v *StaleTolerantView) FindAllTabletAliasesInShard(ctx context.Context, keyspace, shard string) ([]*topodatapb.TabletAlias, error) {
	return v.ts.findAllTabletAliasesInShardByCell(ctx, keyspace, shard, nil, v.ts.cellReaderFor)
}

// cellReaderFor returns a cellReader for the cell, from a CellInfo read within the cache TTL if there is one.
// Otherwise it reads the CellInfo and connects like ConnForCell, and records the read for the next call. With
// the TTL at 0, it is ConnForCell.
//
// Calls that find the cell's CellInfo stale each read it; the last to finish records its read, which may be
// older than another's. The view is read-only, so the worst that does is leave its reads on the cell's old
// topo server for up to the TTL, which is the view's contract anyway. ConnForCell has the same race between
// two of its own calls.
func (ts *Server) cellReaderFor(ctx context.Context, cell string) (cellReader, error) {
	if cell == GlobalCell {
		return ts.ConnForCell(ctx, cell)
	}
	ts.mu.Lock()
	if ts.cellInfoCacheTTL <= 0 {
		ts.mu.Unlock()
		return ts.ConnForCell(ctx, cell)
	}
	conn, ok := ts.freshCellConnLocked(cell)
	generation := ts.cellInfoGeneration
	ts.mu.Unlock()
	if ok {
		return conn, nil
	}

	readStart := time.Now()
	ci, err := ts.GetCellInfo(ctx, cell, false /*strongRead*/)
	if err != nil {
		return nil, err
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()
	conn, err = ts.connForCellInfoLocked(cell, ci)
	if err != nil {
		return nil, err
	}
	// If a CellInfo was expired while we read, what we read may predate the change, so leave it for the next
	// call to read again.
	if ts.cellInfoGeneration == generation {
		ts.cellInfoReads[cell] = cellInfoRead{cellInfo: ci, readStart: readStart}
	}
	return conn, nil
}

// freshCellConnLocked returns the cell's cached connection if it is to the topo server of a CellInfo that
// cellReaderFor read within the TTL. ts.mu must be held.
func (ts *Server) freshCellConnLocked(cell string) (Conn, bool) {
	read, ok := ts.cellInfoReads[cell]
	if !ok || time.Since(read.readStart) >= ts.cellInfoCacheTTL {
		return nil, false
	}
	// ConnForCell may have replaced the connection since, after reading a different CellInfo.
	cc, ok := ts.cellConns[cell]
	if !ok || cc.cellInfo.ServerAddress != read.cellInfo.ServerAddress || cc.cellInfo.Root != read.cellInfo.Root {
		return nil, false
	}
	return cc.conn, true
}

// expireCellInfo makes the next cellReaderFor for cell read its CellInfo from the global topo again, and a
// cellReaderFor that is reading it now not record what it read.
func (ts *Server) expireCellInfo(cell string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	delete(ts.cellInfoReads, cell)
	ts.cellInfoGeneration++
}
