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
	"fmt"
	"path"
	"sort"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// cellReader reads a cell's local topo records. It cannot write or lock, so a connection that may be to a
// cell's old topo server cannot be used to change anything.
type cellReader interface {
	Get(ctx context.Context, filePath string) ([]byte, Version, error)
	List(ctx context.Context, filePathPrefix string) ([]KVInfo, error)
	ListDir(ctx context.Context, dirPath string, full bool) ([]DirEntry, error)
	Watch(ctx context.Context, filePath string) (current *WatchData, changes <-chan *WatchData, err error)
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
	conn, err := v.ts.cellReaderFor(ctx, alias.Cell)
	if err != nil {
		log.Error(fmt.Sprintf("unable to get connection for cell %q: %v", alias.Cell, err))
		return nil, err
	}

	span, ctx := trace.NewSpan(ctx, "TopoServer.StaleTolerantView.GetTablet")
	span.Annotate("tablet", topoproto.TabletAliasString(alias))
	defer span.Finish()

	tabletPath := path.Join(TabletsPath, topoproto.TabletAliasString(alias), TabletFile)
	data, version, err := conn.Get(ctx, tabletPath)
	if err != nil {
		log.Error(fmt.Sprintf("unable to connect to tablet %q: %v", alias, err))
		return nil, err
	}
	tablet := &topodatapb.Tablet{}
	if err := tablet.UnmarshalVT(data); err != nil {
		return nil, err
	}

	return &TabletInfo{
		version: version,
		Tablet:  tablet,
	}, nil
}

// FindAllTabletAliasesInShard is Server.FindAllTabletAliasesInShard, reading each cell's location from the
// CellInfo cache.
func (v *StaleTolerantView) FindAllTabletAliasesInShard(ctx context.Context, keyspace, shard string) ([]*topodatapb.TabletAlias, error) {
	span, ctx := trace.NewSpan(ctx, "topo.StaleTolerantView.FindAllTabletAliasesInShard")
	span.Annotate("keyspace", keyspace)
	span.Annotate("shard", shard)
	defer span.Finish()

	cells, err := v.ts.GetCellInfoNames(ctx)
	if err != nil {
		return nil, err
	}

	// read the shard information to find the cells
	si, err := v.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return nil, err
	}

	resultAsMap := make(map[string]*topodatapb.TabletAlias)
	if si.HasPrimary() {
		if InCellList(si.PrimaryAlias.Cell, cells) {
			resultAsMap[topoproto.TabletAliasString(si.PrimaryAlias)] = si.PrimaryAlias
		}
	}

	// read the replication graph in each cell and add all found tablets
	wg := sync.WaitGroup{}
	mutex := sync.Mutex{}
	rec := concurrency.AllErrorRecorder{}
	for _, cell := range cells {
		wg.Go(func() {
			sri, err := v.getShardReplication(ctx, cell, keyspace, shard)
			switch {
			case err == nil:
				mutex.Lock()
				for _, node := range sri.Nodes {
					resultAsMap[topoproto.TabletAliasString(node.TabletAlias)] = node.TabletAlias
				}
				mutex.Unlock()
			case IsErrType(err, NoNode):
				// There is no shard replication for this shard in this cell. NOOP
			default:
				rec.RecordError(vterrors.Wrapf(err, "GetShardReplication(%v, %v, %v) failed.", cell, keyspace, shard))
			}
		})
	}
	wg.Wait()
	err = nil
	if rec.HasErrors() {
		log.Warn(fmt.Sprintf("FindAllTabletAliasesInShard(%v,%v): got partial result: %v", keyspace, shard, rec.Error()))
		err = NewError(PartialResult, shard)
	}

	result := make([]*topodatapb.TabletAlias, 0, len(resultAsMap))
	for _, a := range resultAsMap {
		result = append(result, a.CloneVT())
	}
	sort.Sort(topoproto.TabletAliasList(result))
	return result, err
}

// getShardReplication is Server.GetShardReplication, reading the cell's location from the CellInfo cache.
func (v *StaleTolerantView) getShardReplication(ctx context.Context, cell, keyspace, shard string) (*ShardReplicationInfo, error) {
	conn, err := v.ts.cellReaderFor(ctx, cell)
	if err != nil {
		return nil, err
	}

	nodePath := path.Join(KeyspacesPath, keyspace, ShardsPath, shard, ShardReplicationFile)
	data, _, err := conn.Get(ctx, nodePath)
	if err != nil {
		return nil, err
	}

	sr := &topodatapb.ShardReplication{}
	if err = sr.UnmarshalVT(data); err != nil {
		return nil, vterrors.Wrap(err, "bad ShardReplication data")
	}

	return NewShardReplicationInfo(sr, cell, keyspace, shard), nil
}

// cellReaderFor returns a cellReader for the cell, from a CellInfo read within the cache TTL if there is one.
// With the TTL at 0, it is ConnForCell.
func (ts *Server) cellReaderFor(ctx context.Context, cell string) (cellReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cell == GlobalCell {
		return ts.ConnForCell(ctx, cell)
	}
	ts.mu.Lock()
	if ts.cellInfoCacheTTL <= 0 {
		ts.mu.Unlock()
		return ts.ConnForCell(ctx, cell)
	}
	conn, ok := ts.freshCellConnLocked(cell)
	ts.mu.Unlock()
	if ok {
		return conn, nil
	}
	return ts.refreshCellConn(ctx, cell)
}

// freshCellConnLocked returns the cell's cached connection if it is to the topo server of a CellInfo that
// cellReaderFor read within the TTL. ts.mu must be held.
func (ts *Server) freshCellConnLocked(cell string) (Conn, bool) {
	read, ok := ts.cellInfoReads[cell]
	if !ok || read.expired || time.Since(read.readStart) >= ts.cellInfoCacheTTL {
		return nil, false
	}
	// ConnForCell may have replaced the connection since, after reading a different CellInfo.
	cc, ok := ts.cellConns[cell]
	if !ok || cc.cellInfo.ServerAddress != read.cellInfo.ServerAddress || cc.cellInfo.Root != read.cellInfo.Root {
		return nil, false
	}
	return cc.conn, true
}

// refreshCellConn reads the cell's CellInfo and returns the cell's connection for it, recording the read for
// cellReaderFor. It returns the cached connection without reading if another call refreshed it while this one
// waited.
func (ts *Server) refreshCellConn(ctx context.Context, cell string) (Conn, error) {
	ts.mu.Lock()
	refresh, ok := ts.cellInfoRefreshes[cell]
	if !ok {
		refresh = semaphore.NewWeighted(1)
		ts.cellInfoRefreshes[cell] = refresh
	}
	ts.mu.Unlock()

	// Refresh the cell one call at a time, so that what we read is never older than what was last recorded.
	if err := refresh.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer refresh.Release(1)
	ts.mu.Lock()
	// Another call may have refreshed the cell while we waited.
	if conn, ok := ts.freshCellConnLocked(cell); ok {
		ts.mu.Unlock()
		return conn, nil
	}
	if ts.closed {
		ts.mu.Unlock()
		return nil, NewError(Interrupted, cell)
	}
	generation := ts.cellInfoGenerations[cell]
	global := ts.globalCell
	ts.mu.Unlock()

	// Other calls for the cell wait for this read, so bound it even if ctx has no deadline. It is a strong read:
	// the result is trusted for the TTL, so it must not come from a GlobalReadOnlyCell that lags behind.
	readCtx, cancel := context.WithTimeout(ctx, RemoteOperationTimeout)
	defer cancel()
	readStart := time.Now()
	ci, err := getCellInfo(readCtx, global, cell)
	if err != nil {
		return nil, err
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.closed {
		// Don't connect after Close, which would leave the connection open.
		return nil, NewError(Interrupted, cell)
	}
	conn, err := ts.connForCellInfoLocked(cell, ci)
	if err != nil {
		return nil, err
	}
	// If the cell's CellInfo was expired while we read it, what we read may predate the change, so leave it
	// expired for the next call to read again.
	ts.cellInfoReads[cell] = cellInfoRead{cellInfo: ci, readStart: readStart, expired: ts.cellInfoGenerations[cell] != generation}
	return conn, nil
}

// expireCellInfo makes the next cellReaderFor for cell read its CellInfo from the global topo again.
func (ts *Server) expireCellInfo(cell string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.cellInfoGenerations[cell]++
	if read, ok := ts.cellInfoReads[cell]; ok {
		read.expired = true
		ts.cellInfoReads[cell] = read
	}
}
