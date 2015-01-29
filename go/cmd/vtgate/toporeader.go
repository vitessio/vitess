package main

import (
	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate"
	"golang.org/x/net/context"
)

// TopoReader implements topo.TopoReader.
type TopoReader struct {
	// the server to get data from
	ts vtgate.SrvTopoServer

	// stats
	queryCount *stats.Counters
	errorCount *stats.Counters
}

// NewTopoReader creates a new TopoReader.
func NewTopoReader(ts vtgate.SrvTopoServer) *TopoReader {
	return &TopoReader{
		ts:         ts,
		queryCount: stats.NewCounters("TopoReaderRpcQueryCount"),
		errorCount: stats.NewCounters("TopoReaderRpcErrorCount"),
	}
}

// GetSrvKeyspaceNames returns the names of all keyspaces for the cell.
func (tr *TopoReader) GetSrvKeyspaceNames(ctx context.Context, req *topo.GetSrvKeyspaceNamesArgs, reply *topo.SrvKeyspaceNames) error {
	tr.queryCount.Add(req.Cell, 1)
	var err error
	reply.Entries, err = tr.ts.GetSrvKeyspaceNames(ctx, req.Cell)
	if err != nil {
		log.Warningf("GetSrvKeyspaceNames(%v) failed: %v", req.Cell, err)
		tr.errorCount.Add(req.Cell, 1)
		return err
	}
	return nil
}

// GetSrvKeyspace returns information about a keyspace
// in a particular cell.
func (tr *TopoReader) GetSrvKeyspace(ctx context.Context, req *topo.GetSrvKeyspaceArgs, reply *topo.SrvKeyspace) (err error) {
	tr.queryCount.Add(req.Cell, 1)
	keyspace, err := tr.ts.GetSrvKeyspace(ctx, req.Cell, req.Keyspace)
	if err != nil {
		log.Warningf("GetSrvKeyspace(%v,%v) failed: %v", req.Cell, req.Keyspace, err)
		tr.errorCount.Add(req.Cell, 1)
		return err
	}
	*reply = *keyspace
	return nil
}

// GetSrvShard returns information about a shard for a keyspace
// in a particular cell.
func (tr *TopoReader) GetSrvShard(ctx context.Context, req *topo.GetSrvShardArgs, reply *topo.SrvShard) (err error) {
	tr.queryCount.Add(req.Cell, 1)
	shard, err := tr.ts.GetSrvShard(ctx, req.Cell, req.Keyspace, req.Shard)
	if err != nil {
		log.Warningf("GetSrvShard(%v,%v,%v) failed: %v", req.Cell, req.Keyspace, req.Shard, err)
		tr.errorCount.Add(req.Cell, 1)
		return err
	}
	*reply = *shard
	return nil
}

// GetEndPoints returns addresses for a tablet type in a shard
// in a keyspace.
func (tr *TopoReader) GetEndPoints(ctx context.Context, req *topo.GetEndPointsArgs, reply *topo.EndPoints) (err error) {
	tr.queryCount.Add(req.Cell, 1)
	addrs, err := tr.ts.GetEndPoints(ctx, req.Cell, req.Keyspace, req.Shard, req.TabletType)
	if err != nil {
		log.Warningf("GetEndPoints(%v,%v,%v,%v) failed: %v", req.Cell, req.Keyspace, req.Shard, req.TabletType, err)
		tr.errorCount.Add(req.Cell, 1)
		return err
	}
	*reply = *addrs
	return nil
}
