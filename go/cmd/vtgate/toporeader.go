package main

import (
	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/topo"
)

type TopoReader struct {
	// the server to get data from
	ts topo.Server

	// stats
	queryCount *stats.Counters
	errorCount *stats.Counters
}

func NewTopoReader(ts topo.Server) *TopoReader {
	return &TopoReader{
		ts:         ts,
		queryCount: stats.NewCounters("VtTopoQueryCount"),
		errorCount: stats.NewCounters("VtTopoErrorCount"),
	}
}

func (tr *TopoReader) GetSrvKeyspaceNames(req topo.GetSrvKeyspaceNamesArgs, reply *topo.SrvKeyspaceNames) error {
	tr.queryCount.Add(req.Cell, 1)
	var err error
	reply.Entries, err = tr.ts.GetSrvKeyspaceNames(req.Cell)
	if err != nil {
		log.Warningf("GetSrvKeyspaceNames(%v) failed: %v", req.Cell, err)
		tr.errorCount.Add(req.Cell, 1)
		return err
	}
	return nil
}

func (tr *TopoReader) GetSrvKeyspace(req topo.GetSrvKeyspaceArgs, reply *topo.SrvKeyspace) (err error) {
	tr.queryCount.Add(req.Cell, 1)
	keyspace, err := tr.ts.GetSrvKeyspace(req.Cell, req.Keyspace)
	if err != nil {
		log.Warningf("GetSrvKeyspace(%v,%v) failed: %v", req.Cell, req.Keyspace, err)
		tr.errorCount.Add(req.Cell, 1)
		return err
	}
	*reply = *keyspace
	return nil
}

func (tr *TopoReader) GetEndPoints(req topo.GetEndPointsArgs, reply *topo.EndPoints) (err error) {
	tr.queryCount.Add(req.Cell, 1)
	addrs, err := tr.ts.GetEndPoints(req.Cell, req.Keyspace, req.Shard, req.TabletType)
	if err != nil {
		log.Warningf("GetEndPoints(%v,%v,%v,%v) failed: %v", req.Cell, req.Keyspace, req.Shard, req.TabletType, err)
		tr.errorCount.Add(req.Cell, 1)
		return err
	}
	*reply = *addrs
	return nil
}
