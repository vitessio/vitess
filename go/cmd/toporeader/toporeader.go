package main

import (
	"sort"

	"github.com/youtube/vitess/go/vt/topo"
)

type TopoReader struct {
	ts topo.Server
}

func (tr *TopoReader) GetSrvKeyspaceNames(req topo.GetSrvKeyspaceNamesArgs, reply *topo.SrvKeyspaceNames) error {
	// FIXME(alainjobart) this is wrong, we need GetSrvKeyspaceNames in API
	var err error
	reply.Entries, err = tr.ts.GetKeyspaces()
	if err != nil {
		return err
	}
	sort.Strings(reply.Entries)
	return nil
}

func (tr *TopoReader) GetSrvKeyspace(req topo.GetSrvKeyspaceArgs, reply *topo.SrvKeyspace) (err error) {
	keyspace, err := tr.ts.GetSrvKeyspace(req.Cell, req.Keyspace)
	if err != nil {
		return err
	}
	*reply = *keyspace
	return nil
}

func (tr *TopoReader) GetEndPoints(req topo.GetEndPointsArgs, reply *topo.VtnsAddrs) (err error) {
	addrs, err := tr.ts.GetSrvTabletType(req.Cell, req.Keyspace, req.Shard, req.TabletType)
	if err != nil {
		return err
	}
	*reply = *addrs
	return nil
}
