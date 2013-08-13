package main

import (
	"fmt"
	"path"
	"sort"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
)

type TopoReader struct {
	zkr zk.ZkReader
}

// FIXME(ryszard): These methods are kinda copy-and-pasted from
// zktopo.Server. In the long-term, the TopoReader should just take a
// topo.Server, which would be backed by a caching ZooKeeper
// connection.

func zkPathForVt(cell string) string {
	return fmt.Sprintf("/zk/%v/vt/ns", cell)
}

func zkPathForVtKeyspace(cell, keyspace string) string {
	return path.Join(zkPathForVt(cell), keyspace)
}

func zkPathForVtShard(cell, keyspace, shard string) string {
	return path.Join(zkPathForVt(cell), keyspace, shard)
}

func (tr *TopoReader) GetSrvKeyspaceNames(req topo.GetSrvKeyspaceNamesArgs, reply *topo.SrvKeyspaceNames) error {
	vtPath := zkPathForVt(req.Cell)
	zkrReply := &zk.ZkNode{}
	if err := tr.zkr.Children(&zk.ZkPath{Path: vtPath}, zkrReply); err != nil {
		return err
	}
	reply.Entries = zkrReply.Children
	sort.Strings(reply.Entries)
	return nil
}

func (tr *TopoReader) GetSrvKeyspace(req topo.GetSrvKeyspaceArgs, reply *topo.SrvKeyspace) (err error) {
	keyspacePath := zkPathForVtKeyspace(req.Cell, req.Keyspace)
	zkrReply := &zk.ZkNode{}
	if err := tr.zkr.Get(&zk.ZkPath{Path: keyspacePath}, zkrReply); err != nil {
		return err
	}

	keyspace, err := topo.NewSrvKeyspace(zkrReply.Data, zkrReply.Stat.Version())
	*reply = *keyspace
	return
}

func (tr *TopoReader) GetEndPoints(req topo.GetEndPointsArgs, reply *topo.VtnsAddrs) (err error) {
	shardPath := zkPathForVtShard(req.Cell, req.Keyspace, req.Shard)
	zkrReply := &zk.ZkNode{}
	if err := tr.zkr.Get(&zk.ZkPath{Path: shardPath}, zkrReply); err != nil {
		return err
	}
	shard, err := topo.NewSrvShard(zkrReply.Data, zkrReply.Stat.Version())
	if err != nil {
		return err
	}
	*reply = shard.AddrsByType[req.TabletType]
	return nil
}
