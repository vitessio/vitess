package main

import (
	"fmt"
	"path"
	"sort"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
)

const (
	globalKeyspacesPath = "/zk/global/vt/keyspaces"
)

type TopoReader struct {
	zkr zk.ZkReader
}

func (tr *TopoReader) GetKeyspaces(req struct{}, reply *[]string) error {
	zkrReply := &zk.ZkNode{}
	if err := tr.zkr.Children(&zk.ZkPath{Path: globalKeyspacesPath}, zkrReply); err != nil {
		return err
	}
	*reply = zkrReply.Children
	sort.Strings(*reply)
	return nil
}

func zkPathForVtKeyspace(cell, keyspace string) string {
	return fmt.Sprintf("/zk/%v/vt/ns/%v", cell, keyspace)
}

func zkPathForVtShard(cell, keyspace, shard string) string {
	return path.Join(zkPathForVtKeyspace(cell, keyspace), shard)
}

type GetSrvKeyspaceArgs struct {
	Cell     string
	Keyspace string
}

// FIXME(ryszard): These methods are kinda copy-and-pasted from
// zktopo.Server. In the long-term, the TopoReader should just take a
// topo.Server, which would be backed by a caching ZooKeeper
// connection.

func (tr *TopoReader) GetSrvKeyspace(req GetSrvKeyspaceArgs, reply *topo.SrvKeyspace) (err error) {
	keyspacePath := zkPathForVtKeyspace(req.Cell, req.Keyspace)
	zkrReply := &zk.ZkNode{}
	if err := tr.zkr.Get(&zk.ZkPath{Path: keyspacePath}, zkrReply); err != nil {
		return err
	}

	keyspace, err := topo.NewSrvKeyspace(zkrReply.Data, zkrReply.Stat.Version())
	*reply = *keyspace
	return
}

type GetEndPointsArgs struct {
	Cell       string
	Keyspace   string
	Shard      string
	TabletType string
}

func (tr *TopoReader) GetEndPoints(req GetEndPointsArgs, reply *topo.VtnsAddrs) (err error) {
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
