package topo

import (
	rpc "github.com/youtube/vitess/go/rpcplus"
)

// TopoReader returns read only information about the topology.
type TopoReader interface {
	// GetKeyspaces returns all the keyspaces in the topology.
	GetKeyspaces(struct{}, *Keyspaces) error

	// GetSrvKeyspace returns information about a keyspace in a
	// particular cell (as specified by the GetSrvKeyspaceArgs).
	GetSrvKeyspace(GetSrvKeyspaceArgs, *SrvKeyspace) error

	// GetEndPoints returns addresses for a tablet type in a shard
	// in a keyspace (as specified in GetEndPointsArgs).
	GetEndPoints(GetEndPointsArgs, *VtnsAddrs) error
}

type GetSrvKeyspaceArgs struct {
	Cell     string
	Keyspace string
}

type Keyspaces struct {
	Entries []string
}

type GetEndPointsArgs struct {
	Cell       string
	Keyspace   string
	Shard      string
	TabletType string
}

func RegisterTopoReader(tr TopoReader) {
	rpc.Register(tr)
}
