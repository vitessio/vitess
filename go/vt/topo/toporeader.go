package topo

import (
	rpc "github.com/youtube/vitess/go/rpcplus"
)

// TopoReader returns read only information about the topology.
type TopoReader interface {
	// GetSrvKeyspaces returns the names of all the keyspaces in
	// the topology for the cell.
	GetSrvKeyspaceNames(GetSrvKeyspaceNamesArgs, *SrvKeyspaceNames) error

	// GetSrvKeyspace returns information about a keyspace in a
	// particular cell (as specified by the GetSrvKeyspaceArgs).
	GetSrvKeyspace(GetSrvKeyspaceArgs, *SrvKeyspace) error

	// GetEndPoints returns addresses for a tablet type in a shard
	// in a keyspace (as specified in GetEndPointsArgs).
	GetEndPoints(GetEndPointsArgs, *EndPoints) error
}

type GetSrvKeyspaceNamesArgs struct {
	Cell string
}

type GetSrvKeyspaceArgs struct {
	Cell     string
	Keyspace string
}

type SrvKeyspaceNames struct {
	Entries []string
}

type GetEndPointsArgs struct {
	Cell       string
	Keyspace   string
	Shard      string
	TabletType TabletType
}

func RegisterTopoReader(tr TopoReader) {
	rpc.Register(tr)
}
