package topo

import (
	rpc "github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/proto"
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
	GetEndPoints(*proto.Context, GetEndPointsArgs, *EndPoints) error
}

// GetSrvKeyspaceNamesArgs is the parameters for TopoReader.GetSrvKeyspaceNames
type GetSrvKeyspaceNamesArgs struct {
	Cell string
}

// GetSrvKeyspaceArgs is the parameters for TopoReader.GetSrvKeyspace
type GetSrvKeyspaceArgs struct {
	Cell     string
	Keyspace string
}

// SrvKeyspaceNames is the response for TopoReader.GetSrvKeyspaceNames
type SrvKeyspaceNames struct {
	Entries []string
}

// GetSrvKeyspaceNamesArgs is the parameters for TopoReader.GetEndPoints
type GetEndPointsArgs struct {
	Cell       string
	Keyspace   string
	Shard      string
	TabletType TabletType
}

// RegisterTopoReader register the provided TopoReader for RPC
func RegisterTopoReader(tr TopoReader) {
	rpc.Register(tr)
}
