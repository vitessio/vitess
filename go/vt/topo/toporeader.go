package topo

import (
	"golang.org/x/net/context"
)

// TopoReader returns read only information about the topology.
type TopoReader interface {
	// GetSrvKeyspaces returns the names of all the keyspaces in
	// the topology for the cell.
	GetSrvKeyspaceNames(context.Context, *GetSrvKeyspaceNamesArgs, *SrvKeyspaceNames) error

	// GetSrvKeyspace returns information about a keyspace in a
	// particular cell (as specified by the GetSrvKeyspaceArgs).
	GetSrvKeyspace(context.Context, *GetSrvKeyspaceArgs, *SrvKeyspace) error

	// GetSrvShard returns information about a shard in a
	// particular cell and keyspace (as specified by the GetSrvShardArgs).
	GetSrvShard(context.Context, *GetSrvShardArgs, *SrvShard) error

	// GetEndPoints returns addresses for a tablet type in a shard
	// in a keyspace (as specified in GetEndPointsArgs).
	GetEndPoints(context.Context, *GetEndPointsArgs, *EndPoints) error
}

// GetSrvKeyspaceNamesArgs is the parameters for TopoReader.GetSrvKeyspaceNames
type GetSrvKeyspaceNamesArgs struct {
	Cell string
}

//go:generate bsongen -file $GOFILE -type GetSrvKeyspaceNamesArgs -o get_srv_keyspace_names_args_bson.go

// GetSrvKeyspaceArgs is the parameters for TopoReader.GetSrvKeyspace
type GetSrvKeyspaceArgs struct {
	Cell     string
	Keyspace string
}

//go:generate bsongen -file $GOFILE -type GetSrvKeyspaceArgs -o get_srv_keyspace_args_bson.go

// GetSrvShardArgs is the parameters for TopoReader.GetSrvShard
type GetSrvShardArgs struct {
	Cell     string
	Keyspace string
	Shard    string
}

//go:generate bsongen -file $GOFILE -type GetSrvShardArgs -o get_srv_shard_args_bson.go

// SrvKeyspaceNames is the response for TopoReader.GetSrvKeyspaceNames
type SrvKeyspaceNames struct {
	Entries []string
}

//go:generate bsongen -file $GOFILE -type SrvKeyspaceNames -o srv_keyspace_names_bson.go

// GetEndPointsArgs is the parameters for TopoReader.GetEndPoints
type GetEndPointsArgs struct {
	Cell       string
	Keyspace   string
	Shard      string
	TabletType TabletType
}

//go:generate bsongen -file $GOFILE -type GetEndPointsArgs -o get_end_points_args_bson.go
