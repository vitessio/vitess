package proto

// contains the structures used for RPC calls to zkocc.
// in a different module so the client only links with this.

import (
	"time"
)

type ZkStat struct {
	Czxid          int64
	Mzxid          int64
	CTime          time.Time
	MTime          time.Time
	Version        int
	CVersion       int
	AVersion       int
	EphemeralOwner int64
	DataLength     int
	NumChildren    int
	Pzxid          int64
}

type ZkPath struct {
	Path string
}

type ZkPathV struct {
	Paths []string
}

type ZkNode struct {
	Path     string
	Data     string
	Stat     ZkStat
	Children []string
	Cached   bool // the response comes from the zkocc cache
	Stale    bool // the response is stale because we're not connected
}

type ZkNodeV struct {
	Nodes []*ZkNode
}
