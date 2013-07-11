// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package naming

import (
	"encoding/json"
	"fmt"
	"sort"

	"code.google.com/p/vitess/go/vt/key"
	"code.google.com/p/vitess/go/zk"
)

// SrvShard contains a roll-up of the shard in the local namespace.
// In zk, it is under /zk/local/vt/ns/<keyspace>/<shard>
type SrvShard struct {
	KeyRange key.KeyRange

	// This is really keyed by TabletType, but the json marshaller doesn't like
	// that since it requires all keys to be "string" - not "string-ish".
	AddrsByType map[string]VtnsAddrs

	// True if the master cannot process writes
	ReadOnly bool
	version  int
}

// A distilled serving copy of keyspace detail stored in the local
// zk cell for fast access. Derived from the global keyspace and
// local details.
// In zk, it is in /zk/local/vt/ns/<keyspace>
type SrvKeyspace struct {
	// List of non-overlapping shards sorted by range.
	Shards []SrvShard
	// List of available tablet types for this keyspace in this cell.
	TabletTypes []TabletType
	version     int
}

type SrvShardArray []SrvShard

func (sa SrvShardArray) Len() int { return len(sa) }

func (sa SrvShardArray) Less(i, j int) bool {
	return sa[i].KeyRange.Start < sa[j].KeyRange.Start
}

func (sa SrvShardArray) Swap(i, j int) {
	sa[i], sa[j] = sa[j], sa[i]
}

func (sa SrvShardArray) Sort() { sort.Sort(sa) }

func NewSrvShard(data string, version int) (*SrvShard, error) {
	srv := new(SrvShard)
	if len(data) > 0 {
		if err := json.Unmarshal([]byte(data), srv); err != nil {
			return nil, fmt.Errorf("SrvShard unmarshal failed: %v %v", data, err)
		}
	}
	srv.version = version
	return srv, nil
}

func ReadSrvShard(zconn zk.Conn, zkPath string) (*SrvShard, error) {
	data, stat, err := zconn.Get(zkPath)
	if err != nil {
		return nil, err
	}
	return NewSrvShard(data, stat.Version())
}

func ReadSrvKeyspace(zconn zk.Conn, zkPath string) (*SrvKeyspace, error) {
	data, stat, err := zconn.Get(zkPath)
	if err != nil {
		return nil, err
	}
	srv := new(SrvKeyspace)
	if len(data) > 0 {
		err = json.Unmarshal([]byte(data), srv)
		if err != nil {
			return nil, fmt.Errorf("SrvKeyspace unmarshal failed: %v %v %v", zkPath, data, err)
		}
	}
	srv.version = stat.Version()
	return srv, nil
}
