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

// /zk/local/vt/ns/<keyspace>/<shard>
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
// zk cell for fast access. Derived from the global keypsace and
// local details.
// /zk/local/vt/ns/<keyspace>
type SrvKeyspace struct {
	// List of non-overlapping shards sorted by range.
	Shards []SrvShard
	// List of available tablet types for this keyspace in this cell.
	TabletTypes []string // TabletType causes import cycle
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

func ReadSrvShard(zconn zk.Conn, zkPath string) (*SrvShard, error) {
	data, stat, err := zconn.Get(zkPath)
	if err != nil {
		return nil, err
	}
	srv := new(SrvShard)
	if len(data) > 0 {
		err = json.Unmarshal([]byte(data), srv)
		if err != nil {
			return nil, fmt.Errorf("SrvShard unmarshal failed: %v %v %v", zkPath, data, err)
		}
	}
	srv.version = stat.Version()
	return srv, nil
}

// These functions deal with keeping data in the shard graph up to date.
//
// The shard graph is the client-side view of the cluster and is derived
// from canonical data sources in zk.
//
// Some of this could be implemented by watching zk nodes, but there are
// enough cases where automatic updating is undersirable. Instead, these
// functions are called where appropriate.
//
// A given tablet should only appear in once in the serving graph.  A
// given tablet can change address, but if it changes db type, so all
// db typenodes need to be scanned and updated as appropriate. That is
// handled by UpdateServingGraphForShard.  If no entry is found for
// this tablet an error is returned.
// func UpdateServingGraphForTablet(zconn zk.Conn, tablet *Tablet) error {
// 	return nil
// }

// Recompute all nodes in the serving graph for a list of tablets in
// the shard.  This will overwrite existing data.
// func UpdateServingGraphForShard(zconn zk.Conn, tablet *[]Tablet) error {
// 	return nil
// }
