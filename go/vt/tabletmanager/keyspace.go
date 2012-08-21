// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"sort"

	"code.google.com/p/vitess/go/vt/naming"
	"code.google.com/p/vitess/go/vt/shard"
)

// Some attributes are per-keyspace and are tracked globally.
//  * schema (ideal version and apply log)
//    * each db will have a slightly different apply log, but generally schema
//      should not diverge.
//  * query blacklist
//  * account config? (vt_app, vt_dba, vt_repl)
// /zk/global/vt/keyspaces/<keyspace>
type Keyspace struct {
	TabletTypes []TabletType
}

// /zk/local/vt/ns/<keyspace>/<shard>
type SrvShard struct {
	KeyRange shard.KeyRange

	// This is really keyed by TabletType, but the json marshaller doesn't like
	// that since it requires all keys to be "string" - not "string-ish".
	AddrsByType map[string]naming.VtnsAddrs

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
