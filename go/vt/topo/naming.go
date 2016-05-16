// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

/*
Handle logical name resolution - sort of like DNS but tailored to
vt and using the topology server.

Naming is disconnected from the backend discovery and is used for
front end clients.

The common query is "resolve keyspace.shard.db_type" and return a list
of host:port tuples that export our default server (vttablet).  You can
get all shards with "keyspace.*.db_type".

In zk, this is in /zk/local/vt/ns/<keyspace>/<shard>/<db type>
*/

import topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"

const (
	// DefaultPortName is the port named used by SrvEntries
	// if "" is given as the named port.
	DefaultPortName = "vt"
)

// NewEndPoint returns a new empty EndPoint
func NewEndPoint(uid uint32, host string) *topodatapb.EndPoint {
	return &topodatapb.EndPoint{
		Uid:     uid,
		Host:    host,
		PortMap: make(map[string]int32),
	}
}

// EndPointEquality returns true iff two EndPoint are representing the same data
func EndPointEquality(left, right *topodatapb.EndPoint) bool {
	if left.Uid != right.Uid {
		return false
	}
	if left.Host != right.Host {
		return false
	}
	if len(left.PortMap) != len(right.PortMap) {
		return false
	}
	for key, lvalue := range left.PortMap {
		rvalue, ok := right.PortMap[key]
		if !ok {
			return false
		}
		if lvalue != rvalue {
			return false
		}
	}
	return true
}
