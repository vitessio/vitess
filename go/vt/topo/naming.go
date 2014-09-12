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
of host:port tuples that export our default server (vtocc).  You can
get all shards with "keyspace.*.db_type".

In zk, this is in /zk/local/vt/ns/<keyspace>/<shard>/<db type>
*/

import (
	"fmt"
	"net"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/netutil"
)

const (
	// DefaultPortName is the port named used by SrvEntries
	// if "" is given as the named port.
	DefaultPortName = "_vtocc"
)

// EndPoint describes a tablet (maybe composed of multiple processes)
// listening on one or more named ports, and its health. Clients use this
// record to connect to a tablet.
type EndPoint struct {
	Uid          uint32            `json:"uid"` // Keep track of which tablet this corresponds to.
	Host         string            `json:"host"`
	NamedPortMap map[string]int    `json:"named_port_map"`
	Health       map[string]string `json:"health"`
}

// EndPoints is a list of EndPoint objects, all of the same type.
type EndPoints struct {
	Entries []EndPoint `json:"entries"`
}

// NewEndPoint returns a new empty EndPoint
func NewEndPoint(uid uint32, host string) *EndPoint {
	return &EndPoint{Uid: uid, Host: host, NamedPortMap: make(map[string]int)}
}

// EndPointEquality returns true iff two EndPoint are representing the same data
func EndPointEquality(left, right *EndPoint) bool {
	if left.Uid != right.Uid {
		return false
	}
	if left.Host != right.Host {
		return false
	}
	if len(left.NamedPortMap) != len(right.NamedPortMap) {
		return false
	}
	for key, lvalue := range left.NamedPortMap {
		rvalue, ok := right.NamedPortMap[key]
		if !ok {
			return false
		}
		if lvalue != rvalue {
			return false
		}
	}
	if len(left.Health) != len(right.Health) {
		return false
	}
	for key, lvalue := range left.Health {
		rvalue, ok := right.Health[key]
		if !ok {
			return false
		}
		if lvalue != rvalue {
			return false
		}
	}
	return true
}

// NewEndPoints creates a EndPoints with a pre-allocated slice for Entries.
func NewEndPoints() *EndPoints {
	return &EndPoints{Entries: make([]EndPoint, 0, 8)}
}

// LookupVtName gets the list of EndPoints for a
// cell/keyspace/shard/tablet type and converts the list to net.SRV records
func LookupVtName(ts Server, cell, keyspace, shard string, tabletType TabletType, namedPort string) ([]*net.SRV, error) {
	addrs, err := ts.GetEndPoints(cell, keyspace, shard, tabletType)
	if err != nil {
		return nil, fmt.Errorf("LookupVtName(%v,%v,%v,%v) failed: %v", cell, keyspace, shard, tabletType, err)
	}
	srvs, err := SrvEntries(addrs, namedPort)
	if err != nil {
		return nil, fmt.Errorf("LookupVtName(%v,%v,%v,%v) failed: %v", cell, keyspace, shard, tabletType, err)
	}
	return srvs, err
}

// SrvEntries converts EndPoints to net.SRV for a given port.
// FIXME(msolomon) merge with zkns
func SrvEntries(addrs *EndPoints, namedPort string) (srvs []*net.SRV, err error) {
	srvs = make([]*net.SRV, 0, len(addrs.Entries))
	var srvErr error
	for _, entry := range addrs.Entries {
		host := entry.Host
		port := 0
		if namedPort == "" {
			namedPort = DefaultPortName
		}
		port = entry.NamedPortMap[namedPort]
		if port == 0 {
			log.Warningf("vtns: bad port %v %v", namedPort, entry)
			continue
		}
		srvs = append(srvs, &net.SRV{Target: host, Port: uint16(port)})
	}
	netutil.SortRfc2782(srvs)
	if srvErr != nil && len(srvs) == 0 {
		return nil, fmt.Errorf("SrvEntries failed: no valid endpoints found")
	}
	return
}

// SrvAddr prints a net.SRV
func SrvAddr(srv *net.SRV) string {
	return fmt.Sprintf("%s:%d", srv.Target, srv.Port)
}
