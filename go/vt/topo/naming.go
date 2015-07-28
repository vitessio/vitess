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
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

const (
	// DefaultPortName is the port named used by SrvEntries
	// if "" is given as the named port.
	DefaultPortName = "vt"
)

// NewEndPoint returns a new empty EndPoint
func NewEndPoint(uid uint32, host string) *pb.EndPoint {
	return &pb.EndPoint{
		Uid:     uid,
		Host:    host,
		PortMap: make(map[string]int32),
	}
}

// EndPointEquality returns true iff two EndPoint are representing the same data
func EndPointEquality(left, right *pb.EndPoint) bool {
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
	if len(left.HealthMap) != len(right.HealthMap) {
		return false
	}
	for key, lvalue := range left.HealthMap {
		rvalue, ok := right.HealthMap[key]
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
func NewEndPoints() *pb.EndPoints {
	return &pb.EndPoints{Entries: make([]*pb.EndPoint, 0, 8)}
}

// LookupVtName gets the list of EndPoints for a
// cell/keyspace/shard/tablet type and converts the list to net.SRV records
func LookupVtName(ctx context.Context, ts Server, cell, keyspace, shard string, tabletType TabletType, namedPort string) ([]*net.SRV, error) {
	addrs, _, err := ts.GetEndPoints(ctx, cell, keyspace, shard, tabletType)
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
func SrvEntries(addrs *pb.EndPoints, namedPort string) (srvs []*net.SRV, err error) {
	srvs = make([]*net.SRV, 0, len(addrs.Entries))
	var srvErr error
	for _, entry := range addrs.Entries {
		host := entry.Host
		port := 0
		if namedPort == "" {
			namedPort = DefaultPortName
		}
		port = int(entry.PortMap[namedPort])
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
