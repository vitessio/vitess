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
	"encoding/json"
	"fmt"
	"net"

	"code.google.com/p/vitess/go/netutil"
	"code.google.com/p/vitess/go/relog"
)

type VtnsAddr struct {
	Uid          uint32         `json:"uid"` // Keep track of which tablet this corresponds to.
	Host         string         `json:"host"`
	Port         int            `json:"port"`
	NamedPortMap map[string]int `json:"named_port_map"`
}

type VtnsAddrs struct {
	Entries []VtnsAddr `json:"entries"`
	version int        // version to allow non-stomping writes
}

func NewAddr(uid uint32, host string, port int) *VtnsAddr {
	return &VtnsAddr{Uid: uid, Host: host, Port: port, NamedPortMap: make(map[string]int)}
}

func VtnsAddrEquality(left, right *VtnsAddr) bool {
	if left.Uid != right.Uid {
		return false
	}
	if left.Host != right.Host {
		return false
	}
	if left.Port != right.Port {
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
	return true
}

func NewAddrs() *VtnsAddrs {
	return &VtnsAddrs{Entries: make([]VtnsAddr, 0, 8), version: -1}
}

func LookupVtName(ts Server, cell, keyspace, shard string, tabletType TabletType, namedPort string) ([]*net.SRV, error) {
	addrs, err := ts.GetSrvTabletType(cell, keyspace, shard, tabletType)
	if err != nil {
		return nil, fmt.Errorf("LookupVtName(%v,%v,%v,%v) failed: %v", cell, keyspace, shard, tabletType, err)
	}
	srvs, err := SrvEntries(addrs, namedPort)
	if err != nil {
		return nil, fmt.Errorf("LookupVtName(%v,%v,%v,%v) failed: %v", cell, keyspace, shard, tabletType, err)
	}
	return srvs, err
}

// FIXME(msolomon) merge with zkns
func SrvEntries(addrs *VtnsAddrs, namedPort string) (srvs []*net.SRV, err error) {
	srvs = make([]*net.SRV, 0, len(addrs.Entries))
	var srvErr error
	for _, entry := range addrs.Entries {
		host := entry.Host
		port := 0
		if namedPort == "" {
			port = entry.Port
		} else {
			port = entry.NamedPortMap[namedPort]
		}
		if port == 0 {
			relog.Warning("vtns: bad port %v %v", namedPort, entry)
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

func SrvAddr(srv *net.SRV) string {
	return fmt.Sprintf("%s:%d", srv.Target, srv.Port)
}

func NewVtnsAddrs(data string, version int) (*VtnsAddrs, error) {
	addrs := new(VtnsAddrs)
	if len(data) > 0 {
		if err := json.Unmarshal([]byte(data), addrs); err != nil {
			return nil, fmt.Errorf("VtnsAddrs unmarshal failed: %v %v", data, err)
		}
	}
	addrs.version = version
	return addrs, nil
}
