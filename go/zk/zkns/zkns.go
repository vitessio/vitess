// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkns

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"

	"code.google.com/p/vitess/go/netutil"
	"code.google.com/p/vitess/go/zk"
)

type ZknsAddr struct {
	// These fields came from a Python app originally that used a different
	// naming convention.
	Host         string         `json:"host"`
	Port         int            `json:"port"` // DEPRECATED
	NamedPortMap map[string]int `json:"named_port_map"`
	IPv4         string         `json:"ipv4"`
	version      int            // zk version to allow non-stomping writes
}

func NewAddr(host string, port int) *ZknsAddr {
	return &ZknsAddr{Host: host, Port: port, NamedPortMap: make(map[string]int)}
}

// SRV records can have multiple endpoints, so this is always a list.
// A record with one entry and a port number zero is interpreted as a CNAME.
// A record with one entry, a port number zero and an IP address is interpreted as an A.
type ZknsAddrs struct {
	Entries []ZknsAddr
	version int // zk version to allow non-stomping writes
}

func NewAddrs() *ZknsAddrs {
	return &ZknsAddrs{Entries: make([]ZknsAddr, 0, 8), version: -1}
}

func toJson(x interface{}) string {
	data, err := json.MarshalIndent(x, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(data)
}

func addrFromJson(data string) *ZknsAddr {
	addr := &ZknsAddr{}
	if err := json.Unmarshal([]byte(data), addr); err != nil {
		panic(err)
	}
	return addr
}

func ReadAddrs(zconn zk.Conn, zkPath string) (*ZknsAddrs, error) {
	data, stat, err := zconn.Get(zkPath)
	if err != nil {
		return nil, err
	}
	addrs := new(ZknsAddrs)
	err = json.Unmarshal([]byte(data), addrs)
	if err != nil {
		return nil, err
	}
	addrs.version = stat.Version()
	return addrs, nil
}

// zkPath is the path to a json file in zk. It can also reference a
// named port: /zk/cell/zkns/path:_named_port
func LookupName(zconn zk.Conn, zkPath string) ([]*net.SRV, error) {
	zkPathParts := strings.Split(zkPath, ":")
	namedPort := ""
	if len(zkPathParts) == 2 {
		zkPath = zkPathParts[0]
		namedPort = zkPathParts[1]
	}

	addrs, err := ReadAddrs(zconn, zkPath)
	if err != nil {
		return nil, fmt.Errorf("LookupName failed: %v %v", zkPath, err)
	}

	srvs := make([]*net.SRV, 0, len(addrs.Entries))
	for _, addr := range addrs.Entries {
		srv := &net.SRV{Target: addr.Host}
		if namedPort == "" {
			// FIXME(msolomon) remove this clause - allow only named ports.
			srv.Port = uint16(addr.Port)
		} else {
			srv.Port = uint16(addr.NamedPortMap[namedPort])
		}
		srvs = append(srvs, srv)
	}
	netutil.SortRfc2782(srvs)
	return srvs, nil
}
