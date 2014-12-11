// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkns

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"

	"github.com/henryanand/vitess/go/netutil"
	"github.com/henryanand/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

// ZknsAddr represents a variety if different address types, primarily in JSON.
type ZknsAddr struct {
	// These fields came from a Python app originally that used a different
	// naming convention.
	Host         string         `json:"host"`
	Port         int            `json:"port,omitempty"` // DEPRECATED
	NamedPortMap map[string]int `json:"named_port_map"`
	IPv4         string         `json:"ipv4"`
	version      int            // zk version to allow non-stomping writes
}

// NewAddr returns a new ZknsAddr.
func NewAddr(host string) *ZknsAddr {
	return &ZknsAddr{Host: host, NamedPortMap: make(map[string]int)}
}

// ZknsAddrs represents a list of individual entries. SRV records can
// have multiple endpoints, so this is always a list.  A record with
// one entry and a port number zero is interpreted as a CNAME.  A
// record with one entry, a port number zero and an IP address is
// interpreted as an A.
type ZknsAddrs struct {
	Entries []ZknsAddr
	version int // zk version to allow non-stomping writes
}

// NewAddrs returns a new ZknsAddrs.
func NewAddrs() *ZknsAddrs {
	return &ZknsAddrs{Entries: make([]ZknsAddr, 0, 8), version: -1}
}

// IsValidA returns the answer to the eternal question - can this be
// interpreted as an A record?
func (zaddrs *ZknsAddrs) IsValidA() bool {
	if zaddrs == nil || len(zaddrs.Entries) == 0 {
		return false
	}
	for _, entry := range zaddrs.Entries {
		if entry.IPv4 == "" || entry.Host != "" || len(entry.NamedPortMap) > 0 {
			return false
		}
	}
	return true
}

// IsValidCNAME returns true if this can be interpreted as a
// CNAME. This method is intentially loose - it allows a SRV record
// with a single entry to be interpreted as a CNAME.
func (zaddrs *ZknsAddrs) IsValidCNAME() bool {
	if zaddrs == nil || len(zaddrs.Entries) != 1 || zaddrs.Entries[0].IPv4 != "" || zaddrs.Entries[0].Host == "" {
		return false
	}
	return true
}

// IsValidSRV returns true if this can be interpreted as a SRV record.
func (zaddrs *ZknsAddrs) IsValidSRV() bool {
	if zaddrs == nil {
		return false
	}
	for _, zaddr := range zaddrs.Entries {
		if zaddr.Host == "" || zaddr.IPv4 != "" || len(zaddr.NamedPortMap) == 0 {
			return false
		}
	}
	return true
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

// ReadAddrs returns a ZknsAddrs record from the given path in Zookeeper.
func ReadAddrs(zconn zk.Conn, zkPath string) (*ZknsAddrs, error) {
	data, stat, err := zconn.Get(zkPath)
	if err != nil {
		return nil, err
	}
	// There are nodes that will have no data - for instance a subdomain node.
	if len(data) == 0 {
		return nil, nil
	}
	addrs := new(ZknsAddrs)
	err = json.Unmarshal([]byte(data), addrs)
	if err != nil {
		return nil, err
	}
	addrs.version = stat.Version()
	return addrs, nil
}

// WriteAddrs writes a ZknsAddres record to a path in Zookeeper.
func WriteAddrs(zconn zk.Conn, zkPath string, addrs *ZknsAddrs) error {
	if !(addrs.IsValidA() || addrs.IsValidCNAME() || addrs.IsValidSRV()) {
		return fmt.Errorf("invalid addrs: %v", zkPath)
	}
	data := toJson(addrs)
	_, err := zk.CreateOrUpdate(zconn, zkPath, data, 0, zookeeper.WorldACL(zk.PERM_FILE), true)
	return err
}

// LookupName returns a list of SRV records. addrPath is the path to a
// json file in zk. It can also reference a named port
// (/zk/cell/zkns/path:_named_port)
func LookupName(zconn zk.Conn, addrPath string) ([]*net.SRV, error) {
	zkPathParts := strings.Split(addrPath, ":")
	zkPath := zkPathParts[0]
	namedPort := ""
	if len(zkPathParts) == 2 {
		namedPort = zkPathParts[1]
	}

	addrs, err := ReadAddrs(zconn, zkPath)
	if err != nil {
		return nil, fmt.Errorf("LookupName failed: %v", err)
	}
	if namedPort == "" {
		if !addrs.IsValidA() && !addrs.IsValidCNAME() {
			return nil, fmt.Errorf("LookupName named port required: %v", addrPath)
		}
	} else if !addrs.IsValidSRV() {
		return nil, fmt.Errorf("LookupName invalid record: %v", addrPath)
	}
	isValidA := addrs.IsValidA()
	srvs := make([]*net.SRV, 0, len(addrs.Entries))
	for _, addr := range addrs.Entries {
		// Set the weight to non-zero, otherwise the sort method is deactivated.
		srv := &net.SRV{Target: addr.Host, Weight: 1}
		if namedPort != "" {
			srv.Port = uint16(addr.NamedPortMap[namedPort])
			if srv.Port == 0 {
				// If the port was requested and not defined it's probably
				// a bug, so fail hard.
				return nil, fmt.Errorf("LookupName found no such named port: %v", addrPath)
			}
		} else if isValidA {
			srv.Target = addr.IPv4
		}
		srvs = append(srvs, srv)
	}
	netutil.SortRfc2782(srvs)
	return srvs, nil
}
