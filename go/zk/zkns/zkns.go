// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkns

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"sort"
	"strings"

	"code.google.com/p/vitess/go/zk"
)

type ZknsAddr struct {
	// These fields came from a Python app originally that used a different
	// naming convention.
	Host         string         `json:"host"`
	Port         int            `json:"port"`
	NamedPortMap map[string]int `json:"named_port_map"`
	version      int            // zk version to allow non-stomping writes
}

func NewAddr(host string, port int) *ZknsAddr {
	return &ZknsAddr{Host: host, Port: port, NamedPortMap: make(map[string]int)}
}

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

// byPriorityWeight sorts records by ascending priority and weight.
type byPriorityWeight []*net.SRV

func (s byPriorityWeight) Len() int { return len(s) }

func (s byPriorityWeight) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s byPriorityWeight) Less(i, j int) bool {
	return s[i].Priority < s[j].Priority ||
		(s[i].Priority == s[j].Priority && s[i].Weight < s[j].Weight)
}

// shuffleByWeight shuffles SRV records by weight using the algorithm
// described in RFC 2782.
func (addrs byPriorityWeight) shuffleByWeight() {
	sum := 0
	for _, addr := range addrs {
		sum += int(addr.Weight)
	}
	for sum > 0 && len(addrs) > 1 {
		s := 0
		n := rand.Intn(sum + 1)
		for i := range addrs {
			s += int(addrs[i].Weight)
			if s >= n {
				if i > 0 {
					t := addrs[i]
					copy(addrs[1:i+1], addrs[0:i])
					addrs[0] = t
				}
				break
			}
		}
		sum -= int(addrs[0].Weight)
		addrs = addrs[1:]
	}
}

// sort reorders SRV records as specified in RFC 2782.
func (addrs byPriorityWeight) sort() {
	sort.Sort(addrs)
	i := 0
	for j := 1; j < len(addrs); j++ {
		if addrs[i].Priority != addrs[j].Priority {
			addrs[i:j].shuffleByWeight()
			i = j
		}
	}
	addrs[i:].shuffleByWeight()
}

// sort reorders SRV records as specified in RFC 2782.
func Sort(srvs []*net.SRV) {
	byPriorityWeight(srvs).sort()
}

/*
zkPath is normally just the path to a file in zk. It can also reference a named
port like this:
  /zk/cell/zkns/path:_named_port
*/
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
	hasError := false
	for _, addr := range addrs.Entries {
		srv := &net.SRV{Target: addr.Host}
		if namedPort == "" {
			srv.Port = uint16(addr.Port)
		} else {
			srv.Port = uint16(addr.NamedPortMap[namedPort])
		}
		if srv.Port == 0 {
			hasError = true
		} else {
			srvs = append(srvs, srv)
		}
	}
	Sort(srvs)
	if hasError && len(srvs) == 0 {
		return nil, fmt.Errorf("LookupName failed: %v no valid endpoints found", zkPath)
	}
	return srvs, nil
}
