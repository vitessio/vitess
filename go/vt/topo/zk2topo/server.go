/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package zk2topo

import (
	"strings"

	"vitess.io/vitess/go/vt/topo"
)

const (
	// Path elements
	locksPath     = "locks"
	electionsPath = "elections"
)

// Factory is the zookeeper topo.Factory implementation.
type Factory struct{}

// hasObservers checks the provided address to see if it has observers.
// If so, it returns the voting server address, the observer address, and true.
// Otherwise, it returns false.
func hasObservers(serverAddr string) (string, string, bool) {
	if i := strings.Index(serverAddr, "|"); i != -1 {
		return serverAddr[:i], serverAddr[i+1:], true
	}
	return "", "", false
}

// HasGlobalReadOnlyCell is part of the topo.Factory interface.
//
// Further implementation design note: Zookeeper supports Observers:
// https://zookeeper.apache.org/doc/trunk/zookeeperObservers.html
// To use them, follow these instructions:
// * setup your observer servers as described in the previous link.
// * specify a second set of servers in serverAddr, after a '|', like:
//   global1:port1,global2:port2|observer1:port1,observer2:port2
// * if HasGlobalReadOnlyCell detects that the serverAddr has both lists,
//   it returns true.
// * the Create method below also splits the values, and if
//   cell is GlobalCell, use the left side, if cell is GlobalReadOnlyCell,
//   use the right side.
func (f Factory) HasGlobalReadOnlyCell(serverAddr, root string) bool {
	_, _, ok := hasObservers(serverAddr)
	return ok
}

// Create is part of the topo.Factory interface.
func (f Factory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	if cell == topo.GlobalCell {
		// For the global cell, extract the voting servers if we
		// have observers.
		newAddr, _, ok := hasObservers(serverAddr)
		if ok {
			serverAddr = newAddr
		}
	}
	if cell == topo.GlobalReadOnlyCell {
		// Use the observers as serverAddr.
		_, serverAddr, _ = hasObservers(serverAddr)
	}
	return NewServer(serverAddr, root), nil
}

// Server is the zookeeper topo.Conn implementation.
type Server struct {
	root string
	conn *ZkConn
}

// NewServer returns a topo.Conn connecting to real Zookeeper processes.
func NewServer(serverAddr, root string) *Server {
	return &Server{
		root: root,
		conn: Connect(serverAddr),
	}
}

// Close is part of topo.Conn interface.
func (zs *Server) Close() {
	zs.conn.Close()
	zs.conn = nil
}
func init() {
	topo.RegisterFactory("zk2", Factory{})
}
