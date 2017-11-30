/*
Copyright 2017 Google Inc.

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
	"github.com/youtube/vitess/go/vt/topo"
)

const (
	// Path elements
	locksPath     = "locks"
	electionsPath = "elections"
)

// Factory is the zookeeper topo.Factory implementation.
type Factory struct{}

// Create is part of the topo.Factory interface.
func (f Factory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	return NewServer(serverAddr, root), nil
}

// Server is the zookeeper topo.Conn implementation.
type Server struct {
	root string
	conn Conn
}

// NewServer returns a topo.Conn connecting to real Zookeeper processes.
func NewServer(serverAddr, root string) *Server {
	return &Server{
		root: root,
		conn: newRealConn(serverAddr),
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
