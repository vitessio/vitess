// Copyright 201b3, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk2topo

import (
	"fmt"
	"path"
	"sync"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

const (
	// Path elements

	cellsPath     = "cells"
	keyspacesPath = "keyspaces"
	shardsPath    = "shards"
	tabletsPath   = "tablets"
	locksPath     = "locks"
	electionsPath = "elections"
)

// instance is holding a Zookeeper connection, and the root directory
// to use on it. It is used for the global cell, and for individual cells.
type instance struct {
	root string
	conn Conn
}

// Server is the zookeeper topo.Impl implementation.
type Server struct {
	// mu protects the following fields.
	mu sync.Mutex
	// instances is a map of cell name to instance.
	instances map[string]*instance
}

// NewServer returns a Server connecting to real Zookeeper processes.
func NewServer(serverAddr, root string) *Server {
	return &Server{
		instances: map[string]*instance{
			topo.GlobalCell: {
				root: root,
				conn: newRealConn(serverAddr),
			},
		},
	}
}

func init() {
	topo.RegisterFactory("zk2", func(serverAddr, root string) (topo.Impl, error) {
		return NewServer(serverAddr, root), nil
	})
}

// connForCell returns the Conn and root for a cell. It creates it if
// it doesn't exist.
func (zs *Server) connForCell(ctx context.Context, cell string) (Conn, string, error) {
	zs.mu.Lock()
	defer zs.mu.Unlock()
	ins, ok := zs.instances[cell]
	if ok {
		return ins.conn, ins.root, nil
	}

	// We do not have a connection yet, let's try to read the CellInfo.
	// We can't use zs.Get() as we are holding the lock.
	ins, ok = zs.instances[topo.GlobalCell]
	if !ok {
		// This should not happen, as we always create and
		// keep the 'global' record entry.
		return nil, "", fmt.Errorf("programming error: no global cell, cannot read CellInfo for cell %v", cell)
	}
	zkPath := path.Join(ins.root, cellsPath, cell, topo.CellInfoFile)
	data, _, err := ins.conn.Get(ctx, zkPath)
	if err != nil {
		return nil, "", convertError(err)
	}
	ci := &topodatapb.CellInfo{}
	if err := proto.Unmarshal(data, ci); err != nil {
		return nil, "", fmt.Errorf("cannot Unmarshal CellInfo for cell %v: %v", cell, err)
	}
	ins = &instance{
		root: ci.Root,
		conn: Connect(ci.ServerAddress),
	}
	zs.instances[cell] = ins
	return ins.conn, ins.root, nil
}

// Close is part of topo.Impl interface.
func (zs *Server) Close() {
	zs.mu.Lock()
	defer zs.mu.Unlock()
	for _, ins := range zs.instances {
		ins.conn.Close()
	}
	zs.instances = nil
}
