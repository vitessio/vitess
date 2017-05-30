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

package etcd2topo

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// cellClient wraps a Client for keeping track of cell-local clusters.
type cellClient struct {
	// cli is the v3 client.
	cli *clientv3.Client

	// root is the root path for this client.
	root string
}

// newCellClient returns a new cellClient for the given address and root.
func newCellClient(serverAddr, root string) (*cellClient, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(serverAddr, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &cellClient{
		cli:  cli,
		root: root,
	}, nil
}

func (c *cellClient) close() {
	c.cli.Close()
}

// cell returns a client for the given cell-local etcd cluster.
// It caches clients for previously requested cells.
func (s *Server) clientForCell(ctx context.Context, cell string) (*cellClient, error) {
	// Global cell is the easy case.
	if cell == topo.GlobalCell {
		return s.global, nil
	}

	// Return a cached client if present.
	s.mu.Lock()
	client, ok := s.cells[cell]
	s.mu.Unlock()
	if ok {
		return client, nil
	}

	// Fetch cell cluster addresses from the global cluster.
	// These can proceed concurrently (we've released the lock).
	serverAddr, root, err := s.getCellAddrs(ctx, cell)
	if err != nil {
		return nil, err
	}

	// Update the cache.
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if another goroutine beat us to creating a client for
	// this cell.
	if client, ok = s.cells[cell]; ok {
		return client, nil
	}

	// Create the client.
	c, err := newCellClient(serverAddr, root)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %v: %v", serverAddr, err)
	}
	s.cells[cell] = c
	return c, nil
}

// getCellAddrs returns the list of etcd servers to try for the given
// cell-local cluster, and the root directory. These lists are stored
// in the global etcd cluster.
func (s *Server) getCellAddrs(ctx context.Context, cell string) (string, string, error) {
	nodePath := path.Join(s.global.root, cellsPath, cell, topo.CellInfoFile)
	resp, err := s.global.cli.Get(ctx, nodePath)
	if err != nil {
		return "", "", convertError(err)
	}
	if len(resp.Kvs) != 1 {
		return "", "", topo.ErrNoNode
	}
	ci := &topodatapb.CellInfo{}
	if err := proto.Unmarshal(resp.Kvs[0].Value, ci); err != nil {
		return "", "", fmt.Errorf("cannot unmarshal cell node %v: %v", nodePath, err)
	}
	if ci.ServerAddress == "" {
		return "", "", fmt.Errorf("CellInfo.ServerAddress node %v is empty, expected list of addresses", nodePath)
	}

	return ci.ServerAddress, ci.Root, nil
}
