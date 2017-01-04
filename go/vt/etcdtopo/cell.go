// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"fmt"
	"path"
	"strings"

	log "github.com/golang/glog"
)

// cellClient wraps a Client for keeping track of cell-local clusters.
type cellClient struct {
	Client

	// version is the etcd ModifiedIndex of the cell record we read from the
	// global cluster for this client.
	version int64
}

func (s *Server) getCellList() ([]string, error) {
	resp, err := s.getGlobal().Get(cellsDirPath, true /* sort */, false /* recursive */)
	if err != nil {
		return nil, convertError(err)
	}
	if resp.Node == nil {
		return nil, ErrBadResponse
	}
	var cells []string
	for _, node := range resp.Node.Nodes {
		cells = append(cells, path.Base(node.Key))
	}
	return cells, nil
}

// cell returns a client for the given cell-local etcd cluster.
// It caches clients for previously requested cells.
func (s *Server) getCell(cell string) (*cellClient, error) {
	// Return a cached client if present.
	s._cellsMutex.Lock()
	client, ok := s._cells[cell]
	s._cellsMutex.Unlock()
	if ok {
		return client, nil
	}

	// Fetch cell cluster addresses from the global cluster.
	// These can proceed concurrently (we've released the lock).
	addrs, version, err := s.getCellAddrs(cell)
	if err != nil {
		return nil, err
	}

	// Update the cache.
	s._cellsMutex.Lock()
	defer s._cellsMutex.Unlock()

	// Check if another goroutine beat us to creating a client for this cell.
	if client, ok = s._cells[cell]; ok {
		// Update the client only if we've fetched newer data.
		if version > client.version {
			client.SetCluster(addrs)
			client.version = version
		}
		return client, nil
	}

	// Create the client.
	client = &cellClient{Client: newEtcdClient(addrs), version: version}
	s._cells[cell] = client
	return client, nil
}

// getCellAddrs returns the list of etcd servers to try for the given cell-local
// cluster. These lists are stored in the global etcd cluster.
// The etcd ModifiedIndex (version) of the node is also returned.
func (s *Server) getCellAddrs(cell string) ([]string, int64, error) {
	nodePath := cellFilePath(cell)
	resp, err := s.getGlobal().Get(nodePath, false /* sort */, false /* recursive */)
	if err != nil {
		return nil, -1, convertError(err)
	}
	if resp.Node == nil {
		return nil, -1, ErrBadResponse
	}
	if resp.Node.Value == "" {
		return nil, -1, fmt.Errorf("cell node %v is empty, expected list of addresses", nodePath)
	}

	return strings.Split(resp.Node.Value, ","), int64(resp.Node.ModifiedIndex), nil
}

func (s *Server) getGlobal() Client {
	s._globalOnce.Do(func() {
		if len(globalAddrs) == 0 {
			// This means either a TopoServer method was called before flag parsing,
			// or the flag was not specified. Either way, it is a fatal condition.
			log.Fatal("etcdtopo: list of addresses for global cluster is empty")
		}
		log.Infof("etcdtopo: global address list = %v", globalAddrs)
		s._global = newEtcdClient([]string(globalAddrs))
	})

	return s._global
}
