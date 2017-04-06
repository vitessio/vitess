// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package etcdtopo implements topo.Server with etcd as the backend.

We expect the following behavior from the etcd client library:

  - Get and Delete return EcodeKeyNotFound if the node doesn't exist.
  - Create returns EcodeNodeExist if the node already exists.
	- Intermediate directories are always created automatically if necessary.
	- CompareAndSwap returns EcodeKeyNotFound if the node doesn't exist already.
	  It returns EcodeTestFailed if the provided version index doesn't match.

We follow these conventions within this package:

  - Call convertError(err) on any errors returned from the etcd client library.
    Functions defined in this package can be assumed to have already converted
    errors as necessary.
*/
package etcdtopo

import (
	"sync"

	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

// Server is the implementation of topo.Server for etcd.
type Server struct {
	// _global is a client configured to talk to a list of etcd instances
	// representing the global etcd cluster. It should be accessed with the
	// Server.getGlobal() method, which will initialize _global on first
	// invocation with the list of global addresses from the command-line flag.
	_global     Client
	_globalOnce sync.Once

	// _cells contains clients configured to talk to a list of etcd instances
	// representing local etcd clusters. These should be accessed with the
	// Server.getCell() method, which will read the list of addresses for that
	// cell from the global cluster and create clients as needed.
	_cells      map[string]*cellClient
	_cellsMutex sync.Mutex
}

// Close implements topo.Server.
func (s *Server) Close() {
}

// GetKnownCells implements topo.Server.
func (s *Server) GetKnownCells(ctx context.Context) ([]string, error) {
	resp, err := s.getGlobal().Get(cellsDirPath, true /* sort */, false /* recursive */)
	if err != nil {
		return nil, convertError(err)
	}
	return getNodeNames(resp)
}

// NewServer returns a new etcdtopo.Server.
func NewServer() *Server {
	return &Server{
		_cells: make(map[string]*cellClient),
	}
}

func init() {
	topo.RegisterFactory("etcd", func(serverAddr, root string) (topo.Impl, error) {
		return NewServer(), nil
	})
}
