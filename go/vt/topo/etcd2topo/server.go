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

/*
Package etcd2topo implements topo.Server with etcd as the backend.

We expect the following behavior from the etcd client library:

  - Get and Delete return ErrorCodeKeyNotFound if the node doesn't exist.
  - Create returns ErrorCodeNodeExist if the node already exists.
  - Intermediate directories are always created automatically if necessary.
  - Set returns ErrorCodeKeyNotFound if the node doesn't exist already.
  - It returns ErrorCodeTestFailed if the provided version index doesn't match.

We follow these conventions within this package:

  - Call convertError(err) on any errors returned from the etcd client library.
    Functions defined in this package can be assumed to have already converted
    errors as necessary.
*/
package etcd2topo

import (
	"sync"

	"github.com/youtube/vitess/go/vt/topo"
)

// Server is the implementation of topo.Server for etcd.
type Server struct {
	// global is a client configured to talk to a list of etcd instances
	// representing the global etcd cluster.
	global *cellClient

	// mu protects the cells variable.
	mu sync.Mutex
	// cells contains clients configured to talk to a list of
	// etcd instances representing local etcd clusters. These
	// should be accessed with the Server.clientForCell() method, which
	// will read the list of addresses for that cell from the
	// global cluster and create clients as needed.
	cells map[string]*cellClient
}

// Close implements topo.Server.Close.
// It will nil out the global and cells fields, so any attempt to
// re-use this server will panic.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.cells {
		c.close()
	}
	s.cells = nil

	s.global.close()
	s.global = nil
}

// NewServer returns a new etcdtopo.Server.
func NewServer(serverAddr, root string) (*Server, error) {
	global, err := newCellClient(serverAddr, root)
	if err != nil {
		return nil, err
	}
	return &Server{
		global: global,
		cells:  make(map[string]*cellClient),
	}, nil
}

func init() {
	topo.RegisterFactory("etcd2", func(serverAddr, root string) (topo.Impl, error) {
		return NewServer(serverAddr, root)
	})
}
