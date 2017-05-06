/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
Package consultopo implements topo.Server with consul as the backend.
*/
package consultopo

import (
	"path"
	"strings"
	"sync"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// Server is the implementation of topo.Server for consul.
type Server struct {
	// global is a client configured to talk to a list of consul instances
	// representing the global consul cluster.
	global *cellClient

	// mu protects the cells variable.
	mu sync.Mutex
	// cells contains clients configured to talk to a list of
	// consul instances representing local consul clusters. These
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

// GetKnownCells implements topo.Server.GetKnownCells.
func (s *Server) GetKnownCells(ctx context.Context) ([]string, error) {
	nodePath := path.Join(s.global.root, cellsPath) + "/"

	keys, _, err := s.global.kv.Keys(nodePath, "", nil)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		// No key starts with this prefix, means the directory
		// doesn't exist.
		return nil, topo.ErrNoNode
	}

	prefixLen := len(nodePath)
	suffix := "/" + topo.CellInfoFile
	suffixLen := len(suffix)

	var result []string
	for _, p := range keys {
		if strings.HasPrefix(p, nodePath) && strings.HasSuffix(p, suffix) {
			p = p[prefixLen : len(p)-suffixLen]
			result = append(result, p)
		}
	}

	return result, nil
}

// NewServer returns a new consultopo.Server.
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
	topo.RegisterFactory("consul", func(serverAddr, root string) (topo.Impl, error) {
		return NewServer(serverAddr, root)
	})
}
