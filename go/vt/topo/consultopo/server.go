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
	"sync"

	"github.com/hashicorp/consul/api"

	"github.com/youtube/vitess/go/vt/topo"
)

// Factory is the consul topo.Factory implementation.
type Factory struct{}

// HasGlobalReadOnlyCell is part of the topo.Factory interface.
func (f Factory) HasGlobalReadOnlyCell(serverAddr, root string) bool {
	return false
}

// Create is part of the topo.Factory interface.
func (f Factory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	return NewServer(serverAddr, root)
}

// Server is the implementation of topo.Server for consul.
type Server struct {
	// client is the consul api client.
	client *api.Client
	kv     *api.KV

	// root is the root path for this client.
	root string

	// mu protects the following fields.
	mu sync.Mutex
	// locks is a map of *lockInstance structures.
	// The key is the filepath of the Lock file.
	locks map[string]*lockInstance
}

// lockInstance keeps track of one lock held by this client.
type lockInstance struct {
	// lock has the api.Lock structure.
	lock *api.Lock

	// done is closed when the lock is release by this process.
	done chan struct{}
}

// NewServer returns a new consultopo.Server.
func NewServer(serverAddr, root string) (*Server, error) {
	cfg := api.DefaultConfig()
	cfg.Address = serverAddr
	client, err := api.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	return &Server{
		client: client,
		kv:     client.KV(),
		root:   root,
		locks:  make(map[string]*lockInstance),
	}, nil
}

// Close implements topo.Server.Close.
// It will nil out the global and cells fields, so any attempt to
// re-use this server will panic.
func (s *Server) Close() {
	s.client = nil
	s.kv = nil
	s.mu.Lock()
	defer s.mu.Unlock()
	s.locks = nil
}

func init() {
	topo.RegisterFactory("consul", Factory{})
}
