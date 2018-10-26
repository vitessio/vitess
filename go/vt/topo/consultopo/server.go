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
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/hashicorp/consul/api"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

var (
	consulAuthClientStaticFile = flag.String("consul_auth_client_static_file", "", "JSON File to read the topos/tokens from.")
)

// AuthClientCred credential to use for consul clusters
type AuthClientCred struct {
	// ACLToken when provided, the client will use this token when making requests to the Consul server.
	ACLToken string `json:"acl_token,omitempty"`
}

// Factory is the consul topo.Factory implementation.
type Factory struct{}

// HasGlobalReadOnlyCell is part of the topo.Factory interface.
func (f Factory) HasGlobalReadOnlyCell(serverAddr, root string) bool {
	return false
}

// Create is part of the topo.Factory interface.
func (f Factory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	return NewServer(cell, serverAddr, root)
}

func getClientConfig() (creds map[string]*AuthClientCred, err error) {
	creds = make(map[string]*AuthClientCred)

	if *consulAuthClientStaticFile == "" {
		// Not configured, nothing to do.
		log.Infof("Consul client auth is not set up. consul_auth_client_static_file was not provided")
		return creds, nil
	}

	data, err := ioutil.ReadFile(*consulAuthClientStaticFile)
	if err != nil {
		err = fmt.Errorf("Failed to read consul_auth_client_static_file file: %v", err)
		return creds, err
	}

	if err := json.Unmarshal(data, &creds); err != nil {
		err = fmt.Errorf(fmt.Sprintf("Error parsing auth server config: %v", err))
	}
	return creds, nil
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
func NewServer(cell, serverAddr, root string) (*Server, error) {
	creds, err := getClientConfig()
	if err != nil {
		return nil, err
	}
	cfg := api.DefaultConfig()
	cfg.Address = serverAddr
	if creds[cell] != nil {
		cfg.Token = creds[cell].ACLToken
	}
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
