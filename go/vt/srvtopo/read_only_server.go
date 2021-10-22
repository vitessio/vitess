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

package srvtopo

import (
	"fmt"

	"vitess.io/vitess/go/vt/topo"
)

// NewReadOnlyServer wraps the server passed by the provided implementation
// and prevents any changes from being made to its associated topo server.
func NewReadOnlyServer(underlying Server) (ReadOnlyServer, error) {
	ros := ReadOnlyServer{underlying: underlying}

	if underlying == nil {
		return ros, ErrNilUnderlyingServer
	}

	topoServer, err := underlying.GetTopoServer()
	if err != nil || topoServer == nil {
		return ros, topo.NewError(topo.NoImplementation, fmt.Sprintf("Could not get underlying topo server: %v", err))
	}
	topoServer.SetReadOnly(true)

	return ros, nil
}

// ReadOnlyServer wraps an underlying Server to ensure the topo server access is read-only
type ReadOnlyServer struct {
	underlying Server
}

// GetTopoServer returns a read-only topo server
func (ros *ReadOnlyServer) GetTopoServer() (*topo.Server, error) {
	// Let's ensure it's read-only
	topoServer, err := ros.underlying.GetTopoServer()
	if err != nil || topoServer == nil {
		return nil, topo.NewError(topo.NoImplementation, fmt.Sprintf("Could not get underlying topo server: %v", err))
	}
	topoServer.SetReadOnly(true)
	if !topoServer.IsReadOnly() {
		return nil, topo.NewError(topo.NoReadOnlyImplementation, fmt.Sprintf("Could not provide read-only topo server: %v", err))
	}
	return topoServer, err
}
