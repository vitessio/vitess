// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package naming

import (
	"errors"
	"fmt"
	"os"

	"code.google.com/p/vitess/go/relog"
)

var (
	// This error is returned by function to specify the
	// requested resource already exists.
	ErrNodeExists = errors.New("Node already exists")
)

// TopologyServer is the interface used to talk to a persistent
// backend storage server and locking service.
//
// Zookeeper is a good example of this, and zktopo contains the
// implementation for this using zookeeper.
//
// Inside Google, we use Chubby.
type TopologyServer interface {
	// TopologyServer management interface
	Close()

	// Tablet management, per cell.
	// The tablet string is json-encoded.

	// CreateTablet creates the given tablet, assuming it doesn't exist
	// yet. Can return ErrNodeExists if it already exists.
	CreateTablet(alias TabletAlias, contents string) error

	// UpdateTablet updates a given tablet. The version is used
	// for atomic updates (use -1 to overwrite any version)
	UpdateTablet(alias TabletAlias, contents string, existingVersion int) (newVersion int, err error)

	// GetTablet returns the tablet contents, and the current version
	GetTablet(alias TabletAlias) (contents string, version int, err error)

	// GetTabletsByCell returns all the tablets in the given cell
	GetTabletsByCell(cell string) ([]TabletAlias, error)
}

// Registry for TopologyServer implementations
var topologyServerImpls map[string]TopologyServer = make(map[string]TopologyServer)

// RegisterTopologyServer adds an implementation for a TopologyServer.
// If an implementation with that name already exists, panics.
// Call this in the 'init' function in your module.
func RegisterTopologyServer(name string, ts TopologyServer) {
	if topologyServerImpls[name] != nil {
		panic(fmt.Errorf("Duplicate TopologyServer registration for %v", name))
	}
	topologyServerImpls[name] = ts
}

// Returns a specific TopologyServer by name, or nil
func GetTopologyServerByName(name string) TopologyServer {
	return topologyServerImpls[name]
}

// Returns 'our' TopologyServer:
// - If only one is registered, that's the one.
// - If more than one are registered, use the 'VT_TOPOLOGY_SERVER'
//   environment variable.
// - Then defaults to 'zookeeper'.
// - Then panics.
func GetTopologyServer() TopologyServer {
	if len(topologyServerImpls) == 1 {
		for name, ts := range topologyServerImpls {
			relog.Info("Using only TopologyServer: %v", name)
			return ts
		}
	}

	name := os.Getenv("VT_TOPOLOGY_SERVER")
	if name == "" {
		name = "zookeeper"
	}
	result := topologyServerImpls[name]
	if result == nil {
		panic(fmt.Errorf("No TopologyServer named %v", name))
	}
	relog.Info("Using TopologyServer: %v", name)
	return result
}

// Close all registered TopologyServer
func CloseTopologyServers() {
	for name, ts := range topologyServerImpls {
		relog.Info("Closing TopologyServer: %v", name)
		ts.Close()
	}
}
