// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// CfgClient interface is registered with upstream service
// such as Zookeeper which can actively push configuration
// updates to the downstream CfgClient.
package cfgclient

type CfgClient interface {
	// UpdateCfg() receives configuration updates from
	// upstream services. It is up to the implementation
	// how the new configuration will be applied
	UpdateCfg([]byte) error

	// GetCfg() returns a recent version of current configuration
	// like UpdateCfg(), it is up to the implementation how
	// the current configuration is retrieved, a possible implementation
	// is to cache the most recent configuration in UpdateCfg()
	GetCfg() ([]byte, error)

	// Init() register CfgClient with its upstream server, thus
	// CfgClient.UpdateCfg() would be called when there is a configuration
	// update, Init() may also do other initialization tasks, depending on
	// implementations
	Init(map[string](interface{})) error
}
