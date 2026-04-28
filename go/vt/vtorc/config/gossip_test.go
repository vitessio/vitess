/*
Copyright 2026 The Vitess Authors.

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

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestComputeGossipNodeIDUsesHostnameForWildcardListenAddrs prevents
// bind-all listeners from sharing one gossip identity.
func TestComputeGossipNodeIDUsesHostnameForWildcardListenAddrs(t *testing.T) {
	origHostname := gossipHostname
	gossipHostname = func() string { return "vtorc-host" }
	t.Cleanup(func() { gossipHostname = origHostname })

	for _, listenAddr := range []string{":16110", "0.0.0.0:16110", "[::]:16110"} {
		assert.Equal(t, "vtorc-host:16110", computeGossipNodeID(listenAddr))
	}
	assert.Equal(t, "10.0.0.1:16110", computeGossipNodeID("10.0.0.1:16110"))
	assert.Equal(t, "[2001:db8::1]:16110", computeGossipNodeID("[2001:db8::1]:16110"))
}

// TestComputeGossipAdvertiseAddrUsesHostnameForWildcardListenAddrs prevents
// VTOrc from advertising bind-all addresses that peers cannot dial.
func TestComputeGossipAdvertiseAddrUsesHostnameForWildcardListenAddrs(t *testing.T) {
	origHostname := gossipHostname
	gossipHostname = func() string { return "vtorc-host" }
	t.Cleanup(func() { gossipHostname = origHostname })

	for _, listenAddr := range []string{":16110", "0.0.0.0:16110", "[::]:16110"} {
		assert.Equal(t, "vtorc-host:16110", computeGossipAdvertiseAddr(listenAddr))
	}
	assert.Equal(t, "10.0.0.1:16110", computeGossipAdvertiseAddr("10.0.0.1:16110"))
	assert.Equal(t, "[2001:db8::1]:16110", computeGossipAdvertiseAddr("[2001:db8::1]:16110"))
}
