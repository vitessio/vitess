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
	"net"
	"os"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/viperutil"
)

var gossipListenAddr = viperutil.Configure(
	"gossip-listen-addr",
	viperutil.Options[string]{
		FlagName: "gossip-listen-addr",
		Default:  "",
		Dynamic:  false,
	},
)

// registerGossipFlags adds the VTOrc-specific gossip flags to fs. Only
// the listen address is process-level; tuning (phi/ping/age) lives in
// topology so operators can change it per-keyspace without restarts.
func registerGossipFlags(fs *pflag.FlagSet) {
	fs.String("gossip-listen-addr", gossipListenAddr.Default(), "Address to bind gossip gRPC server")
	viperutil.BindFlags(fs, gossipListenAddr)
}

// GossipListenAddr returns the --gossip-listen-addr flag value.
// Consumed by the gossip gRPC server bootstrap and by GossipNodeID.
func GossipListenAddr() string {
	return gossipListenAddr.Get()
}

// gossipNodeID is computed once per process so the identifier is stable
// across gossip restarts within the same process lifetime.
var (
	gossipNodeIDOnce sync.Once
	gossipNodeID     string
)

// GossipNodeID returns a unique identifier for this VTOrc instance in the
// gossip network. It prepends the hostname to the configured listen port
// when the listen address is hostless ("[:port]"), so two VTOrcs sharing
// a port on different hosts do not collide. When the hostname is
// unavailable a random UUID is used in its place rather than the bare
// port — bare-port IDs would collapse two VTOrcs on different hosts into
// a single gossip identity, breaking the merge logic.
func GossipNodeID() string {
	gossipNodeIDOnce.Do(func() {
		listenAddr := strings.TrimSpace(GossipListenAddr())
		gossipNodeID = computeGossipNodeID(listenAddr)
	})
	return gossipNodeID
}

// computeGossipNodeID is pulled out for testability. If the listen
// address already has an explicit host (e.g. "host1:8080"), we use it
// verbatim. Otherwise we synthesize a per-process unique host component
// (hostname when available, UUID otherwise) so the resulting node ID is
// always unique across hosts even if multiple VTOrcs share the listen
// port.
func computeGossipNodeID(listenAddr string) string {
	if listenAddr == "" {
		return ""
	}
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		// Not a host:port form — fall back to a UUID so two
		// misconfigured VTOrcs cannot accidentally share an ID.
		return "vtorc-" + uuid.NewString()
	}
	if host != "" {
		return listenAddr
	}
	hostComponent := gossipHostname()
	if hostComponent == "" {
		hostComponent = "vtorc-" + uuid.NewString()
	}
	return net.JoinHostPort(hostComponent, port)
}

// gossipHostname returns the hostname for this process. Exposed as a
// var so tests can override it.
var gossipHostname = func() string {
	hostname, err := os.Hostname()
	if err != nil {
		return ""
	}
	return hostname
}
