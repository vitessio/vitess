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
	"strings"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/viperutil"
	"vitess.io/vitess/go/vt/gossip"
)

var (
	gossipEnabled = viperutil.Configure(
		"gossip-enabled",
		viperutil.Options[bool]{
			FlagName: "gossip-enabled",
			Default:  false,
			Dynamic:  true,
		},
	)
	gossipListenAddr = viperutil.Configure(
		"gossip-listen-addr",
		viperutil.Options[string]{
			FlagName: "gossip-listen-addr",
			Default:  "",
			Dynamic:  false,
		},
	)
	gossipSeedAddrs = viperutil.Configure(
		"gossip-seed-addrs",
		viperutil.Options[[]string]{
			FlagName: "gossip-seed-addrs",
			Default:  []string{},
			Dynamic:  false,
		},
	)
	gossipPhiThreshold = viperutil.Configure(
		"gossip-phi-threshold",
		viperutil.Options[float64]{
			FlagName: "gossip-phi-threshold",
			Default:  4,
			Dynamic:  false,
		},
	)
	gossipPingInterval = viperutil.Configure(
		"gossip-ping-interval",
		viperutil.Options[time.Duration]{
			FlagName: "gossip-ping-interval",
			Default:  1 * time.Second,
			Dynamic:  false,
		},
	)
	gossipProbeTimeout = viperutil.Configure(
		"gossip-probe-timeout",
		viperutil.Options[time.Duration]{
			FlagName: "gossip-probe-timeout",
			Default:  500 * time.Millisecond,
			Dynamic:  false,
		},
	)
	gossipMaxUpdateAge = viperutil.Configure(
		"gossip-max-update-age",
		viperutil.Options[time.Duration]{
			FlagName: "gossip-max-update-age",
			Default:  5 * time.Second,
			Dynamic:  false,
		},
	)
)

func registerGossipFlags(fs *pflag.FlagSet) {
	fs.Bool("gossip-enabled", gossipEnabled.Default(), "Enable gossip protocol participation")
	fs.String("gossip-listen-addr", gossipListenAddr.Default(), "Address to bind gossip gRPC server")
	fs.StringSlice("gossip-seed-addrs", gossipSeedAddrs.Default(), "Comma-separated list of gossip seed addresses")
	fs.Float64("gossip-phi-threshold", gossipPhiThreshold.Default(), "Phi accrual threshold for suspecting peers")
	fs.Duration("gossip-ping-interval", gossipPingInterval.Default(), "Gossip ping interval")
	fs.Duration("gossip-probe-timeout", gossipProbeTimeout.Default(), "Gossip probe timeout")
	fs.Duration("gossip-max-update-age", gossipMaxUpdateAge.Default(), "Max age before marking peer down")

	viperutil.BindFlags(fs,
		gossipEnabled,
		gossipListenAddr,
		gossipSeedAddrs,
		gossipPhiThreshold,
		gossipPingInterval,
		gossipProbeTimeout,
		gossipMaxUpdateAge,
	)
}

func GossipEnabled() bool {
	return gossipEnabled.Get()
}

func GossipListenAddr() string {
	return gossipListenAddr.Get()
}

func GossipSeeds() []gossip.Member {
	seeds := gossipSeedAddrs.Get()
	members := make([]gossip.Member, 0, len(seeds))
	for _, addr := range seeds {
		if addr == "" {
			continue
		}
		members = append(members, gossip.Member{ID: gossip.NodeID(addr), Addr: addr})
	}
	return members
}

func GossipPhiThreshold() float64 {
	return gossipPhiThreshold.Get()
}

func GossipPingInterval() time.Duration {
	return gossipPingInterval.Get()
}

func GossipProbeTimeout() time.Duration {
	return gossipProbeTimeout.Get()
}

func GossipMaxUpdateAge() time.Duration {
	return gossipMaxUpdateAge.Get()
}

func GossipNodeID() string {
	return strings.TrimSpace(GossipListenAddr())
}
