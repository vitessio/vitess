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

package vtgate

import (
	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/srvtopo"
)

// Resolver is the layer to resolve KeyspaceIds and KeyRanges
// to shards. It will try to re-resolve shards if ScatterConn
// returns retryable error, which may imply horizontal or vertical
// resharding happened. It is implemented using a srvtopo.Resolver.
type Resolver struct {
	scatterConn *ScatterConn
	resolver    *srvtopo.Resolver
	toposerv    srvtopo.Server
	cell        string
}

// NewResolver creates a new Resolver.
func NewResolver(resolver *srvtopo.Resolver, serv srvtopo.Server, cell string, sc *ScatterConn) *Resolver {
	return &Resolver{
		scatterConn: sc,
		resolver:    resolver,
		toposerv:    serv,
		cell:        cell,
	}
}

// MessageStream streams messages.
func (res *Resolver) MessageStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, name string, callback func(*sqltypes.Result) error) error {
	var destination key.Destination
	if shard != "" {
		// If we pass in a shard, resolve the keyspace/shard
		// following redirects.
		destination = key.DestinationShard(shard)
	} else {
		// If we pass in a KeyRange, resolve it to the proper shards.
		// Note we support multiple shards here, we will just aggregate
		// the message streams.
		destination = key.DestinationExactKeyRange{KeyRange: keyRange}
	}
	rss, err := res.resolver.ResolveDestination(ctx, keyspace, topodatapb.TabletType_PRIMARY, destination)
	if err != nil {
		return err
	}
	return res.scatterConn.MessageStream(ctx, rss, name, callback)
}

// GetGatewayCacheStatus returns a displayable version of the Gateway cache.
func (res *Resolver) GetGatewayCacheStatus() TabletCacheStatusList {
	return res.scatterConn.GetGatewayCacheStatus()
}
