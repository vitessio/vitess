// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package events

import (
	base "github.com/youtube/vitess/go/vt/events"
	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// MigrateServedFrom is an event that describes a single step in the process of
// adding or removing a forwarding rule to have certain ServedTypes served by
// another keyspace.
type MigrateServedFrom struct {
	base.StatusUpdater

	KeyspaceName     string
	SourceShard      topo.ShardInfo
	DestinationShard topo.ShardInfo
	ServedType       topodatapb.TabletType
	Reverse          bool
}

// MigrateServedTypes is an event that describes a single step in the process of
// switching a ServedType from one set of shards to another.
type MigrateServedTypes struct {
	base.StatusUpdater

	KeyspaceName      string
	SourceShards      []*topo.ShardInfo
	DestinationShards []*topo.ShardInfo
	ServedType        topodatapb.TabletType
	Reverse           bool
}
