// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

const (
	Primary = iota
	Lookup
)

const (
	Unsharded = iota
	HashSharded
)

type GateSchema struct {
	Tables map[string]GateTable
}

type GateTable struct {
	Keyspace      *Keyspace
	KeyspaceIDCol string
	Indexes       []*GateIndex
}

type GateIndex struct {
	Type   int
	Column string
	Lookup *GateLookup
	// TODO(sougou): Currently unused.
	IsOwned bool
}

type GateLookup struct {
	Name     string
	From, To string
}

type Keyspace struct {
	Name           string
	ShardingScheme int
}

var user = &Keyspace{
	Name:           "user",
	ShardingScheme: HashSharded,
}

var musicUserLookup = &GateLookup{
	Name: "music_user_map",
	From: "music_id",
	To:   "user_id",
}

var gateSchema = map[string]*GateTable{
	"user": {
		Keyspace:      user,
		KeyspaceIDCol: "keyspace_id",
		Indexes: []*GateIndex{{
			Type:   Primary,
			Column: "id",
		}},
	},
	"user_extra": {
		Keyspace:      user,
		KeyspaceIDCol: "keyspace_id",
		Indexes: []*GateIndex{{
			Type:   Primary,
			Column: "user_id",
		}},
	},
	"music": {
		Keyspace:      user,
		KeyspaceIDCol: "keyspace_id",
		Indexes: []*GateIndex{{
			Type:   Primary,
			Column: "user_id",
		}, {
			Type:   Lookup,
			Column: "id",
			Lookup: musicUserLookup,
		}},
	},
	"music_extra": {
		Keyspace:      user,
		KeyspaceIDCol: "keyspace_id",
		Indexes: []*GateIndex{{
			Type:   Lookup,
			Column: "music_id",
			Lookup: musicUserLookup,
		}},
	},
}
