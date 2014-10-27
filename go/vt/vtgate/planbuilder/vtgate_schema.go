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

type VTGateSchema struct {
	Tables map[string]*VTGateTable
}

type VTGateTable struct {
	Keyspace      *Keyspace
	KeyspaceIDCol string
	Indexes       []*VTGateIndex
}

type VTGateIndex struct {
	Type   int
	Column string
	Lookup *VTGateLookup
	// TODO(sougou): Currently unused.
	IsOwned bool
}

type VTGateLookup struct {
	Name     string
	From, To string
}

type Keyspace struct {
	Name           string
	ShardingScheme int
}

/*
var user = &Keyspace{
	Name:           "user",
	ShardingScheme: HashSharded,
}

var musicUserLookup = &VTGateLookup{
	Name: "music_user_map",
	From: "music_id",
	To:   "user_id",
}

var vtgateSchema = &VTGateSchema{
	Table: map[string]*VTGateTable{
		"user": {
			Keyspace:      user,
			KeyspaceIDCol: "keyspace_id",
			Indexes: []*VTGateIndex{{
				Type:   Primary,
				Column: "id",
			}},
		},
		"user_extra": {
			Keyspace:      user,
			KeyspaceIDCol: "keyspace_id",
			Indexes: []*VTGateIndex{{
				Type:   Primary,
				Column: "user_id",
			}},
		},
		"music": {
			Keyspace:      user,
			KeyspaceIDCol: "keyspace_id",
			Indexes: []*VTGateIndex{{
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
			Indexes: []*VTGateIndex{{
				Type:   Lookup,
				Column: "music_id",
				Lookup: musicUserLookup,
			}},
		},
	},
}
*/
