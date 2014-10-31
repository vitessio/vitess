// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

const (
	ShardKey = iota
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
	Keyspace *Keyspace
	Indexes  []*VTGateIndex
}

type VTGateIndex struct {
	Column    string
	Name      string
	From, To  string
	Owner     string
	IsAutoInc bool
}

type Keyspace struct {
	Name           string
	ShardingScheme int
	Lookupdb       string
}

type VTGateSchemaMetadata struct {
	Keyspaces map[string]struct {
		ShardingScheme int
		Lookupdb       string
		Indexes        map[string]struct {
			From, To  string
			Owner     string
			IsAutoInc bool
		}
		Tables map[string]struct {
			IndexColumns []struct {
				Column    string
				IndexName string
			}
		}
	}
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
			Indexes: []*VTGateIndex{{
				Type:   ShardKey,
				Column: "id",
			}},
		},
		"user_extra": {
			Keyspace:      user,
			Indexes: []*VTGateIndex{{
				Type:   ShardKey,
				Column: "user_id",
			}},
		},
		"music": {
			Keyspace:      user,
			Indexes: []*VTGateIndex{{
				Type:   ShardKey,
				Column: "user_id",
			}, {
				Type:   Lookup,
				Column: "id",
				Lookup: musicUserLookup,
			}},
		},
		"music_extra": {
			Keyspace:      user,
			Indexes: []*VTGateIndex{{
				Type:   Lookup,
				Column: "music_id",
				Lookup: musicUserLookup,
			}},
		},
	},
}
*/
