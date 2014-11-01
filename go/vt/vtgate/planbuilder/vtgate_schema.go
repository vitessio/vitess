// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"fmt"

	"github.com/youtube/vitess/go/jscfg"
)

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

func BuildSchema(source *VTGateSchemaNormalized) (schema *VTGateSchema, err error) {
	schema = &VTGateSchema{Tables: make(map[string]*VTGateTable)}
	for ksname, ks := range source.Keyspaces {
		keyspace := &VTGateKeyspace{
			Name:           ksname,
			ShardingScheme: ks.ShardingScheme,
			Lookupdb:       ks.Lookupdb,
		}
		for tname, table := range ks.Tables {
			t := &VTGateTable{
				Keyspace: keyspace,
				Indexes:  make([]*VTGateIndex, 0, len(table.IndexColumns)),
			}
			for _, ind := range table.IndexColumns {
				idx, ok := ks.Indexes[ind.IndexName]
				if !ok {
					return nil, fmt.Errorf("index %s not found for table %s", ind.IndexName, tname)
				}
				t.Indexes = append(t.Indexes, &VTGateIndex{
					Column:    ind.Column,
					Name:      ind.IndexName,
					From:      idx.From,
					To:        idx.To,
					Owner:     idx.Owner,
					IsAutoInc: idx.IsAutoInc,
				})
			}
			schema.Tables[tname] = t
		}
	}
	return schema, nil
}

type VTGateTable struct {
	Keyspace *VTGateKeyspace
	Indexes  []*VTGateIndex
}

type VTGateIndex struct {
	Column    string
	Name      string
	From, To  string
	Owner     string
	IsAutoInc bool
}

type VTGateKeyspace struct {
	Name           string
	ShardingScheme int
	Lookupdb       string
}

type VTGateSchemaNormalized struct {
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

func LoadSchemaJSON(filename string) (schema *VTGateSchema, err error) {
	var source VTGateSchemaNormalized
	if err := jscfg.ReadJson(filename, &source); err != nil {
		return nil, err
	}
	return BuildSchema(&source)
}
