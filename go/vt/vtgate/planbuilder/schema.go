// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"fmt"

	"github.com/youtube/vitess/go/jscfg"
)

const (
	Unsharded = iota
	HashSharded
)

const (
	ShardKey = iota
	Lookup
)

type Schema struct {
	Tables map[string]*Table
}

func BuildSchema(source *SchemaNormalized) (schema *Schema, err error) {
	allindexes := make(map[string]string)
	schema = &Schema{Tables: make(map[string]*Table)}
	for ksname, ks := range source.Keyspaces {
		keyspace := &Keyspace{
			Name:           ksname,
			ShardingScheme: ks.ShardingScheme,
		}
		for tname, table := range ks.Tables {
			if _, ok := schema.Tables[tname]; ok {
				return nil, fmt.Errorf("table %s has multiple definitions", tname)
			}
			t := &Table{
				Keyspace: keyspace,
				Indexes:  make([]*Index, 0, len(table.IndexColumns)),
			}
			for i, ind := range table.IndexColumns {
				idx, ok := ks.Indexes[ind.IndexName]
				if !ok {
					return nil, fmt.Errorf("index %s not found for table %s", ind.IndexName, tname)
				}
				if i == 0 && idx.Type != ShardKey {
					return nil, fmt.Errorf("first index is not ShardKey for table %s", tname)
				}
				switch prevks := allindexes[ind.IndexName]; prevks {
				case "":
					allindexes[ind.IndexName] = ksname
				case ksname:
					// We're good.
				default:
					return nil, fmt.Errorf("index %s used in more than one keyspace: %s %s", ind.IndexName, prevks, ksname)
				}
				t.Indexes = append(t.Indexes, &Index{
					Type:      idx.Type,
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

type Table struct {
	Keyspace *Keyspace
	Indexes  []*Index
}

type Keyspace struct {
	Name string
	// ShardingScheme is Unsharded or HashSharded.
	ShardingScheme int
}

type Index struct {
	// Type is ShardKey or Lookup.
	Type      int
	Column    string
	Name      string
	From, To  string
	Owner     string
	IsAutoInc bool
}

type SchemaNormalized struct {
	Keyspaces map[string]struct {
		ShardingScheme int
		Indexes        map[string]struct {
			// Type is ShardKey or Lookup.
			Type      int
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

func LoadSchemaJSON(filename string) (schema *Schema, err error) {
	var source SchemaNormalized
	if err := jscfg.ReadJson(filename, &source); err != nil {
		return nil, err
	}
	return BuildSchema(&source)
}
