// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"encoding/json"
	"fmt"

	"github.com/youtube/vitess/go/jscfg"
)

// Keyspace types.
const (
	Unsharded = iota
	HashSharded
)

// Index types.
const (
	ShardKey = iota
	LookupIndex
)

// Schema represents the denormalized version of SchemaFormal,
// used for building routing plans.
type Schema struct {
	Tables map[string]*Table
}

// Table represnts a table in Schema.
type Table struct {
	Name     string
	Keyspace *Keyspace
	Indexes  []*Index
}

// MarshalJSON should only be used for testing.
func (t *Table) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Name)
}

// Keyspace contains the keyspcae info for each Table.
type Keyspace struct {
	Name string
	// ShardingScheme is Unsharded or HashSharded.
	ShardingScheme int
}

// Index contains the index info for each index of a table.
type Index struct {
	// Type is ShardKey or LookupIndex.
	Type      int
	Column    string
	Name      string
	From, To  string
	Owner     string
	IsAutoInc bool
}

// BuildSchema builds a Schema from a SchemaFormal.
func BuildSchema(source *SchemaFormal) (schema *Schema, err error) {
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
				Name:     tname,
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

// LookupTable returns a pointer to the Table if found.
// Otherwise, it returns a reason, which is equivalent to an error.
func (schema *Schema) LookupTable(tablename string) (table *Table, reason string) {
	if tablename == "" {
		return nil, "complex table expression"
	}
	table = schema.Tables[tablename]
	if table == nil {
		return nil, fmt.Sprintf("table %s not found", tablename)
	}
	return table, ""
}

// SchemaFormal is the formal representation of the schema
// as loaded from the source.
type SchemaFormal struct {
	Keyspaces map[string]KeyspaceFormal
}

// KeyspaceFormal is the keyspace info for each keyspace
// as loaded from the source.
type KeyspaceFormal struct {
	ShardingScheme int
	Indexes        map[string]IndexFormal
	Tables         map[string]TableFormal
}

// IndexFormal is the info for each index as loaded from
// the source.
type IndexFormal struct {
	// Type is ShardKey or LookupIndex.
	Type      int
	From, To  string
	Owner     string
	IsAutoInc bool
}

// TableFormal is the info for each table as loaded from
// the source.
type TableFormal struct {
	IndexColumns []IndexColumnFormal
}

// IndexColumnFormal is the info for each indexed column
// of a table as loaded from the source.
type IndexColumnFormal struct {
	Column    string
	IndexName string
}

// LoadSchemaJSON loads the formal representation of a schema
// from a JSON file and returns the more usable denormalized
// representaion (Schema) for it.
func LoadSchemaJSON(filename string) (schema *Schema, err error) {
	var source SchemaFormal
	if err := jscfg.ReadJson(filename, &source); err != nil {
		return nil, err
	}
	return BuildSchema(&source)
}
