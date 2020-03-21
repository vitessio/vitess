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

package schema

// Yes, this sucks. It's a tiny package that needs to be on its own
// It contains a data structure that's shared between sqlparser & tabletserver

import (
	"sync"
	"time"

	"vitess.io/vitess/go/vt/sqlparser"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Table types
const (
	NoType = iota
	Sequence
	Message
)

// TypeNames allows to fetch a the type name for a table.
// Count must match the number of table types.
var TypeNames = []string{
	"none",
	"sequence",
	"message",
}

// Table contains info about a table.
type Table struct {
	Name      sqlparser.TableIdent
	Fields    []*querypb.Field
	Indexes   []*Index
	PKColumns []int
	Type      int

	// SequenceInfo contains info for sequence tables.
	SequenceInfo *SequenceInfo

	// MessageInfo contains info for message tables.
	MessageInfo *MessageInfo

	// TopicInfo contains info for message topics.
	TopicInfo *TopicInfo
}

// SequenceInfo contains info specific to sequence tabels.
// It must be locked before accessing the values inside.
// If CurVal==LastVal, we have to cache new values.
// When the schema is first loaded, the values are all 0,
// which will trigger caching on first use.
type SequenceInfo struct {
	sync.Mutex
	NextVal int64
	LastVal int64
}

// TopicInfo contains info specific to message topics.
type TopicInfo struct {
	// Subscribers links to all the message tables
	// subscribed to this topic
	Subscribers []*Table
}

// MessageInfo contains info specific to message tables.
type MessageInfo struct {
	// Fields stores the field info to be
	// returned for subscribers.
	Fields []*querypb.Field

	// Optional topic to subscribe to. Any messages
	// published to the topic will be added to this table.
	Topic string

	// AckWaitDuration specifies how long to wait after
	// the message was first sent. The back-off doubles
	// every attempt.
	AckWaitDuration time.Duration

	// PurgeAfterDuration specifies the time after which
	// a successfully acked message can be deleted.
	PurgeAfterDuration time.Duration

	// BatchSize specifies the max number of events to
	// send per response.
	BatchSize int

	// CacheSize specifies the number of messages to keep
	// in cache. Anything that cannot fit in the cache
	// is sent as best effort.
	CacheSize int

	// PollInterval specifies the polling frequency to
	// look for messages to be sent.
	PollInterval time.Duration
}

// NewTable creates a new Table.
func NewTable(name string) *Table {
	return &Table{
		Name: sqlparser.NewTableIdent(name),
	}
}

// Done must be called after columns and indexes are added to
// the table. It will build additional metadata like PKColumns.
func (ta *Table) Done() {
	if len(ta.Indexes) == 0 {
		return
	}
	pkIndex := ta.Indexes[0]
	ta.PKColumns = make([]int, len(pkIndex.Columns))
	for i, pkCol := range pkIndex.Columns {
		ta.PKColumns[i] = ta.FindColumn(pkCol)
	}
}

// FindColumn finds a column in the table. It returns the index if found.
// Otherwise, it returns -1.
func (ta *Table) FindColumn(name sqlparser.ColIdent) int {
	for i, col := range ta.Fields {
		if name.EqualString(col.Name) {
			return i
		}
	}
	return -1
}

// FindPKColumn finds a pk column in the table. It returns the index if found.
// Otherwise, it returns -1.
func (ta *Table) FindPKColumn(name sqlparser.ColIdent) int {
	for i, colnum := range ta.PKColumns {
		if name.EqualString(ta.Fields[colnum].Name) {
			return i
		}
	}
	return -1
}

// GetPKColumn returns the pk column specified by the index.
func (ta *Table) GetPKColumn(index int) *querypb.Field {
	return ta.Fields[ta.PKColumns[index]]
}

// AddIndex adds an index to the table.
func (ta *Table) AddIndex(name string, unique bool) (index *Index) {
	index = NewIndex(name, unique)
	ta.Indexes = append(ta.Indexes, index)
	return index
}

// HasPrimary returns true if the table has a primary key.
func (ta *Table) HasPrimary() bool {
	return len(ta.PKColumns) != 0
}

// IsTopic returns true if TopicInfo is not nil.
func (ta *Table) IsTopic() bool {
	return ta.TopicInfo != nil
}

// UniqueIndexes returns the number of unique indexes on the table
func (ta *Table) UniqueIndexes() int {
	unique := 0
	for _, idx := range ta.Indexes {
		if idx.Unique {
			unique++
		}
	}
	return unique
}

// Index contains info about a table index.
type Index struct {
	Name   sqlparser.ColIdent
	Unique bool
	// Columns are the columns comprising the index.
	Columns []sqlparser.ColIdent
	// Cardinality[i] is the number of distinct values of Columns[i] in the
	// table.
	Cardinality []uint64
}

// NewIndex creates a new Index.
func NewIndex(name string, unique bool) *Index {
	return &Index{Name: sqlparser.NewColIdent(name), Unique: unique}
}

// AddColumn adds a column to the index.
func (idx *Index) AddColumn(name string, cardinality uint64) {
	idx.Columns = append(idx.Columns, sqlparser.NewColIdent(name))
	if cardinality == 0 {
		cardinality = uint64(len(idx.Cardinality) + 1)
	}
	idx.Cardinality = append(idx.Cardinality, cardinality)
}

// FindColumn finds a column in the index. It returns the index if found.
// Otherwise, it returns -1.
func (idx *Index) FindColumn(name sqlparser.ColIdent) int {
	for i, colName := range idx.Columns {
		if colName.Equal(name) {
			return i
		}
	}
	return -1
}
