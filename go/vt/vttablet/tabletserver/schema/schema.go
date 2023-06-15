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

import (
	"sync"
	"time"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/sqlparser"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Table types
const (
	NoType = iota
	Sequence
	Message
	View
)

// TypeNames allows to fetch a the type name for a table.
// Count must match the number of table types.
var TypeNames = []string{
	"none",
	"sequence",
	"message",
	"view",
}

// Table contains info about a table.
type Table struct {
	Name      sqlparser.IdentifierCS
	Fields    []*querypb.Field
	PKColumns []int
	Type      int

	// SequenceInfo contains info for sequence tables.
	SequenceInfo *SequenceInfo

	// MessageInfo contains info for message tables.
	MessageInfo *MessageInfo

	CreateTime    int64
	FileSize      uint64
	AllocatedSize uint64
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

// Reset clears the cache for the sequence. This is called to ensure that we always start with a fresh cache,
// when a new primary is elected, and, when a table is moved into a new keyspace.
// When we first need a new value from a sequence, i.e. when the schema engine sees a uninitialized sequence, it will
// get the next set of values from the backing sequence table and cache them.
func (seq *SequenceInfo) Reset() {
	seq.Lock()
	defer seq.Unlock()
	seq.NextVal = 0
	seq.LastVal = 0
}

func (seq *SequenceInfo) String() {
	seq.Lock()
	defer seq.Unlock()
	log.Infof("SequenceInfo: NextVal: %d, LastVal: %d", seq.NextVal, seq.LastVal)
}

// MessageInfo contains info specific to message tables.
type MessageInfo struct {
	// Fields stores the field info to be
	// returned for subscribers.
	Fields []*querypb.Field

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

	// MinBackoff specifies the shortest duration message manager
	// should wait before rescheduling a message
	MinBackoff time.Duration

	// MaxBackoff specifies the longest duration message manager
	// should wait before rescheduling a message
	MaxBackoff time.Duration
}

// NewTable creates a new Table.
func NewTable(name string, tableType int) *Table {
	return &Table{
		Name: sqlparser.NewIdentifierCS(name),
		Type: tableType,
	}
}

// FindColumn finds a column in the table. It returns the index if found.
// Otherwise, it returns -1.
func (ta *Table) FindColumn(name sqlparser.IdentifierCI) int {
	for i, col := range ta.Fields {
		if name.EqualString(col.Name) {
			return i
		}
	}
	return -1
}

// GetPKColumn returns the pk column specified by the index.
func (ta *Table) GetPKColumn(index int) *querypb.Field {
	return ta.Fields[ta.PKColumns[index]]
}

// HasPrimary returns true if the table has a primary key.
func (ta *Table) HasPrimary() bool {
	return len(ta.PKColumns) != 0
}
