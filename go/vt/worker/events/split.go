package events

import (
	base "vitess.io/vitess/go/vt/events"
)

// SplitClone is an event that describes a single step in a horizontal
// split clone.
type SplitClone struct {
	base.StatusUpdater

	Keyspace, Shard, Cell string
	ExcludeTables         []string
}

// VerticalSplitClone is an event that describes a single step in a vertical
// split clone.
type VerticalSplitClone struct {
	base.StatusUpdater

	Keyspace, Shard, Cell string
	Tables                []string
}
