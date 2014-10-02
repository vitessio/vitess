// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package events

import base "github.com/youtube/vitess/go/vt/events"

// SplitClone is an event that describes a single step in a horizontal
// split clone.
type SplitClone struct {
	base.StatusUpdater

	Keyspace, Shard, Cell string
	ExcludeTables         []string
	Strategy              string
}

// VerticalSplitClone is an event that describes a single step in a vertical
// split clone.
type VerticalSplitClone struct {
	base.StatusUpdater

	Keyspace, Shard, Cell string
	Tables                []string
	Strategy              string
}
