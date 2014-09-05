// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package events

import (
	base "github.com/youtube/vitess/go/vt/events"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
)

// MultiSnapshot is an event that triggers when a tablet has completed a
// filtered snapshot.
type MultiSnapshot struct {
	base.StatusUpdater

	Tablet topo.Tablet
	Args   actionnode.MultiSnapshotArgs
}

// MultiRestore is an event that triggers when a tablet has completed a filtered
// snapshot restore.
type MultiRestore struct {
	base.StatusUpdater

	Tablet topo.Tablet
	Args   actionnode.MultiRestoreArgs
}
