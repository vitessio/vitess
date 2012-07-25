// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"code.google.com/p/vitess/go/zk"

//	"launchpad.net/gozk/zookeeper"
)

/*
These functions deal with keeping data in the shard graph up to date.

The shard graph is the client-side view of the cluster and is derived from canonical
data sources in zk.

 Some of this could be implemented by watching zk nodes, but there are enough cases where
 automatic updating is undersirable. Instead, these functions are called where appropriate.
*/

// A given tablet should only appear in once in the serving graph.
// A given tablet can change address, but if it changes db type, so all db typenodes need to be scanned
// and updated as appropriate. That is handled by UpdateServingGraphForShard.
// If no entry is found for this tablet an error is returned.
func UpdateServingGraphForTablet(zconn zk.Conn, tablet *Tablet) error {
	return nil
}

// Recompute all nodes in the serving graph for a list of tablets in the shard.
// This will overwrite existing data.
func UpdateServingGraphForShard(zconn zk.Conn, tablet *[]Tablet) error {
	return nil
}
