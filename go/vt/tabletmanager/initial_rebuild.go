// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
)

// maybeRebuildKeyspace handles the initial rebuild of SrvKeyspace if needed.
// This should be run as a background task: we use the batch context to check
// the SrvKeyspace object. If it is not there, we try to rebuild it
// for the current cell only.
func (agent *ActionAgent) maybeRebuildKeyspace(cell, keyspace string) {
	_, err := agent.TopoServer.GetSrvKeyspace(agent.batchCtx, cell, keyspace)
	switch err {
	case nil:
		// SrvKeyspace exists, we're done
		log.Infof("SrvKeyspace(%v,%v) exists, not building it", cell, keyspace)
		return
	case topo.ErrNoNode:
		log.Infof("SrvKeyspace(%v,%v) doesn't exist, rebuilding it", cell, keyspace)
		// SrvKeyspace doesn't exist, we'll try to rebuild it
	default:
		log.Warningf("Cannot read SrvKeyspace(%v,%v) (may need to run 'vtctl RebuildKeyspaceGraph %v'), skipping rebuild: %v", cell, keyspace, keyspace, err)
		return
	}

	if err := topotools.RebuildKeyspace(agent.batchCtx, logutil.NewConsoleLogger(), agent.TopoServer, keyspace, []string{cell}); err != nil {
		log.Warningf("RebuildKeyspace(%v,%v) failed: %v, may need to run 'vtctl RebuildKeyspaceGraph %v')", cell, keyspace, err, keyspace)
		return
	}

	if err := topotools.RebuildVSchema(agent.batchCtx, logutil.NewConsoleLogger(), agent.TopoServer, []string{cell}); err != nil {
		log.Warningf("RebuildVSchema(%v) failed: %v, may need to run 'vtctl RebuildVSchemaGraph --cells %v", cell, err, cell)
	}
}
