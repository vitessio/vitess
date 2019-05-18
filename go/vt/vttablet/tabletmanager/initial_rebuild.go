/*
Copyright 2017 Google Inc.

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

package tabletmanager

import (
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topotools"
)

// maybeRebuildKeyspace handles the initial rebuild of SrvKeyspace if needed.
// This should be run as a background task: we use the batch context to check
// the SrvKeyspace object. If it is not there, we try to rebuild it
// for the current cell only.
func (agent *ActionAgent) maybeRebuildKeyspace(cell, keyspace string) {
	_, err := agent.TopoServer.GetSrvKeyspace(agent.batchCtx, cell, keyspace)
	switch {
	case err == nil:
		// SrvKeyspace exists, we're done
		log.Infof("SrvKeyspace(%v,%v) exists, not building it", cell, keyspace)
		return
	case topo.IsErrType(err, topo.NoNode):
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

	if err := agent.TopoServer.RebuildSrvVSchema(agent.batchCtx, []string{cell}); err != nil {
		log.Warningf("RebuildVSchema(%v) failed: %v, may need to run 'vtctl RebuildVSchemaGraph --cells %v", cell, err, cell)
	}
}
