/*
Copyright 2021 The Vitess Authors.

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

package logic

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/vt/orchestrator/attributes"
	"vitess.io/vitess/go/vt/orchestrator/kv"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topotools/events"

	"vitess.io/vitess/go/vt/orchestrator/config"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/inst"
)

// VtOrcReparentFunctions is the VtOrc implementation for ReparentFunctions
type VtOrcReparentFunctions struct {
	analysisEntry        inst.ReplicationAnalysis
	candidateInstanceKey *inst.InstanceKey
	skipProcesses        bool
	topologyRecovery     *TopologyRecovery
	promotedReplica      *inst.Instance
	recoveryAttempted    bool
	hasBestPromotionRule bool
}

// PostERSCompletionHook implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) PostERSCompletionHook(ctx context.Context, ev *events.Reparent, logger logutil.Logger, tmc tmclient.TabletManagerClient) {
	if vtorcReparent.promotedReplica != nil {
		message := fmt.Sprintf("promoted replica: %+v", vtorcReparent.promotedReplica.Key)
		AuditTopologyRecovery(vtorcReparent.topologyRecovery, message)
		inst.AuditOperation("recover-dead-master", &vtorcReparent.analysisEntry.AnalyzedInstanceKey, message)
	}
	// And this is the end; whether successful or not, we're done.
	resolveRecovery(vtorcReparent.topologyRecovery, vtorcReparent.promotedReplica)
	// Now, see whether we are successful or not. From this point there's no going back.
	if vtorcReparent.promotedReplica != nil {
		// Success!
		recoverDeadPrimarySuccessCounter.Inc(1)
		AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadPrimary: successfully promoted %+v", vtorcReparent.promotedReplica.Key))
		AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("- RecoverDeadPrimary: promoted server coordinates: %+v", vtorcReparent.promotedReplica.SelfBinlogCoordinates))

		kvPairs := inst.GetClusterPrimaryKVPairs(vtorcReparent.analysisEntry.ClusterDetails.ClusterAlias, &vtorcReparent.promotedReplica.Key)
		AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("Writing KV %+v", kvPairs))
		for _, kvPair := range kvPairs {
			err := kv.PutKVPair(kvPair)
			log.Errore(err)
		}
		{
			AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("Distributing KV %+v", kvPairs))
			err := kv.DistributePairs(kvPairs)
			log.Errore(err)
		}
		if config.Config.PrimaryFailoverDetachReplicaPrimaryHost {
			postponedFunction := func() error {
				AuditTopologyRecovery(vtorcReparent.topologyRecovery, "- RecoverDeadPrimary: detaching master host on promoted master")
				inst.DetachReplicaPrimaryHost(&vtorcReparent.promotedReplica.Key)
				return nil
			}
			vtorcReparent.topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("RecoverDeadPrimary, detaching promoted master host %+v", vtorcReparent.promotedReplica.Key))
		}
		func() error {
			before := vtorcReparent.analysisEntry.AnalyzedInstanceKey.StringCode()
			after := vtorcReparent.promotedReplica.Key.StringCode()
			AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("- RecoverDeadPrimary: updating cluster_alias: %v -> %v", before, after))
			//~~~inst.ReplaceClusterName(before, after)
			if alias := vtorcReparent.analysisEntry.ClusterDetails.ClusterAlias; alias != "" {
				inst.SetClusterAlias(vtorcReparent.promotedReplica.Key.StringCode(), alias)
			} else {
				inst.ReplaceAliasClusterName(before, after)
			}
			return nil
		}()

		attributes.SetGeneralAttribute(vtorcReparent.analysisEntry.ClusterDetails.ClusterDomain, vtorcReparent.promotedReplica.Key.StringCode())

		if !vtorcReparent.skipProcesses {
			// Execute post master-failover processes
			executeProcesses(config.Config.PostPrimaryFailoverProcesses, "PostPrimaryFailoverProcesses", vtorcReparent.topologyRecovery, false)
		}
	} else {
		recoverDeadPrimaryFailureCounter.Inc(1)
	}
}
