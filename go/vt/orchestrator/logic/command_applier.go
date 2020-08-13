/*
   Copyright 2017 Shlomi Noach, GitHub Inc.

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
	"encoding/json"

	"vitess.io/vitess/go/vt/orchestrator/inst"
	"vitess.io/vitess/go/vt/orchestrator/kv"
	orcraft "vitess.io/vitess/go/vt/orchestrator/raft"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
)

// AsyncRequest represents an entry in the async_request table
type CommandApplier struct {
}

func NewCommandApplier() *CommandApplier {
	applier := &CommandApplier{}
	return applier
}

func (applier *CommandApplier) ApplyCommand(op string, value []byte) interface{} {
	switch op {
	case "heartbeat":
		return nil
	case "async-snapshot":
		return applier.asyncSnapshot(value)
	case "register-node":
		return applier.registerNode(value)
	case "discover":
		return applier.discover(value)
	case "injected-pseudo-gtid":
		return applier.injectedPseudoGTID(value)
	case "forget":
		return applier.forget(value)
	case "forget-cluster":
		return applier.forgetCluster(value)
	case "begin-downtime":
		return applier.beginDowntime(value)
	case "end-downtime":
		return applier.endDowntime(value)
	case "register-candidate":
		return applier.registerCandidate(value)
	case "ack-recovery":
		return applier.ackRecovery(value)
	case "register-hostname-unresolve":
		return applier.registerHostnameUnresolve(value)
	case "submit-pool-instances":
		return applier.submitPoolInstances(value)
	case "register-failure-detection":
		return applier.registerFailureDetection(value)
	case "write-recovery":
		return applier.writeRecovery(value)
	case "write-recovery-step":
		return applier.writeRecoveryStep(value)
	case "resolve-recovery":
		return applier.resolveRecovery(value)
	case "disable-global-recoveries":
		return applier.disableGlobalRecoveries(value)
	case "enable-global-recoveries":
		return applier.enableGlobalRecoveries(value)
	case "put-key-value":
		return applier.putKeyValue(value)
	case "put-instance-tag":
		return applier.putInstanceTag(value)
	case "delete-instance-tag":
		return applier.deleteInstanceTag(value)
	case "leader-uri":
		return applier.leaderURI(value)
	case "request-health-report":
		return applier.healthReport(value)
	case "set-cluster-alias-manual-override":
		return applier.setClusterAliasManualOverride(value)
	}
	return log.Errorf("Unknown command op: %s", op)
}

func (applier *CommandApplier) asyncSnapshot(value []byte) interface{} {
	err := orcraft.AsyncSnapshot()
	return err
}

func (applier *CommandApplier) registerNode(value []byte) interface{} {
	return nil
}

func (applier *CommandApplier) discover(value []byte) interface{} {
	instanceKey := inst.InstanceKey{}
	if err := json.Unmarshal(value, &instanceKey); err != nil {
		return log.Errore(err)
	}
	DiscoverInstance(instanceKey)
	return nil
}

func (applier *CommandApplier) injectedPseudoGTID(value []byte) interface{} {
	var clusterName string
	if err := json.Unmarshal(value, &clusterName); err != nil {
		return log.Errore(err)
	}
	inst.RegisterInjectedPseudoGTID(clusterName)
	return nil
}

func (applier *CommandApplier) forget(value []byte) interface{} {
	instanceKey := inst.InstanceKey{}
	if err := json.Unmarshal(value, &instanceKey); err != nil {
		return log.Errore(err)
	}
	err := inst.ForgetInstance(&instanceKey)
	return err
}

func (applier *CommandApplier) forgetCluster(value []byte) interface{} {
	var clusterName string
	if err := json.Unmarshal(value, &clusterName); err != nil {
		return log.Errore(err)
	}
	err := inst.ForgetCluster(clusterName)
	return err
}

func (applier *CommandApplier) beginDowntime(value []byte) interface{} {
	downtime := inst.Downtime{}
	if err := json.Unmarshal(value, &downtime); err != nil {
		return log.Errore(err)
	}
	err := inst.BeginDowntime(&downtime)
	return err
}

func (applier *CommandApplier) endDowntime(value []byte) interface{} {
	instanceKey := inst.InstanceKey{}
	if err := json.Unmarshal(value, &instanceKey); err != nil {
		return log.Errore(err)
	}
	_, err := inst.EndDowntime(&instanceKey)
	return err
}

func (applier *CommandApplier) registerCandidate(value []byte) interface{} {
	candidate := inst.CandidateDatabaseInstance{}
	if err := json.Unmarshal(value, &candidate); err != nil {
		return log.Errore(err)
	}
	err := inst.RegisterCandidateInstance(&candidate)
	return err
}

func (applier *CommandApplier) ackRecovery(value []byte) interface{} {
	ack := RecoveryAcknowledgement{}
	err := json.Unmarshal(value, &ack)
	if err != nil {
		return log.Errore(err)
	}
	if ack.AllRecoveries {
		_, err = AcknowledgeAllRecoveries(ack.Owner, ack.Comment)
	}
	if ack.ClusterName != "" {
		_, err = AcknowledgeClusterRecoveries(ack.ClusterName, ack.Owner, ack.Comment)
	}
	if ack.Key.IsValid() {
		_, err = AcknowledgeInstanceRecoveries(&ack.Key, ack.Owner, ack.Comment)
	}
	if ack.Id > 0 {
		_, err = AcknowledgeRecovery(ack.Id, ack.Owner, ack.Comment)
	}
	if ack.UID != "" {
		_, err = AcknowledgeRecoveryByUID(ack.UID, ack.Owner, ack.Comment)
	}
	return err
}

func (applier *CommandApplier) registerHostnameUnresolve(value []byte) interface{} {
	registration := inst.HostnameRegistration{}
	if err := json.Unmarshal(value, &registration); err != nil {
		return log.Errore(err)
	}
	err := inst.RegisterHostnameUnresolve(&registration)
	return err
}

func (applier *CommandApplier) submitPoolInstances(value []byte) interface{} {
	submission := inst.PoolInstancesSubmission{}
	if err := json.Unmarshal(value, &submission); err != nil {
		return log.Errore(err)
	}
	err := inst.ApplyPoolInstances(&submission)
	return err
}

func (applier *CommandApplier) registerFailureDetection(value []byte) interface{} {
	analysisEntry := inst.ReplicationAnalysis{}
	if err := json.Unmarshal(value, &analysisEntry); err != nil {
		return log.Errore(err)
	}
	_, err := AttemptFailureDetectionRegistration(&analysisEntry)
	return err
}

func (applier *CommandApplier) writeRecovery(value []byte) interface{} {
	topologyRecovery := TopologyRecovery{}
	if err := json.Unmarshal(value, &topologyRecovery); err != nil {
		return log.Errore(err)
	}
	if _, err := writeTopologyRecovery(&topologyRecovery); err != nil {
		return err
	}
	return nil
}

func (applier *CommandApplier) writeRecoveryStep(value []byte) interface{} {
	topologyRecoveryStep := TopologyRecoveryStep{}
	if err := json.Unmarshal(value, &topologyRecoveryStep); err != nil {
		return log.Errore(err)
	}
	err := writeTopologyRecoveryStep(&topologyRecoveryStep)
	return err
}

func (applier *CommandApplier) resolveRecovery(value []byte) interface{} {
	topologyRecovery := TopologyRecovery{}
	if err := json.Unmarshal(value, &topologyRecovery); err != nil {
		return log.Errore(err)
	}
	if err := writeResolveRecovery(&topologyRecovery); err != nil {
		return log.Errore(err)
	}
	return nil
}

func (applier *CommandApplier) disableGlobalRecoveries(value []byte) interface{} {
	err := DisableRecovery()
	return err
}

func (applier *CommandApplier) enableGlobalRecoveries(value []byte) interface{} {
	err := EnableRecovery()
	return err
}

func (applier *CommandApplier) putKeyValue(value []byte) interface{} {
	kvPair := kv.KVPair{}
	if err := json.Unmarshal(value, &kvPair); err != nil {
		return log.Errore(err)
	}
	err := kv.PutKVPair(&kvPair)
	return err
}

func (applier *CommandApplier) putInstanceTag(value []byte) interface{} {
	instanceTag := inst.InstanceTag{}
	if err := json.Unmarshal(value, &instanceTag); err != nil {
		return log.Errore(err)
	}
	err := inst.PutInstanceTag(&instanceTag.Key, &instanceTag.T)
	return err
}

func (applier *CommandApplier) deleteInstanceTag(value []byte) interface{} {
	instanceTag := inst.InstanceTag{}
	if err := json.Unmarshal(value, &instanceTag); err != nil {
		return log.Errore(err)
	}
	_, err := inst.Untag(&instanceTag.Key, &instanceTag.T)
	return err
}

func (applier *CommandApplier) leaderURI(value []byte) interface{} {
	var uri string
	if err := json.Unmarshal(value, &uri); err != nil {
		return log.Errore(err)
	}
	orcraft.LeaderURI.Set(uri)
	return nil
}

func (applier *CommandApplier) healthReport(value []byte) interface{} {
	var authenticationToken string
	if err := json.Unmarshal(value, &authenticationToken); err != nil {
		return log.Errore(err)
	}
	orcraft.ReportToRaftLeader(authenticationToken)
	return nil
}

func (applier *CommandApplier) setClusterAliasManualOverride(value []byte) interface{} {
	var params [2]string
	if err := json.Unmarshal(value, &params); err != nil {
		return log.Errore(err)
	}
	clusterName, alias := params[0], params[1]
	err := inst.SetClusterAliasManualOverride(clusterName, alias)
	return err
}
