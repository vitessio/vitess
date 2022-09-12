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
	"fmt"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/orchestrator/inst"
)

// AsyncRequest represents an entry in the async_request table
type CommandApplier struct {
}

func NewCommandApplier() *CommandApplier {
	applier := &CommandApplier{}
	return applier
}

func (applier *CommandApplier) ApplyCommand(op string, value []byte) any {
	switch op {
	case "heartbeat":
		return nil
	case "register-node":
		return applier.registerNode(value)
	case "discover":
		return applier.discover(value)
	case "injected-pseudo-gtid":
		return nil // depracated
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
	case "put-instance-tag":
		return applier.putInstanceTag(value)
	case "delete-instance-tag":
		return applier.deleteInstanceTag(value)
	case "set-cluster-alias-manual-override":
		return applier.noop(value)
	}
	errMsg := fmt.Sprintf("Unknown command op: %s", op)
	log.Errorf(errMsg)
	return fmt.Errorf(errMsg)
}

func (applier *CommandApplier) registerNode(value []byte) any {
	return nil
}

func (applier *CommandApplier) discover(value []byte) any {
	instanceKey := inst.InstanceKey{}
	if err := json.Unmarshal(value, &instanceKey); err != nil {
		log.Error(err)
		return err
	}
	DiscoverInstance(instanceKey, false /* forceDiscovery */)
	return nil
}

func (applier *CommandApplier) forget(value []byte) any {
	instanceKey := inst.InstanceKey{}
	if err := json.Unmarshal(value, &instanceKey); err != nil {
		log.Error(err)
		return err
	}
	err := inst.ForgetInstance(&instanceKey)
	return err
}

func (applier *CommandApplier) forgetCluster(value []byte) any {
	var clusterName string
	if err := json.Unmarshal(value, &clusterName); err != nil {
		log.Error(err)
		return err
	}
	err := inst.ForgetCluster(clusterName)
	return err
}

func (applier *CommandApplier) beginDowntime(value []byte) any {
	downtime := inst.Downtime{}
	if err := json.Unmarshal(value, &downtime); err != nil {
		log.Error(err)
		return err
	}
	err := inst.BeginDowntime(&downtime)
	return err
}

func (applier *CommandApplier) endDowntime(value []byte) any {
	instanceKey := inst.InstanceKey{}
	if err := json.Unmarshal(value, &instanceKey); err != nil {
		log.Error(err)
		return err
	}
	_, err := inst.EndDowntime(&instanceKey)
	return err
}

func (applier *CommandApplier) registerCandidate(value []byte) any {
	candidate := inst.CandidateDatabaseInstance{}
	if err := json.Unmarshal(value, &candidate); err != nil {
		log.Error(err)
		return err
	}
	err := inst.RegisterCandidateInstance(&candidate)
	return err
}

func (applier *CommandApplier) ackRecovery(value []byte) any {
	ack := RecoveryAcknowledgement{}
	err := json.Unmarshal(value, &ack)
	if err != nil {
		log.Error(err)
		return err
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
	if ack.ID > 0 {
		_, err = AcknowledgeRecovery(ack.ID, ack.Owner, ack.Comment)
	}
	if ack.UID != "" {
		_, err = AcknowledgeRecoveryByUID(ack.UID, ack.Owner, ack.Comment)
	}
	return err
}

func (applier *CommandApplier) registerHostnameUnresolve(value []byte) any {
	registration := inst.HostnameRegistration{}
	if err := json.Unmarshal(value, &registration); err != nil {
		log.Error(err)
		return err
	}
	err := inst.RegisterHostnameUnresolve(&registration)
	return err
}

func (applier *CommandApplier) submitPoolInstances(value []byte) any {
	submission := inst.PoolInstancesSubmission{}
	if err := json.Unmarshal(value, &submission); err != nil {
		log.Error(err)
		return err
	}
	err := inst.ApplyPoolInstances(&submission)
	return err
}

func (applier *CommandApplier) registerFailureDetection(value []byte) any {
	analysisEntry := inst.ReplicationAnalysis{}
	if err := json.Unmarshal(value, &analysisEntry); err != nil {
		log.Error(err)
		return err
	}
	_, err := AttemptFailureDetectionRegistration(&analysisEntry)
	return err
}

func (applier *CommandApplier) writeRecovery(value []byte) any {
	topologyRecovery := TopologyRecovery{}
	if err := json.Unmarshal(value, &topologyRecovery); err != nil {
		log.Error(err)
		return err
	}
	if _, err := writeTopologyRecovery(&topologyRecovery); err != nil {
		return err
	}
	return nil
}

func (applier *CommandApplier) writeRecoveryStep(value []byte) any {
	topologyRecoveryStep := TopologyRecoveryStep{}
	if err := json.Unmarshal(value, &topologyRecoveryStep); err != nil {
		log.Error(err)
		return err
	}
	err := writeTopologyRecoveryStep(&topologyRecoveryStep)
	return err
}

func (applier *CommandApplier) resolveRecovery(value []byte) any {
	topologyRecovery := TopologyRecovery{}
	if err := json.Unmarshal(value, &topologyRecovery); err != nil {
		log.Error(err)
		return err
	}
	if err := writeResolveRecovery(&topologyRecovery); err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (applier *CommandApplier) disableGlobalRecoveries(value []byte) any {
	err := DisableRecovery()
	return err
}

func (applier *CommandApplier) enableGlobalRecoveries(value []byte) any {
	err := EnableRecovery()
	return err
}

func (applier *CommandApplier) putInstanceTag(value []byte) any {
	instanceTag := inst.InstanceTag{}
	if err := json.Unmarshal(value, &instanceTag); err != nil {
		log.Error(err)
		return err
	}
	err := inst.PutInstanceTag(&instanceTag.Key, &instanceTag.T)
	return err
}

func (applier *CommandApplier) deleteInstanceTag(value []byte) any {
	instanceTag := inst.InstanceTag{}
	if err := json.Unmarshal(value, &instanceTag); err != nil {
		log.Error(err)
		return err
	}
	_, err := inst.Untag(&instanceTag.Key, &instanceTag.T)
	return err
}

func (applier *CommandApplier) noop(value []byte) any {
	return nil
}
