/*
Copyright 2020 The Vitess Authors.

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

package inst

import (
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
)

// IsReplicaSemiSync returns the replica semi-sync setting for the instance.
func IsReplicaSemiSync[V InstanceKey | *topodatapb.Tablet](durabilityPolicy reparentutil.Durabler, primaryInstance V, replicaInstance V) bool {
	primary, err := getTablet(primaryInstance)
	if err != nil {
		return false
	}
	replica, err := getTablet(replicaInstance)
	if err != nil {
		return false
	}
	return reparentutil.IsReplicaSemiSync(durabilityPolicy, primary, replica)
}

// SemiSyncAckers returns the primary semi-sync setting for the instance.
// 0 means none. Non-zero specifies the number of required ackers.
func SemiSyncAckers[V InstanceKey | *topodatapb.Tablet](durabilityPolicy reparentutil.Durabler, instance V) int {
	primary, err := getTablet(instance)
	if err != nil {
		return 0
	}
	return reparentutil.SemiSyncAckers(durabilityPolicy, primary)
}

// PromotionRule returns the promotion rule for the instance.
func PromotionRule[V InstanceKey | *topodatapb.Tablet](durabilityPolicy reparentutil.Durabler, instance V) promotionrule.CandidatePromotionRule {
	tablet, err := getTablet(instance)
	if err != nil {
		return promotionrule.MustNot
	}
	return reparentutil.PromotionRule(durabilityPolicy, tablet)
}

func getTablet[V InstanceKey | *topodatapb.Tablet](instance V) (*topodatapb.Tablet, error) {
	var instanceTablet *topodatapb.Tablet
	var err error
	switch node := any(instance).(type) {
	case InstanceKey:
		instanceTablet, err = ReadTablet(node)
		if err != nil {
			return nil, err
		}
	case *topodatapb.Tablet:
		instanceTablet = node
	}
	return instanceTablet, nil
}

// GetDurabilityPolicy gets the durability policy for the keyspace of the given instance
func GetDurabilityPolicy[V InstanceKey | *topodatapb.Tablet](instance V) (reparentutil.Durabler, error) {
	tablet, err := getTablet(instance)
	if err != nil {
		return nil, err
	}
	ki, err := ReadKeyspace(tablet.Keyspace)
	if err != nil {
		return nil, err
	}
	return reparentutil.GetDurabilityPolicy(ki.DurabilityPolicy)
}
