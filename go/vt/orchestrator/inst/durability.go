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
	"fmt"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

//=======================================================================

var (
	durabilityPolicies  = make(map[string]durabler)
	curDurabilityPolicy durabler
)

func init() {
	registerDurability("none", &durabilityNone{})
	registerDurability("semi_sync", &durabilitySemiSync{})
	registerDurability("cross_cell", &durabilityCrossCell{})
}

type durabler interface {
	promotionRule(*topodatapb.Tablet) CandidatePromotionRule
	masterSemiSync(InstanceKey) int
	replicaSemiSync(master, replica *topodatapb.Tablet) bool
}

func registerDurability(name string, d durabler) {
	if durabilityPolicies[name] != nil {
		log.Fatalf("durability policy %v already registered", name)
	}
	durabilityPolicies[name] = d
}

//=======================================================================

func SetDurabilityPolicy(name string) error {
	curDurabilityPolicy = durabilityPolicies[name]
	if curDurabilityPolicy == nil {
		return fmt.Errorf("durability policy %v not found", name)
	}
	log.Infof("Durability setting: %v", name)
	return nil
}

// PromotionRule returns the promotion rule for the instance.
func PromotionRule(tablet *topodatapb.Tablet) CandidatePromotionRule {
	return curDurabilityPolicy.promotionRule(tablet)
}

// MasterSemiSync returns the master semi-sync setting for the instance.
// 0 means none. Non-zero specifies the number of required ackers.
func MasterSemiSync(instanceKey InstanceKey) int {
	return curDurabilityPolicy.masterSemiSync(instanceKey)
}

// ReplicaSemiSync returns the replica semi-sync setting for the instance.
func ReplicaSemiSync(masterKey, replicaKey InstanceKey) bool {
	master, err := ReadTablet(masterKey)
	if err != nil {
		return false
	}
	replica, err := ReadTablet(replicaKey)
	if err != nil {
		return false
	}
	return curDurabilityPolicy.replicaSemiSync(master, replica)
}

// ReplicaSemiSyncFromTablet returns the replica semi-sync setting from the tablet record.
// Prefer using this function if tablet record is available.
func ReplicaSemiSyncFromTablet(master, replica *topodatapb.Tablet) bool {
	return curDurabilityPolicy.replicaSemiSync(master, replica)
}

//=======================================================================

type durabilityNone struct{}

func (d *durabilityNone) promotionRule(tablet *topodatapb.Tablet) CandidatePromotionRule {
	switch tablet.Type {
	case topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA:
		return NeutralPromoteRule
	}
	return MustNotPromoteRule
}

func (d *durabilityNone) masterSemiSync(instanceKey InstanceKey) int {
	return 0
}

func (d *durabilityNone) replicaSemiSync(master, replica *topodatapb.Tablet) bool {
	return false
}

//=======================================================================

type durabilitySemiSync struct{}

func (d *durabilitySemiSync) promotionRule(tablet *topodatapb.Tablet) CandidatePromotionRule {
	switch tablet.Type {
	case topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA:
		return NeutralPromoteRule
	}
	return MustNotPromoteRule
}

func (d *durabilitySemiSync) masterSemiSync(instanceKey InstanceKey) int {
	return 1
}

func (d *durabilitySemiSync) replicaSemiSync(master, replica *topodatapb.Tablet) bool {
	switch replica.Type {
	case topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA:
		return true
	}
	return false
}

//=======================================================================

type durabilityCrossCell struct{}

func (d *durabilityCrossCell) promotionRule(tablet *topodatapb.Tablet) CandidatePromotionRule {
	switch tablet.Type {
	case topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA:
		return NeutralPromoteRule
	}
	return MustNotPromoteRule
}

func (d *durabilityCrossCell) masterSemiSync(instanceKey InstanceKey) int {
	return 1
}

func (d *durabilityCrossCell) replicaSemiSync(master, replica *topodatapb.Tablet) bool {
	// Prevent panics.
	if master.Alias == nil || replica.Alias == nil {
		return false
	}
	switch replica.Type {
	case topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA:
		return master.Alias.Cell != replica.Alias.Cell
	}
	return false
}
