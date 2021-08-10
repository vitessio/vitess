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

	"vitess.io/vitess/go/vt/topo/topoproto"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

//=======================================================================

// A newDurabler is a function that creates a new durabler based on the
// properties specified in the input map. Every durabler must
// register a newDurabler function.
type newDurabler func(map[string]string) durabler

var (
	durabilityPolicies  = make(map[string]newDurabler)
	curDurabilityPolicy durabler
)

func init() {
	registerDurability("none", func(map[string]string) durabler {
		return &durabilityNone{}
	})
	registerDurability("semi_sync", func(map[string]string) durabler {
		return &durabilitySemiSync{}
	})
	registerDurability("cross_cell", func(map[string]string) durabler {
		return &durabilityCrossCell{}
	})
	registerDurability("specified", newDurabilitySpecified)
}

type durabler interface {
	promotionRule(*topodatapb.Tablet) CandidatePromotionRule
	masterSemiSync(InstanceKey) int
	replicaSemiSync(master, replica *topodatapb.Tablet) bool
}

func registerDurability(name string, newDurablerFunc newDurabler) {
	if durabilityPolicies[name] != nil {
		log.Fatalf("durability policy %v already registered", name)
	}
	durabilityPolicies[name] = newDurablerFunc
}

//=======================================================================

// SetDurabilityPolicy is used to set the durability policy from the registered policies
func SetDurabilityPolicy(name string, durabilityParams map[string]string) error {
	log.Error("received map", durabilityParams)
	newDurabilityCreationFunc, found := durabilityPolicies[name]
	if !found {
		return fmt.Errorf("durability policy %v not found", name)
	}
	log.Infof("Durability setting: %v", name)
	curDurabilityPolicy = newDurabilityCreationFunc(durabilityParams)
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
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA:
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
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA:
		return NeutralPromoteRule
	}
	return MustNotPromoteRule
}

func (d *durabilitySemiSync) masterSemiSync(instanceKey InstanceKey) int {
	return 1
}

func (d *durabilitySemiSync) replicaSemiSync(master, replica *topodatapb.Tablet) bool {
	switch replica.Type {
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA:
		return true
	}
	return false
}

//=======================================================================

type durabilityCrossCell struct{}

func (d *durabilityCrossCell) promotionRule(tablet *topodatapb.Tablet) CandidatePromotionRule {
	switch tablet.Type {
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA:
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
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA:
		return master.Alias.Cell != replica.Alias.Cell
	}
	return false
}

//=======================================================================

type durabilitySpecified struct {
	promotionRules map[string]CandidatePromotionRule
}

func (d *durabilitySpecified) promotionRule(tablet *topodatapb.Tablet) CandidatePromotionRule {
	promoteRule, isFound := d.promotionRules[topoproto.TabletAliasString(tablet.Alias)]
	if isFound {
		return promoteRule
	}

	switch tablet.Type {
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA:
		return NeutralPromoteRule
	}
	return MustNotPromoteRule
}

func (d *durabilitySpecified) masterSemiSync(instanceKey InstanceKey) int {
	return 0
}

func (d *durabilitySpecified) replicaSemiSync(master, replica *topodatapb.Tablet) bool {
	return false
}

func newDurabilitySpecified(m map[string]string) durabler {
	promotionRules := map[string]CandidatePromotionRule{}
	for tabletAliasStr, promotionRuleStr := range m {
		promotionRule, err := ParseCandidatePromotionRule(promotionRuleStr)
		if err != nil {
			log.Errorf("invalid promotion rule %s found, received error - %v", promotionRuleStr, err)
			continue
		}
		promotionRules[tabletAliasStr] = promotionRule
	}

	return &durabilitySpecified{
		promotionRules: promotionRules,
	}
}
