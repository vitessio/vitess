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

package reparentutil

import (
	"fmt"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
)

//=======================================================================

// A newDurabler is a function that creates a new durabler based on the
// properties specified in the input map. Every durabler must
// register a newDurabler function.
type newDurabler func(map[string]string) durabler

var (
	// durabilityPolicies is a map that stores the functions needed to create a new durabler
	durabilityPolicies = make(map[string]newDurabler)
	// curDurabilityPolicy is the current durability policy in use
	curDurabilityPolicy durabler
)

func init() {
	// register all the durability rules with their functions to create them
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

// durabler is the interface which is used to get the promotion rules for candidates and the semi sync setup
type durabler interface {
	promotionRule(*topodatapb.Tablet) promotionrule.CandidatePromotionRule
	primarySemiSync(*topodatapb.Tablet) int
	replicaSemiSync(primary, replica *topodatapb.Tablet) bool
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
	newDurabilityCreationFunc, found := durabilityPolicies[name]
	if !found {
		return fmt.Errorf("durability policy %v not found", name)
	}
	log.Infof("Durability setting: %v", name)
	curDurabilityPolicy = newDurabilityCreationFunc(durabilityParams)
	return nil
}

// PromotionRule returns the promotion rule for the instance.
func PromotionRule(tablet *topodatapb.Tablet) promotionrule.CandidatePromotionRule {
	return curDurabilityPolicy.promotionRule(tablet)
}

// PrimarySemiSyncFromTablet returns the primary semi-sync setting for the instance.
// 0 means none. Non-zero specifies the number of required ackers.
func PrimarySemiSyncFromTablet(tablet *topodatapb.Tablet) int {
	return curDurabilityPolicy.primarySemiSync(tablet)
}

// ReplicaSemiSyncFromTablet returns the replica semi-sync setting from the tablet record.
// Prefer using this function if tablet record is available.
func ReplicaSemiSyncFromTablet(primary, replica *topodatapb.Tablet) bool {
	return curDurabilityPolicy.replicaSemiSync(primary, replica)
}

//=======================================================================

// durabilityNone has no semi-sync and returns NeutralPromoteRule for Primary and Replica tablet types, MustNotPromoteRule for everything else
type durabilityNone struct{}

func (d *durabilityNone) promotionRule(tablet *topodatapb.Tablet) promotionrule.CandidatePromotionRule {
	switch tablet.Type {
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA:
		return promotionrule.Neutral
	}
	return promotionrule.MustNot
}

func (d *durabilityNone) primarySemiSync(tablet *topodatapb.Tablet) int {
	return 0
}

func (d *durabilityNone) replicaSemiSync(primary, replica *topodatapb.Tablet) bool {
	return false
}

//=======================================================================

// durabilitySemiSync has 1 semi-sync setup. It only allows Primary and Replica type servers to acknowledge semi sync
// It returns NeutralPromoteRule for Primary and Replica tablet types, MustNotPromoteRule for everything else
type durabilitySemiSync struct{}

func (d *durabilitySemiSync) promotionRule(tablet *topodatapb.Tablet) promotionrule.CandidatePromotionRule {
	switch tablet.Type {
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA:
		return promotionrule.Neutral
	}
	return promotionrule.MustNot
}

func (d *durabilitySemiSync) primarySemiSync(tablet *topodatapb.Tablet) int {
	return 1
}

func (d *durabilitySemiSync) replicaSemiSync(primary, replica *topodatapb.Tablet) bool {
	switch replica.Type {
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA:
		return true
	}
	return false
}

//=======================================================================

// durabilityCrossCell has 1 semi-sync setup. It only allows Primary and Replica type servers from a different cell to acknowledge semi sync.
// This means that a transaction must be in two cells for it to be acknowledged
// It returns NeutralPromoteRule for Primary and Replica tablet types, MustNotPromoteRule for everything else
type durabilityCrossCell struct{}

func (d *durabilityCrossCell) promotionRule(tablet *topodatapb.Tablet) promotionrule.CandidatePromotionRule {
	switch tablet.Type {
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA:
		return promotionrule.Neutral
	}
	return promotionrule.MustNot
}

func (d *durabilityCrossCell) primarySemiSync(tablet *topodatapb.Tablet) int {
	return 1
}

func (d *durabilityCrossCell) replicaSemiSync(primary, replica *topodatapb.Tablet) bool {
	// Prevent panics.
	if primary.Alias == nil || replica.Alias == nil {
		return false
	}
	switch replica.Type {
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA:
		return primary.Alias.Cell != replica.Alias.Cell
	}
	return false
}

//=======================================================================

// durabilitySpecified is like durabilityNone. It has an additional map which it first queries with the tablet alias as the key
// If a CandidatePromotionRule is found in that map, then that is used as the promotion rule. Otherwise, it reverts to the same logic as durabilityNone
type durabilitySpecified struct {
	promotionRules map[string]promotionrule.CandidatePromotionRule
}

func (d *durabilitySpecified) promotionRule(tablet *topodatapb.Tablet) promotionrule.CandidatePromotionRule {
	promoteRule, isFound := d.promotionRules[topoproto.TabletAliasString(tablet.Alias)]
	if isFound {
		return promoteRule
	}

	switch tablet.Type {
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA:
		return promotionrule.Neutral
	}
	return promotionrule.MustNot
}

func (d *durabilitySpecified) primarySemiSync(tablet *topodatapb.Tablet) int {
	return 0
}

func (d *durabilitySpecified) replicaSemiSync(primary, replica *topodatapb.Tablet) bool {
	return false
}

// newDurabilitySpecified is a function that is used to create a new durabilitySpecified struct
func newDurabilitySpecified(m map[string]string) durabler {
	promotionRules := map[string]promotionrule.CandidatePromotionRule{}
	// range over the map given by the user
	for tabletAliasStr, promotionRuleStr := range m {
		// parse the promotion rule
		promotionRule, err := promotionrule.Parse(promotionRuleStr)
		// if parsing is not successful, skip over this rule
		if err != nil {
			log.Errorf("invalid promotion rule %s found, received error - %v", promotionRuleStr, err)
			continue
		}
		// set the promotion rule in the map at the given tablet alias
		promotionRules[tabletAliasStr] = promotionRule
	}

	return &durabilitySpecified{
		promotionRules: promotionRules,
	}
}
