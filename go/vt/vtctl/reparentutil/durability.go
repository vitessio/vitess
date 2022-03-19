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
	"sync"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
)

//=======================================================================

// A newDurabler is a function that creates a new durabler based on the
// properties specified in the input map. Every durabler must
// register a newDurabler function.
type newDurabler func() durabler

var (
	// durabilityPolicies is a map that stores the functions needed to create a new durabler
	durabilityPolicies = make(map[string]newDurabler)
	// curDurabilityPolicy is the current durability policy in use
	curDurabilityPolicy durabler
	// curDurabilityPolicyMutex is the mutex protecting the curDurabilityPolicy variable
	curDurabilityPolicyMutex sync.Mutex
)

func init() {
	// register all the durability rules with their functions to create them
	registerDurability("none", func() durabler {
		return &durabilityNone{}
	})
	registerDurability("semi_sync", func() durabler {
		return &durabilitySemiSync{}
	})
	registerDurability("cross_cell", func() durabler {
		return &durabilityCrossCell{}
	})
	registerDurability("test", func() durabler {
		return &durabilityTest{}
	})
}

// durabler is the interface which is used to get the promotion rules for candidates and the semi sync setup
type durabler interface {
	// promotionRule represents the precedence in which we want to tablets to be promoted.
	// The higher the promotion rule of a tablet, the more we want it to be promoted in case of a failover
	promotionRule(*topodatapb.Tablet) promotionrule.CandidatePromotionRule
	// semiSyncAckers represents the number of semi-sync ackers required for a given tablet if it were to become the PRIMARY instance
	semiSyncAckers(*topodatapb.Tablet) int
	// isReplicaSemiSync returns whether the "replica" should send semi-sync acks if "primary" were to become the PRIMARY instance
	isReplicaSemiSync(primary, replica *topodatapb.Tablet) bool
}

func registerDurability(name string, newDurablerFunc newDurabler) {
	if durabilityPolicies[name] != nil {
		log.Fatalf("durability policy %v already registered", name)
	}
	durabilityPolicies[name] = newDurablerFunc
}

//=======================================================================

// SetDurabilityPolicy is used to set the durability policy from the registered policies
func SetDurabilityPolicy(name string) error {
	newDurabilityCreationFunc, found := durabilityPolicies[name]
	if !found {
		return fmt.Errorf("durability policy %v not found", name)
	}
	log.Infof("Setting durability policy to %v", name)
	curDurabilityPolicyMutex.Lock()
	defer curDurabilityPolicyMutex.Unlock()
	curDurabilityPolicy = newDurabilityCreationFunc()
	return nil
}

// PromotionRule returns the promotion rule for the instance.
func PromotionRule(tablet *topodatapb.Tablet) promotionrule.CandidatePromotionRule {
	// Prevent panics.
	if tablet == nil || tablet.Alias == nil {
		return promotionrule.MustNot
	}
	curDurabilityPolicyMutex.Lock()
	defer curDurabilityPolicyMutex.Unlock()
	return curDurabilityPolicy.promotionRule(tablet)
}

// SemiSyncAckers returns the primary semi-sync setting for the instance.
// 0 means none. Non-zero specifies the number of required ackers.
func SemiSyncAckers(tablet *topodatapb.Tablet) int {
	curDurabilityPolicyMutex.Lock()
	defer curDurabilityPolicyMutex.Unlock()
	return curDurabilityPolicy.semiSyncAckers(tablet)
}

// IsReplicaSemiSync returns the replica semi-sync setting from the tablet record.
// Prefer using this function if tablet record is available.
func IsReplicaSemiSync(primary, replica *topodatapb.Tablet) bool {
	// Prevent panics.
	if primary == nil || primary.Alias == nil || replica == nil || replica.Alias == nil {
		return false
	}
	curDurabilityPolicyMutex.Lock()
	defer curDurabilityPolicyMutex.Unlock()
	return curDurabilityPolicy.isReplicaSemiSync(primary, replica)
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

func (d *durabilityNone) semiSyncAckers(tablet *topodatapb.Tablet) int {
	return 0
}

func (d *durabilityNone) isReplicaSemiSync(primary, replica *topodatapb.Tablet) bool {
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

func (d *durabilitySemiSync) semiSyncAckers(tablet *topodatapb.Tablet) int {
	return 1
}

func (d *durabilitySemiSync) isReplicaSemiSync(primary, replica *topodatapb.Tablet) bool {
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

func (d *durabilityCrossCell) semiSyncAckers(tablet *topodatapb.Tablet) int {
	return 1
}

func (d *durabilityCrossCell) isReplicaSemiSync(primary, replica *topodatapb.Tablet) bool {
	switch replica.Type {
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA:
		return primary.Alias.Cell != replica.Alias.Cell
	}
	return false
}

//=======================================================================

// durabilityTest is like durabilityNone. It overrides the type for a specific tablet to prefer. It is only meant to be used for testing purposes!
type durabilityTest struct{}

func (d *durabilityTest) promotionRule(tablet *topodatapb.Tablet) promotionrule.CandidatePromotionRule {
	if topoproto.TabletAliasString(tablet.Alias) == "zone2-0000000200" {
		return promotionrule.Prefer
	}

	switch tablet.Type {
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA:
		return promotionrule.Neutral
	}
	return promotionrule.MustNot
}

func (d *durabilityTest) semiSyncAckers(tablet *topodatapb.Tablet) int {
	return 0
}

func (d *durabilityTest) isReplicaSemiSync(primary, replica *topodatapb.Tablet) bool {
	return false
}
