/*
   Copyright 2016 Simon J Mudd

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

	"vitess.io/vitess/go/vt/orchestrator/db"
)

// CandidateDatabaseInstance contains information about explicit promotion rules for an instance
type CandidateDatabaseInstance struct {
	Hostname            string
	Port                int
	PromotionRule       CandidatePromotionRule
	LastSuggestedString string
	PromotionRuleExpiry string // generated when retrieved from database for consistency reasons
}

func NewCandidateDatabaseInstance(instanceKey *InstanceKey, promotionRule CandidatePromotionRule) *CandidateDatabaseInstance {
	return &CandidateDatabaseInstance{
		Hostname:      instanceKey.Hostname,
		Port:          instanceKey.Port,
		PromotionRule: promotionRule,
	}
}

func (cdi *CandidateDatabaseInstance) WithCurrentTime() *CandidateDatabaseInstance {
	cdi.LastSuggestedString, _ = db.ReadTimeNow()
	return cdi
}

// String returns a string representation of the CandidateDatabaseInstance struct
func (cdi *CandidateDatabaseInstance) String() string {
	return fmt.Sprintf("%s:%d %s", cdi.Hostname, cdi.Port, cdi.PromotionRule)
}

// Key returns an instance key representing this candidate
func (cdi *CandidateDatabaseInstance) Key() *InstanceKey {
	return &InstanceKey{Hostname: cdi.Hostname, Port: cdi.Port}
}
