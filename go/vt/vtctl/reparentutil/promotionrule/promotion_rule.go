/*
   Copyright 2014 Outbrain Inc.

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

package promotionrule

import (
	"errors"
	"fmt"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// CandidatePromotionRule describe the promotion preference/rule for an instance.
type CandidatePromotionRule string

const (
	Must      CandidatePromotionRule = "must"
	Prefer    CandidatePromotionRule = "prefer"
	Neutral   CandidatePromotionRule = "neutral"
	PreferNot CandidatePromotionRule = "prefer_not"
	MustNot   CandidatePromotionRule = "must_not"
)

var ErrUnsupportedPromotionRule = errors.New("unsupported promotion rule")

var promotionRuleOrderMap = map[CandidatePromotionRule]int{
	Must:      0,
	Prefer:    1,
	Neutral:   2,
	PreferNot: 3,
	MustNot:   4,
}

// AllPromotionRules returns all the CandidatePromotionRules in a list
// sorted by their priority.
func AllPromotionRules() []CandidatePromotionRule {
	return []CandidatePromotionRule{Must, Prefer, Neutral, PreferNot, MustNot}
}

func (this *CandidatePromotionRule) BetterThan(other CandidatePromotionRule) bool {
	otherOrder, ok := promotionRuleOrderMap[other]
	if !ok {
		return false
	}
	return promotionRuleOrderMap[*this] < otherOrder
}

// Parse returns a CandidatePromotionRule by name.
// It returns an error if there is no known rule by the given name.
func Parse(ruleName string) (CandidatePromotionRule, error) {
	switch ruleName {
	case "prefer", "neutral", "prefer_not", "must_not":
		return CandidatePromotionRule(ruleName), nil
	case "must":
		return CandidatePromotionRule(""), fmt.Errorf("CandidatePromotionRule: %v not supported yet", ruleName)
	default:
		return CandidatePromotionRule(""), fmt.Errorf("Invalid CandidatePromotionRule: %v", ruleName)
	}
}

// ParseFromProto returns a *CandidatePromotionRule from a topodatapb.PromotionRule.
func ParseFromProto(promotionRule topodatapb.PromotionRule) (CandidatePromotionRule, error) {
	switch promotionRule {
	case topodatapb.PromotionRule_NEUTRAL, topodatapb.PromotionRule_NONE:
		return Neutral, nil
	case topodatapb.PromotionRule_MUST:
		return Must, fmt.Errorf("ParseFromProto: %v not supported yet", promotionRule)
	case topodatapb.PromotionRule_PREFER:
		return Prefer, nil
	case topodatapb.PromotionRule_PREFER_NOT:
		return PreferNot, nil
	case topodatapb.PromotionRule_MUST_NOT:
		return MustNot, nil
	default:
		return Neutral, ErrUnsupportedPromotionRule
	}
}
