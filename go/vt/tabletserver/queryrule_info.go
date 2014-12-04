// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"errors"
	"sync"

	"github.com/youtube/vitess/go/vt/tabletserver/planbuilder"
)

// QueryRuleInfo is the maintainer of QueryRules from multiple sources
type QueryRuleInfo struct {
	mu            sync.Mutex
	queryRulesMap map[string]*QueryRules
}

// Names for QueryRules coming from different sources
// QueryRules from keyrange
const KeyrangeQueryRules string = "KEYRANGE_QUERY_RULES"

// QueryRules from blacklist
const BlacklistQueryRules string = "BLACKLIST_QUERY_RULES"

// QueryRules from custom rules
const CustomQueryRules string = "CUSTOM_QUERY_RULES"

func NewQueryRuleInfo() *QueryRuleInfo {
	qri := &QueryRuleInfo{
		queryRulesMap: map[string]*QueryRules{
			KeyrangeQueryRules:  NewQueryRules(),
			BlacklistQueryRules: NewQueryRules(),
			CustomQueryRules:    NewQueryRules(),
		},
	}
	return qri
}

// SetRules takes a external QueryRules structure and overwrite one of the
// internal QueryRules as designated by queryRuleSet parameter
func (qri *QueryRuleInfo) SetRules(queryRuleSet string, newRules *QueryRules) error {
	qri.mu.Lock()
	defer qri.mu.Unlock()
	if _, ok := qri.queryRulesMap[queryRuleSet]; ok {
		qri.queryRulesMap[queryRuleSet] = newRules.Copy()
		return nil
	} else {
		return errors.New("QueryRules identifier " + queryRuleSet + " is not valid")
	}
}

// GetRules returns the corresponding QueryRules as designated by queryRuleSet parameter
func (qri *QueryRuleInfo) GetRules(queryRuleSet string) (error, *QueryRules) {
	qri.mu.Lock()
	defer qri.mu.Unlock()
	if ruleset, ok := qri.queryRulesMap[queryRuleSet]; ok {
		return nil, ruleset.Copy()
	} else {
		return errors.New("QueryRules identifier " + queryRuleSet + " is not valid"), nil
	}
}

// filterByPlan creates a new QueryRules by prefiltering on all query rules that are contained in internal
// QueryRules structures, in other words, query rules from all predefined sources will be applied
func (qri *QueryRuleInfo) filterByPlan(query string, planid planbuilder.PlanType, tableName string) (newqrs *QueryRules) {
	newqrs = NewQueryRules()
	newqrs.Append(qri.queryRulesMap[KeyrangeQueryRules].filterByPlan(query, planid, tableName))
	newqrs.Append(qri.queryRulesMap[BlacklistQueryRules].filterByPlan(query, planid, tableName))
	newqrs.Append(qri.queryRulesMap[CustomQueryRules].filterByPlan(query, planid, tableName))
	return newqrs
}
