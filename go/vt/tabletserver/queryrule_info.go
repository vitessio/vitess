// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"errors"
	"sync"

	"github.com/youtube/vitess/go/vt/tabletserver/planbuilder"
)

var QueryRuleSources *QueryRuleInfo = NewQueryRuleInfo()

// QueryRuleInfo is the maintainer of QueryRules from multiple sources
type QueryRuleInfo struct {
	// mutex to protect following queryRulesMap
	mu sync.Mutex
	// queryRulesMap maps the names of different query rule sources to the actual QueryRules structure
	queryRulesMap map[string]*QueryRules
}

func NewQueryRuleInfo() *QueryRuleInfo {
	qri := &QueryRuleInfo{
		queryRulesMap: map[string]*QueryRules{},
	}
	return qri
}

// RegisterQueryRuleSource registers a query rule source name with QueryRuleInfo
func (qri *QueryRuleInfo) RegisterQueryRuleSource(ruleSource string) error {
	qri.mu.Lock()
	defer qri.mu.Unlock()
	if _, existed := qri.queryRulesMap[ruleSource]; existed {
		return errors.New("Query rule source " + ruleSource + " has been registered")
	}
	qri.queryRulesMap[ruleSource] = NewQueryRules()
	return nil
}

// SetRules takes an external QueryRules structure and overwrite one of the
// internal QueryRules as designated by ruleSource parameter
func (qri *QueryRuleInfo) SetRules(ruleSource string, newRules *QueryRules) error {
	if newRules == nil {
		newRules = NewQueryRules()
	}
	qri.mu.Lock()
	defer qri.mu.Unlock()
	if _, ok := qri.queryRulesMap[ruleSource]; ok {
		qri.queryRulesMap[ruleSource] = newRules.Copy()
		return nil
	}
	return errors.New("Rule source identifier " + ruleSource + " is not valid")
}

// GetRules returns the corresponding QueryRules as designated by ruleSource parameter
func (qri *QueryRuleInfo) GetRules(ruleSource string) (error, *QueryRules) {
	qri.mu.Lock()
	defer qri.mu.Unlock()
	if ruleset, ok := qri.queryRulesMap[ruleSource]; ok {
		return nil, ruleset.Copy()
	}
	return errors.New("Rule source identifier " + ruleSource + " is not valid"), NewQueryRules()
}

// filterByPlan creates a new QueryRules by prefiltering on all query rules that are contained in internal
// QueryRules structures, in other words, query rules from all predefined sources will be applied
func (qri *QueryRuleInfo) filterByPlan(query string, planid planbuilder.PlanType, tableName string) (newqrs *QueryRules) {
	qri.mu.Lock()
	defer qri.mu.Unlock()
	newqrs = NewQueryRules()
	for _, rules := range qri.queryRulesMap {
		newqrs.Append(rules.filterByPlan(query, planid, tableName))
	}
	return newqrs
}
