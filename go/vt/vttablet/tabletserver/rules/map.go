// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rules

import (
	"encoding/json"
	"errors"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/planbuilder"
)

// Map is the maintainer of Rules from multiple sources
type Map struct {
	// mutex to protect following queryRulesMap
	mu sync.Mutex
	// queryRulesMap maps the names of different query rule sources to the actual Rules structure
	queryRulesMap map[string]*Rules
}

// NewMap returns an empty Map object.
func NewMap() *Map {
	qri := &Map{
		queryRulesMap: map[string]*Rules{},
	}
	return qri
}

// RegisterSource registers a query rule source name with Map.
func (qri *Map) RegisterSource(ruleSource string) {
	qri.mu.Lock()
	defer qri.mu.Unlock()
	if _, existed := qri.queryRulesMap[ruleSource]; existed {
		log.Errorf("Query rule source " + ruleSource + " has been registered")
		panic("Query rule source " + ruleSource + " has been registered")
	}
	qri.queryRulesMap[ruleSource] = New()
}

// UnRegisterSource removes a registered query rule source name.
func (qri *Map) UnRegisterSource(ruleSource string) {
	qri.mu.Lock()
	defer qri.mu.Unlock()
	delete(qri.queryRulesMap, ruleSource)
}

// SetRules takes an external Rules structure and overwrite one of the
// internal Rules as designated by ruleSource parameter.
func (qri *Map) SetRules(ruleSource string, newRules *Rules) error {
	if newRules == nil {
		newRules = New()
	}
	qri.mu.Lock()
	defer qri.mu.Unlock()
	if _, ok := qri.queryRulesMap[ruleSource]; ok {
		qri.queryRulesMap[ruleSource] = newRules.Copy()
		return nil
	}
	return errors.New("Rule source identifier " + ruleSource + " is not valid")
}

// Get returns the corresponding Rules as designated by ruleSource parameter.
func (qri *Map) Get(ruleSource string) (*Rules, error) {
	qri.mu.Lock()
	defer qri.mu.Unlock()
	if ruleset, ok := qri.queryRulesMap[ruleSource]; ok {
		return ruleset.Copy(), nil
	}
	return New(), errors.New("Rule source identifier " + ruleSource + " is not valid")
}

// FilterByPlan creates a new Rules by prefiltering on all query rules that are contained in internal
// Rules structures, in other words, query rules from all predefined sources will be applied.
func (qri *Map) FilterByPlan(query string, planid planbuilder.PlanType, tableName string) (newqrs *Rules) {
	qri.mu.Lock()
	defer qri.mu.Unlock()
	newqrs = New()
	for _, rules := range qri.queryRulesMap {
		newqrs.Append(rules.FilterByPlan(query, planid, tableName))
	}
	return newqrs
}

// MarshalJSON marshals to JSON.
func (qri *Map) MarshalJSON() ([]byte, error) {
	qri.mu.Lock()
	defer qri.mu.Unlock()
	return json.Marshal(qri.queryRulesMap)
}
