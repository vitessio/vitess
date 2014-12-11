// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package customrule

import (
	"errors"
	"flag"
	"fmt"

	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/zk"
)

// CustomRuleManager outlines the methods needed to manage
// custom query rules. CustomRuleManager gets custom query
// rules from certain souces (file, zookeeper, etc) and push
// the rules to vttablet through SqlQuery.SetQueryRules
type CustomRuleManager interface {
	Open(rulePath string, queryService *tabletserver.SqlQuery) error    // Open sets up everything ready for future actions
	Close()                                                             // Close tears down everything
	GetRules() (qrs *tabletserver.QueryRules, version int64, err error) // GetRules returns the current set of rules available to CustomRuleManager
}

const InvalidQueryRulesVersion int64 = -1

// CustomRuleImplements maps name of implementation to the actual structure pointers
// All registered CustomRuleManager implementations are stored here
var CustomRuleImplements map[string]CustomRuleManager = make(map[string]CustomRuleManager)
var CustomRuleManagerInUse = flag.String("customrule_manager", FileCustomRuleImpl, "the customrule manager to use")

// RegisterCustomRuleImpl registers a CustomRuleManager implementation by setting up
// an entry in CustomRuleImplements.
func RegisterCustomRuleImpl(name string, manager CustomRuleManager) error {
	if _, ok := CustomRuleImplements[name]; ok {
		return errors.New(fmt.Sprintf("Custom Rule implementation %s has already been registered", name))
	}
	CustomRuleImplements[name] = manager
	return nil
}

// Names of different custom rule implementations
const FileCustomRuleImpl string = "file"    // file based custom rule implementation
const ZkCustomRuleImpl string = "zookeeper" // Zookeeper based custom rule implementation

func init() {
	RegisterCustomRuleImpl(FileCustomRuleImpl, NewFileCustomRule(DefaultFilePollingSeconds))
	RegisterCustomRuleImpl(ZkCustomRuleImpl, NewZkCustomRule(zk.NewMetaConn(false)))
}

func InitializeCustomRuleManager(customRulePath string, queryService *tabletserver.SqlQuery) error {
	if manager, ok := CustomRuleImplements[*CustomRuleManagerInUse]; ok {
		return manager.Open(customRulePath, queryService)
	}
	return errors.New(fmt.Sprintf("Custom rule implementation %s is unsupported", *CustomRuleManagerInUse))
}

func TearDownCustomRuleManager() {
	if manager, ok := CustomRuleImplements[*CustomRuleManagerInUse]; ok {
		manager.Close()
	}
}
