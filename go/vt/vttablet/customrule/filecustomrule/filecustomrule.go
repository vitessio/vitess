/*
Copyright 2019 The Vitess Authors.

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

// Package filecustomrule implements static custom rule from a config file
package filecustomrule

import (
	"flag"
	"io/ioutil"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

var (
	// Actual FileCustomRule object in charge of rule updates
	fileCustomRule = NewFileCustomRule()
	// Commandline flag to specify rule path
	fileRulePath = flag.String("filecustomrules", "", "file based custom rule path")
)

// FileCustomRule is an implementation of CustomRuleManager, it reads custom query
// rules from local file for once and push it to vttablet
type FileCustomRule struct {
	path                    string       // Path to the file containing custom query rules
	currentRuleSet          *rules.Rules // Query rules built from local file
	currentRuleSetTimestamp int64        // Unix timestamp when currentRuleSet is built from local file
}

// FileCustomRuleSource is the name of the file based custom rule source
const FileCustomRuleSource string = "FILE_CUSTOM_RULE"

// NewFileCustomRule returns pointer to new FileCustomRule structure
func NewFileCustomRule() (fcr *FileCustomRule) {
	fcr = new(FileCustomRule)
	fcr.path = ""
	fcr.currentRuleSet = rules.New()
	return fcr
}

// Open try to build query rules from local file and push the rules to vttablet
func (fcr *FileCustomRule) Open(qsc tabletserver.Controller, rulePath string) error {
	fcr.path = rulePath
	if fcr.path == "" {
		// Don't go further if path is empty
		return nil
	}
	data, err := ioutil.ReadFile(fcr.path)
	if err != nil {
		log.Warningf("Error reading file %v: %v", fcr.path, err)
		// Don't update any internal cache, just return error
		return err
	}
	qrs := rules.New()
	err = qrs.UnmarshalJSON(data)
	if err != nil {
		log.Warningf("Error unmarshaling query rules %v", err)
		return err
	}
	fcr.currentRuleSetTimestamp = time.Now().Unix()
	fcr.currentRuleSet = qrs.Copy()
	// Push query rules to vttablet
	qsc.SetQueryRules(FileCustomRuleSource, qrs.Copy())
	log.Infof("Custom rule loaded from file: %s", fcr.path)
	return nil
}

// GetRules returns query rules built from local file
func (fcr *FileCustomRule) GetRules() (qrs *rules.Rules, version int64, err error) {
	return fcr.currentRuleSet.Copy(), fcr.currentRuleSetTimestamp, nil
}

// ActivateFileCustomRules activates this static file based custom rule mechanism
func ActivateFileCustomRules(qsc tabletserver.Controller) {
	if *fileRulePath != "" {
		qsc.RegisterQueryRuleSource(FileCustomRuleSource)
		fileCustomRule.Open(qsc, *fileRulePath)
	}
}

func init() {
	tabletserver.RegisterFunctions = append(tabletserver.RegisterFunctions, ActivateFileCustomRules)
}
