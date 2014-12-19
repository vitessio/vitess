// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package customrule

import (
	"io/ioutil"
	"reflect"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/tabletserver"
)

// FileCustomRule is an implementation of CustomRuleManager based on
// polling on a file which contains custom query rule definitions
type FileCustomRule struct {
	mu                      sync.Mutex
	path                    string                   // Path to the file containing custom query rules
	pollingInterval         time.Duration            // Interval in seconds at which FileCustomRule polls the file
	currentRuleSet          *tabletserver.QueryRules // Caches latest successfully retrived QueryRules from polling
	currentRuleSetTimestamp int64                    // Unix timestamp of last successful polling of QueryRules
	queryService            *tabletserver.SqlQuery   // SqlQuery structure of vttablet
	finish                  chan int                 // Used by Close to signal the polling go routine to exit
}

// The minimum interval in seconds to poll the custom rule file
const MinFilePollingSeconds time.Duration = 1

// The default interval in seconds to poll the custom rule file
const DefaultFilePollingSeconds time.Duration = 10

// NewFileCustomRule returns pointer to new FileCustomRule structure
func NewFileCustomRule(pollingInterval time.Duration) (fcr *FileCustomRule) {
	fcr = new(FileCustomRule)
	fcr.path = ""
	fcr.pollingInterval = pollingInterval
	if pollingInterval < MinFilePollingSeconds {
		log.Warningf("Cannot poll a query rule file at an interval of less than %v secs, falling back to default interval(%v s)",
			MinFilePollingSeconds, DefaultFilePollingSeconds)
		fcr.pollingInterval = DefaultFilePollingSeconds
	}
	fcr.currentRuleSet = tabletserver.NewQueryRules()
	fcr.finish = make(chan int, 1)
	return fcr
}

// loadCustomRules loads rules from file and push rules to SqlQuery if rules are built correctly
func (fcr *FileCustomRule) loadCustomRules() (qrs *tabletserver.QueryRules, err error) {
	if fcr.path == "" {
		// Don't go further if path is empty
		return tabletserver.NewQueryRules(), nil
	}
	data, err := ioutil.ReadFile(fcr.path)
	if err != nil {
		log.Warningf("Error reading file %v: %v", fcr.path, err)
		// Don't update any internal cache, just return error
		// The effect is that if a file is removed, the internal custom rules won't be cleared,
		// we choose this behavior because administrators may update rule file
		// by removing the old one and copying a new one, if we clear the query rules upon detecting
		// the removal, then vttablet may be exposed to banned queries for a short period
		return tabletserver.NewQueryRules(), err
	}

	fcr.mu.Lock()
	defer fcr.mu.Unlock()
	qrs = tabletserver.NewQueryRules()
	err = qrs.UnmarshalJSON(data)
	if err != nil {
		log.Warningf("Error unmarshaling query rules %v", err)
		// Don't update internal cache either
		return tabletserver.NewQueryRules(), err
	}
	fcr.currentRuleSetTimestamp = time.Now().Unix()
	if !reflect.DeepEqual(fcr.currentRuleSet, qrs) {
		fcr.currentRuleSet = qrs.Copy()
		// Update only when it is necessary so that we avoid clear query plan cache
		// too often
		fcr.queryService.SetQueryRules(tabletserver.CustomQueryRules, qrs.Copy())
	}
	return qrs, nil
}

// Poll does polling on the query rule file and update internal caches accordingly
func (fcr *FileCustomRule) Poll() {
	for {
		select {
		case <-fcr.finish:
			return // Close() is invoked, don't need to poll any more
		case <-time.After(time.Second * fcr.pollingInterval):
			fcr.loadCustomRules()
		}
	}
}

// Open sets up go routine for polling rule file and gets the initial version of custom query rules
func (fcr *FileCustomRule) Open(rulePath string, queryService *tabletserver.SqlQuery) error {
	fcr.path = rulePath
	fcr.queryService = queryService
	fcr.loadCustomRules()
	if rulePath != "" {
		// polling routine will run when a meaningful path is available
		go fcr.Poll()
	}
	return nil
}

// Close tears down the polling routine
func (fcr *FileCustomRule) Close() {
	fcr.finish <- 1
}

// GetRules returns most recent cached query rules
func (fcr *FileCustomRule) GetRules() (qrs *tabletserver.QueryRules, version int64, err error) {
	fcr.mu.Lock()
	fcr.mu.Unlock()
	return fcr.currentRuleSet.Copy(), fcr.currentRuleSetTimestamp, nil
}
