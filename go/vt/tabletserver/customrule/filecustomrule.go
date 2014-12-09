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
	mu              sync.Mutex
	path            string                   // path to the file containing custom query rules
	pollingInterval time.Duration            // interval in seconds at which FileCustomRule polls the file
	currentRuleSet  *tabletserver.QueryRules // cache most recent successfully polled QueryRules
	queryService    *tabletserver.SqlQuery   // SqlQuery structure of vttablet
	finish          chan int                 // Used by Close routine to signal the polling go routine to exit
}

// The minimum interval to poll the custom rule file
const MinFilePollingSeconds time.Duration = 30

// The default interval to poll the custom rule file
const DefaultFilePollingSeconds time.Duration = 120

// NewFileCustomRule returns empty FileCustomRule structure
func NewFileCustomRule(pollingInterval time.Duration) (fcr *FileCustomRule) {
	fcr = new(FileCustomRule)
	if pollingInterval < MinFilePollingSeconds {
		log.Warningf("Cannot poll a query rule file at an interval of less than %v secs, falling back to default interval(%v s)",
			MinFilePollingSeconds, DefaultFilePollingSeconds)
		pollingInterval = DefaultFilePollingSeconds
	}
	fcr.currentRuleSet = tabletserver.NewQueryRules()
	fcr.pollingInterval = pollingInterval * time.Second
	fcr.finish = make(chan int, 1)
	return fcr
}

func (fcr *FileCustomRule) loadCustomRules() (err error, qrs *tabletserver.QueryRules) {
	if fcr.path == "" {
		return nil, tabletserver.NewQueryRules()
	}
	data, err := ioutil.ReadFile(fcr.path)
	if err != nil {
		log.Errorf("Error reading file %v: %v", fcr.path, err)
		// Don't update any internal cache, just return
		return err, tabletserver.NewQueryRules()
	}

	fcr.mu.Lock()
	defer fcr.mu.Unlock()
	qrs = tabletserver.NewQueryRules()
	err = qrs.UnmarshalJSON(data)
	if err != nil {
		log.Errorf("Error unmarshaling query rules %v", err)
		// Don't update internal cache either
		return err, tabletserver.NewQueryRules()
	}
	if !reflect.DeepEqual(fcr.currentRuleSet, qrs) {
		fcr.currentRuleSet = qrs.Copy()
		// Update only when it is necessary so that we avoid clear query plan cache
		// too often
		fcr.queryService.SetQueryRules(tabletserver.CustomQueryRules, qrs.Copy())
	}
	return nil, qrs
}

// Poll do polling on the query rule file and update internal caches accordingly
func (fcr *FileCustomRule) Poll() {
	for {
		select {
		case <-fcr.finish:
			return // Close() is invoked, don't need to poll any more
		case <-time.After(fcr.pollingInterval):
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
		go fcr.Poll()
	}
	return nil
}

// Close tears down the polling routine
func (fcr *FileCustomRule) Close() {
	fcr.finish <- 1
}

// GetRules returns most recent cached query rules
func (fcr *FileCustomRule) GetRules() (err error, qrs *tabletserver.QueryRules, version int64) {
	err, qrs = fcr.loadCustomRules()
	if err != nil {
		return err, tabletserver.NewQueryRules(), InvalidQueryRulesVersion
	}
	return nil, qrs, time.Now().Unix()
}
