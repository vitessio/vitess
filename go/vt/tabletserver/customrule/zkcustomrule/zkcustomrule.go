// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkcustomrule

import (
	"flag"
	"reflect"
	"sync"
	"time"

	log "github.com/golang/glog"
	zookeeper "github.com/samuel/go-zookeeper/zk"

	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/zk"
)

var (
	// Actual ZkCustomRule object in charge of rule updates
	zkCustomRule = NewZkCustomRule(zk.NewMetaConn())
	// Commandline flag to specify rule path in zookeeper
	zkRulePath = flag.String("zkcustomrules", "", "zookeeper based custom rule path")
)

// InvalidQueryRulesVersion is used to mark invalid query rules
const InvalidQueryRulesVersion int64 = -1

// ZkCustomRuleSource is zookeeper based custom rule source name
const ZkCustomRuleSource string = "ZK_CUSTOM_RULE"

// ZkCustomRule is Zookeeper backed implementation of CustomRuleManager
type ZkCustomRule struct {
	mu                    sync.Mutex
	path                  string
	zconn                 zk.Conn
	watch                 <-chan zookeeper.Event // Zookeeper watch for listenning data change notifications
	currentRuleSet        *tabletserver.QueryRules
	currentRuleSetVersion int64 // implemented with Zookeeper transaction id
	finish                chan int
}

// NewZkCustomRule Creates new ZkCustomRule structure
func NewZkCustomRule(zkconn zk.Conn) *ZkCustomRule {
	return &ZkCustomRule{
		zconn:                 zkconn,
		currentRuleSet:        tabletserver.NewQueryRules(),
		currentRuleSetVersion: InvalidQueryRulesVersion,
		finish:                make(chan int, 1)}
}

// Open Registers Zookeeper watch, gets inital QueryRules and starts polling routine
func (zkcr *ZkCustomRule) Open(qsc tabletserver.Controller, rulePath string) (err error) {
	zkcr.path = rulePath
	err = zkcr.refreshWatch()
	if err != nil {
		return err
	}
	err = zkcr.refreshData(qsc, false)
	if err != nil {
		return err
	}
	go zkcr.poll(qsc)
	return nil
}

// refreshWatch gets a new watch channel for ZkCustomRule, it is called when
// the old watch channel is closed on errors
func (zkcr *ZkCustomRule) refreshWatch() error {
	_, _, watch, err := zkcr.zconn.GetW(zkcr.path)
	if err != nil {
		log.Warningf("Fail to get a valid watch from ZK service: %v", err)
		return err
	}
	zkcr.watch = watch
	return nil
}

// refreshData gets query rules from Zookeeper and refresh internal QueryRules cache
// this function will also call TabletServer.SetQueryRules to propagate rule changes to query service
func (zkcr *ZkCustomRule) refreshData(qsc tabletserver.Controller, nodeRemoval bool) error {
	data, stat, err := zkcr.zconn.Get(zkcr.path)
	zkcr.mu.Lock()
	defer zkcr.mu.Unlock()
	if err == nil {
		qrs := tabletserver.NewQueryRules()
		if !nodeRemoval {
			err = qrs.UnmarshalJSON([]byte(data))
			if err != nil {
				log.Warningf("Error unmarshaling query rules %v, original data '%s'", err, data)
				return nil
			}
		}
		zkcr.currentRuleSetVersion = stat.Mzxid
		if !reflect.DeepEqual(zkcr.currentRuleSet, qrs) {
			zkcr.currentRuleSet = qrs.Copy()
			qsc.SetQueryRules(ZkCustomRuleSource, qrs.Copy())
			log.Infof("Custom rule version %v fetched from Zookeeper and applied to vttablet", zkcr.currentRuleSetVersion)
		}
		return nil
	}
	log.Warningf("Error encountered when trying to get data and watch from Zk: %v", err)
	return err
}

const sleepDuringZkFailure time.Duration = 30

// poll polls the Zookeeper watch channel for data changes and refresh watch channel if watch channel is closed
// by Zookeeper Go library on error conditions such as connection reset
func (zkcr *ZkCustomRule) poll(qsc tabletserver.Controller) {
	for {
		select {
		case <-zkcr.finish:
			return
		case event := <-zkcr.watch:
			switch event.Type {
			case zookeeper.EventNodeCreated, zookeeper.EventNodeDataChanged, zookeeper.EventNodeDeleted:
				err := zkcr.refreshData(qsc, event.Type == zookeeper.EventNodeDeleted) // refresh rules
				if err != nil {
					// Sleep to avoid busy waiting during connection re-establishment
					<-time.After(time.Second * sleepDuringZkFailure)
				}
			case zookeeper.EventSession:
				err := zkcr.refreshWatch() // need to to get a new watch
				if err != nil {
					// Sleep to avoid busy waiting during connection re-establishment
					<-time.After(time.Second * sleepDuringZkFailure)
				}
				zkcr.refreshData(qsc, false)
			}
		}
	}
}

// Close signals an termination to polling go routine and closes Zookeeper connection object
func (zkcr *ZkCustomRule) Close() {
	zkcr.zconn.Close()
	zkcr.finish <- 1
}

// GetRules retrives cached rules
func (zkcr *ZkCustomRule) GetRules() (qrs *tabletserver.QueryRules, version int64, err error) {
	zkcr.mu.Lock()
	defer zkcr.mu.Unlock()
	return zkcr.currentRuleSet.Copy(), zkcr.currentRuleSetVersion, nil
}

// ActivateZkCustomRules activates zookeeper dynamic custom rule mechanism
func ActivateZkCustomRules(qsc tabletserver.Controller) {
	if *zkRulePath != "" {
		qsc.RegisterQueryRuleSource(ZkCustomRuleSource)
		zkCustomRule.Open(qsc, *zkRulePath)
	}
}

func init() {
	tabletserver.RegisterFunctions = append(tabletserver.RegisterFunctions, ActivateZkCustomRules)
	servenv.OnTerm(zkCustomRule.Close)
}
