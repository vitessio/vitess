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
	"github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/rules"
	"github.com/youtube/vitess/go/vt/topo/zk2topo"
)

var (
	// Commandline flag to specify rule server and path.
	zkRuleServer = flag.String("zkcustomrules_address", "", "zookeeper server to get rules from")
	zkRulePath   = flag.String("zkcustomrules_path", "", "path in the zookeeper server to get rules from")
)

// invalidQueryRulesVersion is used to mark invalid query rules
const invalidQueryRulesVersion int64 = -1

// ZkCustomRuleSource is zookeeper based custom rule source name
const zkCustomRuleSource string = "ZK_CUSTOM_RULE"

// ZkCustomRule is Zookeeper backed implementation of CustomRuleManager
type ZkCustomRule struct {
	// Zookeeper connection. Set at construction time.
	zconn zk2topo.Conn

	// Path of the rules files. Set at construction time.
	path string

	// mu protects all the following fields.
	mu                    sync.Mutex
	watch                 <-chan zk.Event // Zookeeper watch for listenning data change notifications
	currentRuleSet        *rules.Rules
	currentRuleSetVersion int64 // implemented with Zookeeper modification version
	done                  chan struct{}
}

// NewZkCustomRule Creates new ZkCustomRule structure
func NewZkCustomRule(server, path string) *ZkCustomRule {
	return &ZkCustomRule{
		zconn:                 zk2topo.Connect(server),
		path:                  path,
		currentRuleSet:        rules.New(),
		currentRuleSetVersion: invalidQueryRulesVersion,
		done: make(chan struct{}),
	}
}

// Start registers Zookeeper watch, gets inital Rules and starts
// polling routine.
func (zkcr *ZkCustomRule) Start(qsc tabletserver.Controller) (err error) {
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
	ctx := context.Background()
	_, _, watch, err := zkcr.zconn.GetW(ctx, zkcr.path)
	if err != nil {
		log.Warningf("Fail to get a valid watch from ZK service: %v", err)
		return err
	}
	zkcr.watch = watch
	return nil
}

// refreshData gets query rules from Zookeeper and refresh internal Rules cache
// this function will also call rules.SetQueryRules to propagate rule changes to query service
func (zkcr *ZkCustomRule) refreshData(qsc tabletserver.Controller, nodeRemoval bool) error {
	ctx := context.Background()
	data, stat, err := zkcr.zconn.Get(ctx, zkcr.path)
	if err != nil {
		log.Warningf("Error encountered when trying to get data and watch from Zk: %v", err)
		return err
	}

	qrs := rules.New()
	if !nodeRemoval {
		if err = qrs.UnmarshalJSON([]byte(data)); err != nil {
			log.Warningf("Error unmarshaling query rules %v, original data '%s'", err, data)
			return nil
		}
	}

	zkcr.mu.Lock()
	defer zkcr.mu.Unlock()

	zkcr.currentRuleSetVersion = stat.Mzxid
	if !reflect.DeepEqual(zkcr.currentRuleSet, qrs) {
		zkcr.currentRuleSet = qrs.Copy()
		qsc.SetQueryRules(zkCustomRuleSource, qrs.Copy())
		log.Infof("Custom rule version %v fetched from Zookeeper and applied to vttablet", zkcr.currentRuleSetVersion)
	}

	return nil
}

const sleepDuringZkFailure time.Duration = 30 * time.Second

// poll polls the Zookeeper watch channel for data changes and refresh watch channel if watch channel is closed
// by Zookeeper Go library on error conditions such as connection reset
func (zkcr *ZkCustomRule) poll(qsc tabletserver.Controller) {
	for {
		select {
		case <-zkcr.done:
			return
		case event := <-zkcr.watch:
			switch event.Type {
			case zk.EventNodeCreated, zk.EventNodeDataChanged, zk.EventNodeDeleted:
				err := zkcr.refreshData(qsc, event.Type == zk.EventNodeDeleted) // refresh rules
				if err != nil {
					// Sleep to avoid busy waiting during connection re-establishment
					<-time.After(sleepDuringZkFailure)
				}
			case zk.EventSession:
				err := zkcr.refreshWatch() // need to to get a new watch
				if err != nil {
					// Sleep to avoid busy waiting during connection re-establishment
					<-time.After(sleepDuringZkFailure)
				}
				zkcr.refreshData(qsc, false)
			}
		}
	}
}

// Stop signals a termination to polling go routine and closes
// Zookeeper connection object.
func (zkcr *ZkCustomRule) Stop() {
	close(zkcr.done)
	zkcr.zconn.Close()
}

// GetRules retrives cached rules.
func (zkcr *ZkCustomRule) GetRules() (qrs *rules.Rules, version int64, err error) {
	zkcr.mu.Lock()
	defer zkcr.mu.Unlock()
	return zkcr.currentRuleSet.Copy(), zkcr.currentRuleSetVersion, nil
}

// activateZkCustomRules activates zookeeper dynamic custom rule mechanism.
func activateZkCustomRules(qsc tabletserver.Controller) {
	if *zkRuleServer != "" && *zkRulePath != "" {
		qsc.RegisterQueryRuleSource(zkCustomRuleSource)

		zkCustomRule := NewZkCustomRule(*zkRuleServer, *zkRulePath)
		zkCustomRule.Start(qsc)

		servenv.OnTerm(zkCustomRule.Stop)
	}
}

func init() {
	tabletserver.RegisterFunctions = append(tabletserver.RegisterFunctions, activateZkCustomRules)
}
