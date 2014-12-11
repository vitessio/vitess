// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package customrule

import (
	"reflect"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

type ZkCustomRule struct {
	mu                    sync.Mutex
	path                  string
	queryService          *tabletserver.SqlQuery
	zconn                 zk.Conn
	watch                 <-chan zookeeper.Event
	currentRuleSet        *tabletserver.QueryRules
	currentRuleSetVersion int64
	finish                chan int
}

const InvalidRulesetVersion int64 = -1

func NewZkCustomRule(zkconn zk.Conn) *ZkCustomRule {
	return &ZkCustomRule{
		zconn:                 zkconn,
		currentRuleSet:        tabletserver.NewQueryRules(),
		currentRuleSetVersion: InvalidRulesetVersion,
		finish:                make(chan int, 1)}
}

func (zkcr *ZkCustomRule) Open(rulePath string, queryService *tabletserver.SqlQuery) (err error) {
	zkcr.path = rulePath
	zkcr.queryService = queryService
	err = zkcr.refreshWatch()
	if err != nil {
		return err
	}
	err = zkcr.refreshData(false)
	if err != nil {
		return err
	}
	go zkcr.poll()
	return nil
}

func (zkcr *ZkCustomRule) refreshWatch() error {
	_, _, watch, err := zkcr.zconn.GetW(zkcr.path)
	if err != nil {
		log.Errorf("Fail to get a valid watch from ZK service: %v", err)
		return err
	}
	zkcr.watch = watch
	return nil
}

func (zkcr *ZkCustomRule) refreshData(nodeRemoval bool) error {
	data, stat, err := zkcr.zconn.Get(zkcr.path)
	zkcr.mu.Lock()
	defer zkcr.mu.Unlock()
	if err == nil {
		qrs := tabletserver.NewQueryRules()
		if !nodeRemoval {
			err = qrs.UnmarshalJSON([]byte(data))
			if err != nil {
				log.Errorf("Error unmarshaling query rules %v, original data '%s'", err, data)
				return nil
			}
		}
		zkcr.currentRuleSetVersion = stat.Mzxid()
		if !reflect.DeepEqual(zkcr.currentRuleSet, qrs) {
			zkcr.currentRuleSet = qrs.Copy()
			zkcr.queryService.SetQueryRules(tabletserver.CustomQueryRules, qrs.Copy())
		}
		return nil
	}
	log.Errorf("Error encountered when trying to get data and watch from Zk: %v", err)
	return err
}

func (zkcr *ZkCustomRule) poll() {
	for {
		select {
		case <-zkcr.finish:
			return
		case event := <-zkcr.watch:
			switch event.Type {
			case zookeeper.EVENT_CREATED, zookeeper.EVENT_CHANGED, zookeeper.EVENT_DELETED:
				zkcr.refreshData(event.Type == zookeeper.EVENT_DELETED)
			case zookeeper.EVENT_CLOSED:
				zkcr.refreshWatch() // need to refresh to get a new watch
				zkcr.refreshData(false)
			}
		}
	}
}

func (zkcr *ZkCustomRule) Close() {
	zkcr.zconn.Close()
	zkcr.finish <- 1
}

func (zkcr *ZkCustomRule) GetRules() (qrs *tabletserver.QueryRules, version int64, err error) {
	zkcr.mu.Lock()
	defer zkcr.mu.Unlock()
	return zkcr.currentRuleSet.Copy(), zkcr.currentRuleSetVersion, nil
}
