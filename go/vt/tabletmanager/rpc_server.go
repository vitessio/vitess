// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"fmt"
	"time"

	log "github.com/golang/glog"
)

// This file contains the RPC method helpers for the tablet manager.

// rpcTimeout is used for timing out the queries on the server in a
// reasonable amount of time. The actions are stored in the
// topo.Server, and if the client goes away, it cleans up the action
// node, and the server doesn't do the action. In the RPC case, if the
// client goes away (while waiting on the action mutex), the server
// won't know, and may still execute the RPC call at a later time.
// To prevent that, if it takes more than rpcTimeout to take the action mutex,
// we return an error to the caller.
const rpcTimeout = time.Second * 30

//
// Utility functions for RPC service
//

// rpcWrapper handles all the logic for rpc calls.
func (agent *ActionAgent) rpcWrapper(from, name string, args, reply interface{}, verbose bool, f func() error, lock, runAfterAction, reloadSchema bool) (err error) {
	defer func() {
		if x := recover(); x != nil {
			log.Errorf("TabletManager.%v(%v) panic: %v", name, args, x)
			err = fmt.Errorf("caught panic during %v: %v", name, x)
		}
	}()

	if lock {
		beforeLock := time.Now()
		agent.actionMutex.Lock()
		defer agent.actionMutex.Unlock()
		if time.Now().Sub(beforeLock) > rpcTimeout {
			return fmt.Errorf("server timeout for " + name)
		}
	}

	if err = f(); err != nil {
		log.Warningf("TabletManager.%v(%v)(from %v) error: %v", name, args, from, err.Error())
		return fmt.Errorf("TabletManager.%v on %v error: %v", name, agent.TabletAlias, err)
	}
	if verbose {
		log.Infof("TabletManager.%v(%v)(from %v): %v", name, args, from, reply)
	}
	if runAfterAction {
		agent.afterAction("RPC("+name+")", reloadSchema)
	}
	return
}

// There are multiple kinds of actions:
// 1 - read-only actions that can be executed in parallel.
// 2 - read-write actions that change something, and need to take the
//     action lock.
// 3 - read-write actions that need to take the action lock, and also
//     need to reload the tablet state.
// 4 - read-write actions that need to take the action lock, need to
//     reload the tablet state, and reload the schema afterwards.

func (agent *ActionAgent) RpcWrap(from, name string, args, reply interface{}, f func() error) error {
	return agent.rpcWrapper(from, name, args, reply, false /*verbose*/, f,
		false /*lock*/, false /*runAfterAction*/, false /*reloadSchema*/)
}

func (agent *ActionAgent) RpcWrapLock(from, name string, args, reply interface{}, f func() error) error {
	return agent.rpcWrapper(from, name, args, reply, true /*verbose*/, f,
		true /*lock*/, false /*runAfterAction*/, false /*reloadSchema*/)
}

func (agent *ActionAgent) RpcWrapLockAction(from, name string, args, reply interface{}, f func() error) error {
	return agent.rpcWrapper(from, name, args, reply, true /*verbose*/, f,
		true /*lock*/, true /*runAfterAction*/, false /*reloadSchema*/)
}

func (agent *ActionAgent) RpcWrapLockActionSchema(from, name string, args, reply interface{}, f func() error) error {
	return agent.rpcWrapper(from, name, args, reply, true /*verbose*/, f,
		true /*lock*/, true /*runAfterAction*/, true /*reloadSchema*/)
}

//
// Glue to delay registration of RPC servers until we have all the objects
//

type RegisterQueryService func(*ActionAgent)

var RegisterQueryServices []RegisterQueryService

// registerQueryService will register all the instances
func (agent *ActionAgent) registerQueryService() {
	for _, f := range RegisterQueryServices {
		f(agent)
	}
}
