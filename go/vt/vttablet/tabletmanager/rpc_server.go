/*
Copyright 2017 Google Inc.

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

package tabletmanager

import (
	"fmt"

	"vitess.io/vitess/go/vt/vterrors"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/vt/callinfo"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// This file contains the RPC method helpers for the tablet manager.

//
// Utility functions for RPC service
//

// lock is used at the beginning of an RPC call, to lock the
// action mutex. It returns ctx.Err() if <-ctx.Done() after the lock.
func (agent *ActionAgent) lock(ctx context.Context) error {
	agent.actionMutex.Lock()
	agent.actionMutexLocked = true

	// After we take the lock (which could take a long time), we
	// check the client is still here.
	select {
	case <-ctx.Done():
		agent.actionMutexLocked = false
		agent.actionMutex.Unlock()
		return ctx.Err()
	default:
		return nil
	}
}

// unlock is the symetrical action to lock.
func (agent *ActionAgent) unlock() {
	agent.actionMutexLocked = false
	agent.actionMutex.Unlock()
}

// checkLock checks we have locked the actionMutex.
func (agent *ActionAgent) checkLock() {
	if !agent.actionMutexLocked {
		panic("programming error: this action should have taken the actionMutex")
	}
}

// HandleRPCPanic is part of the RPCAgent interface.
func (agent *ActionAgent) HandleRPCPanic(ctx context.Context, name string, args, reply interface{}, verbose bool, err *error) {
	// panic handling
	if x := recover(); x != nil {
		log.Errorf("TabletManager.%v(%v) on %v panic: %v\n%s", name, args, topoproto.TabletAliasString(agent.TabletAlias), x, tb.Stack(4))
		*err = fmt.Errorf("caught panic during %v: %v", name, x)
		return
	}

	// quick check for fast path
	if !verbose && *err == nil {
		return
	}

	// we gotta log something, get the source
	from := ""
	ci, ok := callinfo.FromContext(ctx)
	if ok {
		from = ci.Text()
	}

	if *err != nil {
		// error case
		log.Warningf("TabletManager.%v(%v)(on %v from %v) error: %v", name, args, topoproto.TabletAliasString(agent.TabletAlias), from, (*err).Error())
		*err = vterrors.Wrapf(*err, "TabletManager.%v on %v error: %v", name, topoproto.TabletAliasString(agent.TabletAlias), (*err).Error())
	} else {
		// success case
		log.Infof("TabletManager.%v(%v)(on %v from %v): %#v", name, args, topoproto.TabletAliasString(agent.TabletAlias), from, reply)
	}
}

//
// RegisterQueryService is used to delay registration of RPC servers until we have all the objects.
type RegisterQueryService func(*ActionAgent)

// RegisterQueryServices is a list of functions to call when the delayed registration is triggered.
var RegisterQueryServices []RegisterQueryService

// registerQueryService will register all the instances.
func (agent *ActionAgent) registerQueryService() {
	for _, f := range RegisterQueryServices {
		f(agent)
	}
}
