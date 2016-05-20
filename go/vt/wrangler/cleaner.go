// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// Cleaner remembers a list of cleanup steps to perform.  Just record
// action cleanup steps, and execute them at the end in reverse
// order, with various guarantees.
type Cleaner struct {
	// mu protects the following members
	mu      sync.Mutex
	actions []cleanerActionReference
}

// cleanerActionReference is the node used by Cleaner
type cleanerActionReference struct {
	name   string
	target string
	action CleanerFunction
}

// CleanerFunction is the interface that clean-up actions need to implement
type CleanerFunction func(context.Context, *Wrangler) error

// Record will add a cleaning action to the list
func (cleaner *Cleaner) Record(name, target string, action CleanerFunction) {
	cleaner.mu.Lock()
	cleaner.actions = append(cleaner.actions, cleanerActionReference{
		name:   name,
		target: target,
		action: action,
	})
	cleaner.mu.Unlock()
}

type cleanUpHelper struct {
	err error
}

// CleanUp will run the recorded actions.
// If an action on a target fails, it will not run the next action on
// the same target.
// We return the aggregate errors for all cleanups.
// CleanUp uses its own context, with a timeout of 5 minutes, so that clean up action will run even if the original context times out.
// TODO(alainjobart) Actions should run concurrently on a per target
// basis. They are then serialized on each target.
func (cleaner *Cleaner) CleanUp(wr *Wrangler) error {
	// we use a background context so we're not dependent on the original context timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	actionMap := make(map[string]*cleanUpHelper)
	rec := concurrency.AllErrorRecorder{}
	cleaner.mu.Lock()
	for i := len(cleaner.actions) - 1; i >= 0; i-- {
		actionReference := cleaner.actions[i]
		helper, ok := actionMap[actionReference.target]
		if !ok {
			helper = &cleanUpHelper{
				err: nil,
			}
			actionMap[actionReference.target] = helper
		}
		if helper.err != nil {
			wr.Logger().Warningf("previous action failed on target %v, not running %v", actionReference.target, actionReference.name)
			continue
		}
		err := actionReference.action(ctx, wr)
		if err != nil {
			helper.err = err
			rec.RecordError(err)
			wr.Logger().Errorf("action %v failed on %v: %v", actionReference.name, actionReference.target, err)
		} else {
			wr.Logger().Infof("action %v successful on %v", actionReference.name, actionReference.target)
		}
	}
	cleaner.mu.Unlock()
	cancel()
	return rec.Error()
}

// RemoveActionByName removes an action from the cleaner list
func (cleaner *Cleaner) RemoveActionByName(name, target string) error {
	cleaner.mu.Lock()
	defer cleaner.mu.Unlock()
	for i, action := range cleaner.actions {
		if action.name == name && action.target == target {
			newActions := make([]cleanerActionReference, 0, len(cleaner.actions)-1)
			if i > 0 {
				newActions = append(newActions, cleaner.actions[0:i]...)
			}
			if i < len(cleaner.actions)-1 {
				newActions = append(newActions, cleaner.actions[i+1:len(cleaner.actions)]...)
			}
			cleaner.actions = newActions
			return nil
		}
	}
	return topo.ErrNoNode
}

//
// ChangeSlaveTypeAction CleanerFunction
//

// ChangeSlaveTypeActionName is the name of the action to change a slave type
// (can be used to find such an action by name)
const ChangeSlaveTypeActionName = "ChangeSlaveTypeAction"

// RecordChangeSlaveTypeAction records a new ChangeSlaveTypeAction
// into the specified Cleaner
func RecordChangeSlaveTypeAction(cleaner *Cleaner, tabletAlias *topodatapb.TabletAlias, from topodatapb.TabletType, to topodatapb.TabletType) {
	cleaner.Record(ChangeSlaveTypeActionName, topoproto.TabletAliasString(tabletAlias), func(ctx context.Context, wr *Wrangler) error {
		ti, err := wr.ts.GetTablet(ctx, tabletAlias)
		if err != nil {
			return err
		}
		if ti.Type != from {
			return fmt.Errorf("tablet %v is not of the right type (got %v expected %v), not changing it to %v", topoproto.TabletAliasString(tabletAlias), ti.Type, from, to)
		}
		if !topo.IsTrivialTypeChange(ti.Type, to) {
			return fmt.Errorf("tablet %v type change %v -> %v is not an allowed transition for ChangeSlaveType", topoproto.TabletAliasString(tabletAlias), ti.Type, to)
		}

		// ask the tablet to make the change
		return wr.tmc.ChangeType(ctx, ti.Tablet, to)
	})
}

//
// TabletTagAction CleanerFunction
//

// TabletTagActionName is the name of the Tag action
const TabletTagActionName = "TabletTagAction"

// RecordTabletTagAction records a new action to set / remove a tag
// into the specified Cleaner
func RecordTabletTagAction(cleaner *Cleaner, tabletAlias *topodatapb.TabletAlias, name, value string) {
	cleaner.Record(TabletTagActionName, topoproto.TabletAliasString(tabletAlias), func(ctx context.Context, wr *Wrangler) error {
		_, err := wr.TopoServer().UpdateTabletFields(ctx, tabletAlias, func(tablet *topodatapb.Tablet) error {
			if tablet.Tags == nil {
				tablet.Tags = make(map[string]string)
			}
			if value != "" {
				tablet.Tags[name] = value
			} else {
				delete(tablet.Tags, name)
			}
			return nil
		})
		return err
	})
}

//
// StartSlaveAction CleanerAction
//

// StartSlaveActionName is the name of the slave start action
const StartSlaveActionName = "StartSlaveAction"

// RecordStartSlaveAction records a new action to restart binlog replication on a server
// into the specified Cleaner
func RecordStartSlaveAction(cleaner *Cleaner, tablet *topodatapb.Tablet) {
	cleaner.Record(StartSlaveActionName, topoproto.TabletAliasString(tablet.Alias), func(ctx context.Context, wr *Wrangler) error {
		return wr.TabletManagerClient().StartSlave(ctx, tablet)
	})
}

//
// StartBlpAction CleanerAction
//

// StartBlpActionName is the name of the action to start binlog player
const StartBlpActionName = "StartBlpAction"

// RecordStartBlpAction records an action to restart binlog replication on a server
// into the specified Cleaner
func RecordStartBlpAction(cleaner *Cleaner, tablet *topodatapb.Tablet) {
	cleaner.Record(StartBlpActionName, topoproto.TabletAliasString(tablet.Alias), func(ctx context.Context, wr *Wrangler) error {
		return wr.TabletManagerClient().StartBlp(ctx, tablet)
	})
}
