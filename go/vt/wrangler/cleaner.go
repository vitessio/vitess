// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/topo"
)

// Cleaner remembers a list of cleanup steps to perform.  Just record
// action cleanup steps, and execute them at the end in reverse
// order, with various guarantees.
type Cleaner struct {
	// folowing members protected by lock
	mu      sync.Mutex
	actions []cleanerActionReference
}

// cleanerActionReference is the node used by Cleaner
type cleanerActionReference struct {
	name   string
	target string
	action CleanerAction
}

// CleanerAction is the interface that clean-up actions need to implement
type CleanerAction interface {
	CleanUp(wr *Wrangler) error
}

// Record will add a cleaning action to the list
func (cleaner *Cleaner) Record(name, target string, action CleanerAction) {
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
// TODO(alainjobart) Actions should run concurrently on a per target
// basis. They are then serialized on each target.
func (cleaner *Cleaner) CleanUp(wr *Wrangler) error {
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
			log.Warningf("previous action failed on target %v, no running %v", actionReference.target, actionReference.name)
			continue
		}
		err := actionReference.action.CleanUp(wr)
		if err != nil {
			helper.err = err
			rec.RecordError(err)
			log.Errorf("action %v failed on %v: %v", actionReference.name, actionReference.target, err)
		} else {
			log.Infof("action %v successfull on %v", actionReference.name, actionReference.target)
		}
	}
	cleaner.mu.Unlock()
	return rec.Error()
}

// GetActionByName returns the first action in the list with the given
// name and target
func (cleaner *Cleaner) GetActionByName(name, target string) (CleanerAction, error) {
	cleaner.mu.Lock()
	defer cleaner.mu.Unlock()
	for _, action := range cleaner.actions {
		if action.name == name && action.target == target {
			return action.action, nil
		}
	}
	return nil, topo.ErrNoNode
}

// ChangeSlaveTypeAction will change a server type to another type
type ChangeSlaveTypeAction struct {
	TabletAlias topo.TabletAlias
	TabletType  topo.TabletType
}

const ChangeSlaveTypeActionName = "ChangeSlaveTypeAction"

// RecordChangeSlaveTypeAction records a new ChangeSlaveTypeAction
// into the specified Cleaner
func RecordChangeSlaveTypeAction(cleaner *Cleaner, tabletAlias topo.TabletAlias, tabletType topo.TabletType) {
	cleaner.Record(ChangeSlaveTypeActionName, tabletAlias.String(), &ChangeSlaveTypeAction{
		TabletAlias: tabletAlias,
		TabletType:  tabletType,
	})
}

// FindChangeSlaveTypeActionByTarget finds the first action for the target
func FindChangeSlaveTypeActionByTarget(cleaner *Cleaner, tabletAlias topo.TabletAlias) (*ChangeSlaveTypeAction, error) {
	action, err := cleaner.GetActionByName(ChangeSlaveTypeActionName, tabletAlias.String())
	if err != nil {
		return nil, err
	}
	result, ok := action.(*ChangeSlaveTypeAction)
	if !ok {
		return nil, fmt.Errorf("Action with wrong type: %v", action)
	}
	return result, nil
}

// CleanUp is part of CleanerAction interface.
func (csta ChangeSlaveTypeAction) CleanUp(wr *Wrangler) error {
	return wr.ChangeType(csta.TabletAlias, csta.TabletType, false)
}
