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

// Package events defines the structures used for events dispatched from the
// wrangler package.
package events

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"vitess.io/vitess/go/protoutil"
	eventsdatapb "vitess.io/vitess/go/vt/proto/eventsdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
)

// Reparent is an event that describes a single step in the reparent process.
type Reparent struct {
	eventsdatapb.ReparentEvent
	phaseEvents []*eventsdatapb.ReparentPhaseEvent
}

// NewReparent inits a new Reparent object.
func NewReparent(si *topo.ShardInfo, src *eventsdatapb.Source, reparentType eventsdatapb.ReparentType, newPrimary, oldPrimary *topodatapb.Tablet) *Reparent {
	eventData := eventsdatapb.ReparentEvent{
		Id:         uuid.NewString(),
		Timestamp:  protoutil.TimeToProto(time.Now()),
		Source:     src,
		Type:       reparentType,
		NewPrimary: newPrimary,
		OldPrimary: oldPrimary,
	}
	if si != nil {
		eventData.ShardInfo = &topodatapb.ShardInfo{
			Keyspace:  si.Keyspace(),
			ShardName: si.ShardName(),
			Shard:     si.Shard,
		}
	}
	return &Reparent{
		ReparentEvent: *eventData.CloneVT(),
		phaseEvents:   make([]*eventsdatapb.ReparentPhaseEvent, 0),
	}
}

// Update updates the status of the reparent event.
func (r *Reparent) Update(phaseType any) {
	reparentPhase, ok := phaseType.(eventsdatapb.ReparentPhaseType)
	if !ok {
		return
	}

	switch reparentPhase {
	case eventsdatapb.ReparentPhaseType_Failed:
		r.Status = fmt.Sprintf("failed %s: %s", r.Type, r.Error)
	case eventsdatapb.ReparentPhaseType_Finished:
		r.Status = fmt.Sprintf("finished %s", r.Type)
	case eventsdatapb.ReparentPhaseType_ReadAllTablets:
		r.Status = "reading all tablets"
	case eventsdatapb.ReparentPhaseType_ReparentAllTablets:
		r.Status = "reparenting all tablets"
	case eventsdatapb.ReparentPhaseType_PrimaryElection:
		r.Status = "electing a primary candidate"
	case eventsdatapb.ReparentPhaseType_PrimaryElected:
		r.Status = "elected new primary candidate"
	case eventsdatapb.ReparentPhaseType_DemoteOldPrimary:
		r.Status = "demoting old primary"
	case eventsdatapb.ReparentPhaseType_ReadTabletMap:
		r.Status = "reading tablet map"
	}

	r.Phase = reparentPhase
	r.Timestamp = protoutil.TimeToProto(time.Now())
	r.phaseEvents = append(r.phaseEvents, &eventsdatapb.ReparentPhaseEvent{
		Phase:     r.Phase,
		Timestamp: r.Timestamp,
		Status:    r.Status,
		Error:     r.Error,
	})
}

// GetPhaseEvents() returns a slice of eventsdatapb.ReparentPhaseEvent.
func (r *Reparent) GetPhaseEvents() []*eventsdatapb.ReparentPhaseEvent {
	return r.phaseEvents
}
