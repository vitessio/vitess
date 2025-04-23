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
	"time"

	"vitess.io/vitess/go/protoutil"
	eventsdatapb "vitess.io/vitess/go/vt/proto/eventsdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
)

// Reparent is an event that describes a single step in the reparent process.
type Reparent struct {
	eventsdatapb.ReparentEvent
}

// NewReparent inits a new Reparent object.
func NewReparent(si *topo.ShardInfo, newPrimary, oldPrimary *topodatapb.Tablet) *Reparent {
	eventData := eventsdatapb.ReparentEvent{
		Timestamp: protoutil.TimeToProto(time.Now()),
		ShardInfo: &topodatapb.ShardInfo{
			Keyspace:  si.Keyspace(),
			ShardName: si.ShardName(),
			Shard:     si.Shard,
		},
		NewPrimary: newPrimary,
		OldPrimary: oldPrimary,
	}
	return &Reparent{eventData}
}

// Update updates the status of the reparent event.
func (r *Reparent) Update(status any) {
	r.Status = status.(string)
}
