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

package events

import (
	"time"

	"github.com/google/uuid"

	"vitess.io/vitess/go/protoutil"
	eventsdatapb "vitess.io/vitess/go/vt/proto/eventsdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// KeyspaceChange is an event that describes changes to a keyspace.
type KeyspaceChange struct {
	eventsdatapb.KeyspaceChangeEvent
}

// NewKeyspaceChange inits a KeyspaceChange event.
func NewKeyspaceChange(src *eventsdatapb.Source, keyspaceName string, newKeyspace, oldKeyspace *topodatapb.Keyspace) *KeyspaceChange {
	return &KeyspaceChange{
		eventsdatapb.KeyspaceChangeEvent{
			Meta: &eventsdatapb.Metadata{
				Id:        uuid.NewString(),
				Source:    src,
				Timestamp: protoutil.TimeToProto(time.Now()),
			},
			KeyspaceName: keyspaceName,
			NewKeyspace:  newKeyspace,
			OldKeyspace:  oldKeyspace,
		},
	}
}

// Update updates the status of the event.
func (kc *KeyspaceChange) Update(update any) {
	status, ok := update.(string)
	if !ok {
		return
	}
	kc.Meta.Timestamp = protoutil.TimeToProto(time.Now())
	kc.Status = status
}
