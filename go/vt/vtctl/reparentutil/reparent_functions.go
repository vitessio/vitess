/*
Copyright 2021 The Vitess Authors.

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

package reparentutil

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/vt/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

type (
	// EmergencyReparentOptions provides optional parameters to
	// EmergencyReparentShard operations. Options are passed by value, so it is safe
	// for callers to mutate and reuse options structs for multiple calls.
	EmergencyReparentOptions struct {
		newPrimaryAlias           *topodatapb.TabletAlias
		ignoreReplicas            sets.String
		waitReplicasTimeout       time.Duration
		preventCrossCellPromotion bool

		lockAction string
	}
)

// NewEmergencyReparentOptions creates a new EmergencyReparentOptions which is used in ERS ans PRS
func NewEmergencyReparentOptions(newPrimaryAlias *topodatapb.TabletAlias, ignoreReplicas sets.String, waitReplicasTimeout time.Duration, preventCrossCellPromotion bool) EmergencyReparentOptions {
	return EmergencyReparentOptions{
		newPrimaryAlias:           newPrimaryAlias,
		ignoreReplicas:            ignoreReplicas,
		waitReplicasTimeout:       waitReplicasTimeout,
		preventCrossCellPromotion: preventCrossCellPromotion,
	}
}

// PostERSCompletionHook implements the ReparentFunctions interface
func (opts *EmergencyReparentOptions) PostERSCompletionHook(ctx context.Context, ev *events.Reparent, logger logutil.Logger, tmc tmclient.TabletManagerClient) {
}
