/*
Copyright 2026 The Vitess Authors.

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

package throttle

import tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"

// ThrottlerType identifies which throttler is being used
type ThrottlerType string

const (
	// DefaultTabletThrottler is the standard throttler for replication lag
	DefaultTabletThrottler ThrottlerType = "DefaultTabletThrottler"
	// QueryTabletThrottler is the throttler for incoming query throttling
	QueryTabletThrottler ThrottlerType = "QueryTabletThrottler"
)

// ToProto returns the protobuf ThrottlerType corresponding to this ThrottlerType.
func (t ThrottlerType) ToProto() tabletmanagerdatapb.ThrottlerType {
	switch t {
	case QueryTabletThrottler:
		return tabletmanagerdatapb.ThrottlerType_DedicatedQueryThrottler
	default:
		return tabletmanagerdatapb.ThrottlerType_DefaultLagThrottler
	}
}
