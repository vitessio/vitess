/*
Copyright 2025 The Vitess Authors.

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

package balancer

import (
	"net/http"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// ConsistentHashBalancer implements the [TabletBalancer] interface. For a given
// session, it will return the same tablet for its duration. The tablet is initially
// selected randomly, with preference to tablets in the local cell.
type ConsistentHashBalancer struct {
	// localCell is the cell the gateway is currently running in.
	localCell string
}

// Pick is the main entry point to the balancer.
//
// For a given session, it will return the same tablet for its duration. The tablet is
// initially selected randomly, with preference to tablets in the local cell.
func Pick(target *querypb.Target, _ []*discovery.TabletHealth, opts *PickOpts) *discovery.TabletHealth {
	return nil
}

// DebugHandler provides a summary of the consistent hash balancer state.
func DebugHandler(w http.ResponseWriter, r *http.Request) {}
