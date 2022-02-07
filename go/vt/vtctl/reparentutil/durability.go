/*
Copyright 2022 The Vitess Authors.

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
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

func SemiSyncACKersForPrimary(primary *topodatapb.Tablet, allTablets []*topodatapb.Tablet) (semiSyncACKers []*topodatapb.Tablet) {
	for _, tablet := range allTablets {
		if topoproto.TabletAliasEqual(primary.Alias, tablet.Alias) {
			continue
		}
		if IsReplicaSemiSync(primary, tablet) {
			semiSyncACKers = append(semiSyncACKers, tablet)
		}
	}
	return
}
