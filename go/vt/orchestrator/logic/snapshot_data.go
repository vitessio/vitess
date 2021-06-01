/*
   Copyright 2017 Shlomi Noach, GitHub Inc.

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

package logic

import (
	"vitess.io/vitess/go/vt/orchestrator/inst"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"
)

type SnapshotData struct {
	Keys             []inst.InstanceKey // Kept for backwards comapatibility
	MinimalInstances []inst.MinimalInstance
	RecoveryDisabled bool

	ClusterAlias,
	ClusterAliasOverride,
	ClusterDomainName,
	HostAttributes,
	InstanceTags,
	AccessToken,
	PoolInstances,
	InjectedPseudoGTIDClusters,
	HostnameResolves,
	HostnameUnresolves,
	DowntimedInstances,
	Candidates,
	Detections,
	KVStore,
	Recovery,
	RecoverySteps sqlutils.NamedResultData

	LeaderURI string
}

func NewSnapshotData() *SnapshotData {
	return &SnapshotData{}
}

type SnapshotDataCreatorApplier struct {
}

func NewSnapshotDataCreatorApplier() *SnapshotDataCreatorApplier {
	generator := &SnapshotDataCreatorApplier{}
	return generator
}
