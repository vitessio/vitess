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

package topoproto

import topodatapb "vitess.io/vitess/go/vt/proto/topodata"

var shardReplicationProblemTypeName map[int32]string

func init() {
	shardReplicationProblemTypeName = make(map[int32]string, len(topodatapb.ShardReplicationProblem_Type_name))
	for k, v := range topodatapb.ShardReplicationProblem_Type_name {
		shardReplicationProblemTypeName[k] = v
	}
}

// ShardReplicationProblemTypeString returns a string representation of the
// ShardReplicationProblem type, or "UNKNOWN" if not known.
func ShardReplicationProblemTypeString(t topodatapb.ShardReplicationProblem_Type) string {
	v, ok := shardReplicationProblemTypeName[int32(t)]
	if !ok {
		return "UNKNOWN"
	}

	return v
}
