/*
Copyright 2017 Google Inc.

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

package gateway

import (
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/vterrors"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// NewShardError returns a new error with the shard info amended.
func NewShardError(in error, target *querypb.Target, tablet *topodatapb.Tablet, inTransaction bool) error {
	if in == nil {
		return nil
	}
	if tablet != nil {
		return vterrors.Errorf(vterrors.Code(in), "target: %s.%s.%s, used tablet: %s, %v", target.Keyspace, target.Shard, topoproto.TabletTypeLString(target.TabletType), topotools.TabletIdent(tablet), in)
	}
	return vterrors.Errorf(vterrors.Code(in), "target: %s.%s.%s, %v", target.Keyspace, target.Shard, topoproto.TabletTypeLString(target.TabletType), in)
}
