/*
Copyright 2020 The Vitess Authors.

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

package engine

import (
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/srvtopo"
)

var _ StreamExecutor = (*shardRoute)(nil)

// shardRoute is an internal primitive used by Route
// for performing merge sorts.
type shardRoute struct {
	query string
	rs    *srvtopo.ResolvedShard
	bv    map[string]*querypb.BindVariable
}

// StreamExecute performs a streaming exec.
func (sr *shardRoute) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return vcursor.StreamExecuteMulti(sr.query, []*srvtopo.ResolvedShard{sr.rs}, []map[string]*querypb.BindVariable{sr.bv}, callback)
}
