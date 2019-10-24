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

package tabletmanager

import (
	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// VReplicationExec executes a vreplication command.
func (agent *ActionAgent) VReplicationExec(ctx context.Context, query string) (*querypb.QueryResult, error) {
	qr, err := agent.VREngine.Exec(query)
	if err != nil {
		return nil, err
	}
	return sqltypes.ResultToProto3(qr), nil
}

// VReplicationWaitForPos waits for the specified position.
func (agent *ActionAgent) VReplicationWaitForPos(ctx context.Context, id int, pos string) error {
	return agent.VREngine.WaitForPos(ctx, id, pos)
}
