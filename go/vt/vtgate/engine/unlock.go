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
	"context"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Primitive = (*Unlock)(nil)

// Unlock primitive will execute unlock tables to all connections in the session.
type Unlock struct {
	noTxNeeded
	noInputs
}

const unlockTables = "unlock tables"

func (u *Unlock) RouteType() string {
	return "UNLOCK"
}

func (u *Unlock) GetKeyspaceName() string {
	return ""
}

func (u *Unlock) GetTableName() string {
	return ""
}

func (u *Unlock) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.VT13001("GetFields should not be called for unlock tables")
}

func (u *Unlock) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	rss := vcursor.Session().ShardSession()

	if len(rss) == 0 {
		return &sqltypes.Result{}, nil
	}
	bqs := make([]*querypb.BoundQuery, len(rss))
	for i := 0; i < len(rss); i++ {
		bqs[i] = &querypb.BoundQuery{Sql: unlockTables}
	}
	qr, errs := vcursor.ExecuteMultiShard(ctx, u, rss, bqs, true, false)
	return qr, vterrors.Aggregate(errs)
}

func (u *Unlock) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	qr, err := u.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(qr)
}

func (u *Unlock) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "UnlockTables",
	}
}
