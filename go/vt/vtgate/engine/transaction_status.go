/*
Copyright 2024 The Vitess Authors.

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
	"fmt"
	"strings"
	"time"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*TransactionStatus)(nil)

// TransactionStatus is a primitive to call into executor via vcursor.
type TransactionStatus struct {
	noInputs
	noTxNeeded

	TransactionID string
}

func (t *TransactionStatus) RouteType() string {
	return "TransactionStatus"
}

func (t *TransactionStatus) GetKeyspaceName() string {
	return ""
}

func (t *TransactionStatus) GetTableName() string {
	return ""
}

func (t *TransactionStatus) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{
		Fields: t.getFields(),
	}, nil
}

func (t *TransactionStatus) getFields() []*querypb.Field {
	return []*querypb.Field{
		{
			Name: "id",
			Type: sqltypes.VarChar,
		},
		{
			Name: "state",
			Type: sqltypes.VarChar,
		},
		{
			Name: "record_time",
			Type: sqltypes.Datetime,
		},
		{
			Name: "participants",
			Type: sqltypes.VarChar,
		},
	}
}

func (t *TransactionStatus) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	transactionState, err := vcursor.ReadTransaction(ctx, t.TransactionID)
	if err != nil {
		return nil, err
	}
	res := &sqltypes.Result{}
	if wantfields {
		res.Fields = t.getFields()
	}
	if transactionState != nil && transactionState.Dtid != "" {
		var participantString []string
		for _, participant := range transactionState.Participants {
			participantString = append(participantString, fmt.Sprintf("%s:%s", participant.Keyspace, participant.Shard))
		}
		res.Rows = append(res.Rows, sqltypes.Row{
			sqltypes.NewVarChar(transactionState.Dtid),
			sqltypes.NewVarChar(transactionState.State.String()),
			sqltypes.NewDatetime(time.Unix(0, transactionState.TimeCreated).UTC().String()),
			sqltypes.NewVarChar(strings.Join(participantString, ",")),
		})
	}
	return res, nil
}

func (t *TransactionStatus) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, err := t.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(res)
}

func (t *TransactionStatus) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "TransactionStatus",
		Other: map[string]any{
			"TransactionID": t.TransactionID,
		},
	}
}
