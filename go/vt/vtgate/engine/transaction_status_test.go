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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestTransactionStatusOutput tests the output and the fields of the transaction state query engine.
func TestTransactionStatusOutput(t *testing.T) {
	tests := []struct {
		name                    string
		transactionStatusOutput []*querypb.TransactionMetadata
		resultErr               error
		expectedRes             *sqltypes.Result
		primitive               *TransactionStatus
	}{
		{
			name:                    "Empty Transaction Status",
			transactionStatusOutput: nil,
			expectedRes:             sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|state|record_time|participants", "varchar|varchar|datetime|varchar")),
			primitive:               &TransactionStatus{},
		}, {
			name: "Valid Transaction Status for a transaction ID",
			primitive: &TransactionStatus{
				TransactionID: "ks:-80:v24s7843sf78934l3",
			},
			transactionStatusOutput: []*querypb.TransactionMetadata{
				{
					Dtid:        "ks:-80:v24s7843sf78934l3",
					State:       querypb.TransactionState_PREPARE,
					TimeCreated: 1257894000000000000,
					Participants: []*querypb.Target{
						{
							Keyspace:   "ks",
							Shard:      "-80",
							TabletType: topodatapb.TabletType_PRIMARY,
						}, {
							Keyspace:   "ks",
							Shard:      "80-a0",
							TabletType: topodatapb.TabletType_PRIMARY,
						}, {
							Keyspace:   "ks",
							Shard:      "a0-",
							TabletType: topodatapb.TabletType_PRIMARY,
						},
					},
				},
			},
			expectedRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("id|state|record_time|participants", "varchar|varchar|datetime|varchar"),
				"ks:-80:v24s7843sf78934l3|PREPARE|2009-11-10 23:00:00 +0000 UTC|ks:-80,ks:80-a0,ks:a0-"),
		}, {
			name: "Error getting transaction metadata",
			primitive: &TransactionStatus{
				TransactionID: "ks:-80:v24s7843sf78934l3",
			},
			resultErr: fmt.Errorf("failed reading transaction state"),
		}, {
			name: "Valid Transaction Statuses for a keyspace",
			primitive: &TransactionStatus{
				Keyspace: "ks",
			},
			transactionStatusOutput: []*querypb.TransactionMetadata{
				{
					Dtid:        "ks:-80:v24s7843sf78934l3",
					State:       querypb.TransactionState_PREPARE,
					TimeCreated: 1257894000000000000,
					Participants: []*querypb.Target{
						{
							Keyspace:   "ks",
							Shard:      "-80",
							TabletType: topodatapb.TabletType_PRIMARY,
						}, {
							Keyspace:   "ks",
							Shard:      "80-a0",
							TabletType: topodatapb.TabletType_PRIMARY,
						}, {
							Keyspace:   "ks",
							Shard:      "a0-",
							TabletType: topodatapb.TabletType_PRIMARY,
						},
					},
				},
				{
					Dtid:        "ks:-80:v34afdfdsfdfd",
					State:       querypb.TransactionState_PREPARE,
					TimeCreated: 1259894000000000000,
					Participants: []*querypb.Target{
						{
							Keyspace:   "ks",
							Shard:      "-80",
							TabletType: topodatapb.TabletType_PRIMARY,
						}, {
							Keyspace:   "ks",
							Shard:      "80-",
							TabletType: topodatapb.TabletType_PRIMARY,
						},
					},
				},
			},
			expectedRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("id|state|record_time|participants", "varchar|varchar|datetime|varchar"),
				"ks:-80:v24s7843sf78934l3|PREPARE|2009-11-10 23:00:00 +0000 UTC|ks:-80,ks:80-a0,ks:a0-",
				"ks:-80:v34afdfdsfdfd|PREPARE|2009-12-04 02:33:20 +0000 UTC|ks:-80,ks:80-",
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.primitive.TryExecute(context.Background(), &loggingVCursor{
				transactionStatusOutput: test.transactionStatusOutput,
				resultErr:               test.resultErr,
			}, nil, true)
			if test.resultErr != nil {
				require.EqualError(t, err, test.resultErr.Error())
				return
			}
			require.NoError(t, err)
			expectResult(t, res, test.expectedRes)
		})
	}
}
