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

package vstreamer

import (
	"context"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

func TestRowStreamerWaitForSource(t *testing.T) {
	type fields struct {
		vse                   *Engine
		cp                    dbconfigs.Connector
		se                    *schema.Engine
		ReplicationLagSeconds int64
		maxTrxHistLen         int64
		maxReplLagSecs        int64
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "Small mvcc impact limit",
			fields: fields{
				vse:            engine,
				se:             engine.se,
				maxTrxHistLen:  100,
				maxReplLagSecs: 5000,
			},
			wantErr: true,
		},
		{
			name: "Small lag impact limit",
			fields: fields{
				vse:            engine,
				se:             engine.se,
				maxTrxHistLen:  10000,
				maxReplLagSecs: 5,
			},
			wantErr: true,
		},
		{
			name: "Large impact limit",
			fields: fields{
				vse:            engine,
				se:             engine.se,
				maxTrxHistLen:  10000,
				maxReplLagSecs: 200,
			},
			wantErr: false,
		},
	}
	testDB := fakesqldb.New(t)
	hostres := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"hostname|port",
		"varchar|int64"),
		"localhost|3306",
	)
	thlres := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"history_len",
		"int64"),
		"1000",
	)
	sbmres := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Seconds_Behind_Master",
		"int64"),
		"10",
	)
	testDB.AddQuery(hostQuery, hostres)
	testDB.AddQuery(trxHistoryLenQuery, thlres)
	testDB.AddQuery(replicaLagQuery, sbmres)
	for _, tt := range tests {
		tt.fields.cp = testDB.ConnParams()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		t.Run(tt.name, func(t *testing.T) {
			uvs := &uvstreamer{
				ctx:                   ctx,
				cancel:                cancel,
				vse:                   tt.fields.vse,
				cp:                    tt.fields.cp,
				se:                    tt.fields.se,
				ReplicationLagSeconds: tt.fields.ReplicationLagSeconds,
			}
			env.TabletEnv.Config().RowStreamer.MaxTrxHistLen = tt.fields.maxTrxHistLen
			env.TabletEnv.Config().RowStreamer.MaxReplicaLagSeconds = tt.fields.maxReplLagSecs
			if err := uvs.waitForSource(); (err != nil) != tt.wantErr {
				t.Errorf("uvstreamer.waitForSource() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
