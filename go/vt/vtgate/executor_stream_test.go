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

package vtgate

import (
	"testing"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	_ "github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"github.com/youtube/vitess/go/vt/vttablet/sandboxconn"
	"golang.org/x/net/context"
)

func TestStreamSQLUnsharded(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "stream * from user_msgs"
	result, err := executorStreamMessages(executor, sql)
	if err != nil {
		t.Error(err)
	}
	wantResult := sandboxconn.StreamRowResult
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func executorStreamMessages(executor *Executor, sql string) (qr *sqltypes.Result, err error) {
	results := make(chan *sqltypes.Result, 100)
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Millisecond)
	err = executor.StreamExecute(
		ctx,
		"TestExecuteStream",
		NewSafeSession(masterSession),
		sql,
		nil,
		querypb.Target{
			TabletType: topodatapb.TabletType_MASTER,
		},
		func(qr *sqltypes.Result) error {
			results <- qr
			return nil
		},
	)
	close(results)
	if err != nil {
		return nil, err
	}
	first := true
	for r := range results {
		if first {
			qr = &sqltypes.Result{Fields: r.Fields}
			first = false
		}
		qr.Rows = append(qr.Rows, r.Rows...)
	}
	return qr, nil
}
