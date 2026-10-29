/*
Copyright 2026 The Vitess Authors.

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

package txresolver

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/log/logtest"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type fakeTxConn struct {
	err error
}

func (f *fakeTxConn) ResolveTransactions(context.Context, *querypb.Target) error {
	return f.err
}

func TestResolveTransactionsLogs(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	target := &querypb.Target{Keyspace: "ks", Shard: "-80"}

	NewTxResolver(nil, &fakeTxConn{err: vterrors.New(vtrpcpb.Code_INTERNAL, "resolve failed")}).resolveTransactions(t.Context(), target)
	record := logs.Record(t, "failed to resolve transactions for shard")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "ks", record["keyspace"])
	assert.Equal(t, "-80", record["shard"])
	assert.Equal(t, "resolve failed", record["error"])
	assert.Equal(t, "INTERNAL", record["error_code"])

	NewTxResolver(nil, &fakeTxConn{}).resolveTransactions(t.Context(), target)
	record = logs.Record(t, "successfully resolved all the transactions for shard")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, "ks", record["keyspace"])
	assert.Equal(t, "-80", record["shard"])
}
