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

package vtgate

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

func TestFailToMultiShardWhenSetToSingleDb(t *testing.T) {
	session := NewSafeSession(&vtgatepb.Session{
		InTransaction: true, TransactionMode: vtgatepb.TransactionMode_SINGLE,
	})

	sess0 := &vtgatepb.Session_ShardSession{
		Target:        &querypb.Target{Keyspace: "keyspace", Shard: "0"},
		TabletAlias:   &topodatapb.TabletAlias{Cell: "cell", Uid: 0},
		TransactionId: 1,
	}
	sess1 := &vtgatepb.Session_ShardSession{
		Target:        &querypb.Target{Keyspace: "keyspace", Shard: "1"},
		TabletAlias:   &topodatapb.TabletAlias{Cell: "cell", Uid: 1},
		TransactionId: 1,
	}

	err := session.AppendOrUpdate(sess0, vtgatepb.TransactionMode_SINGLE)
	require.NoError(t, err)
	err = session.AppendOrUpdate(sess1, vtgatepb.TransactionMode_SINGLE)
	require.Error(t, err)
}

func TestPrequeries(t *testing.T) {
	session := NewSafeSession(&vtgatepb.Session{
		SystemVariables: map[string]string{
			"s1": "'apa'",
			"s2": "42",
		},
	})

	q1 := "set @@s1 = 'apa'"
	q2 := "set @@s2 = 42"
	want := []string{q1, q2}
	wantReversed := []string{q2, q1}
	preQueries := session.SetPreQueries()

	if !reflect.DeepEqual(want, preQueries) && !reflect.DeepEqual(wantReversed, preQueries) {
		t.Errorf("got %v but wanted %v", preQueries, want)
	}
}
