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

package tabletserver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testConn struct {
	id     int64
	query  string
	killed bool
}

func (tc *testConn) Current() string { return tc.query }

func (tc *testConn) ID() int64 { return tc.id }

func (tc *testConn) Kill(string, time.Duration) error {
	tc.killed = true
	return nil
}

func (tc *testConn) IsKilled() bool {
	return tc.killed
}

func TestQueryList(t *testing.T) {
	ql := NewQueryList("test")
	connID := int64(1)
	qd := NewQueryDetail(context.Background(), &testConn{id: connID})
	ql.Add(qd)

	if qd1, ok := ql.queryDetails[connID]; !ok || qd1[0].connID != connID {
		t.Errorf("failed to add to QueryList")
	}

	conn2ID := int64(2)
	qd2 := NewQueryDetail(context.Background(), &testConn{id: conn2ID})
	ql.Add(qd2)

	rows := ql.AppendQueryzRows(nil)
	if len(rows) != 2 || rows[0].ConnID != 1 || rows[1].ConnID != 2 {
		t.Errorf("wrong rows returned %v", rows)
	}

	ql.Remove(qd)
	if _, ok := ql.queryDetails[connID]; ok {
		t.Errorf("failed to remove from QueryList")
	}
}

func TestQueryListChangeConnIDInMiddle(t *testing.T) {
	ql := NewQueryList("test")
	connID := int64(1)
	qd1 := NewQueryDetail(context.Background(), &testConn{id: connID})
	ql.Add(qd1)

	conn := &testConn{id: connID}
	qd2 := NewQueryDetail(context.Background(), conn)
	ql.Add(qd2)

	require.Len(t, ql.queryDetails[1], 2)

	// change the connID in the middle
	conn.id = 2

	// remove the same object.
	ql.Remove(qd2)

	require.Len(t, ql.queryDetails[1], 1)
	require.Equal(t, qd1, ql.queryDetails[1][0])
	require.NotEqual(t, qd2, ql.queryDetails[1][0])
}
