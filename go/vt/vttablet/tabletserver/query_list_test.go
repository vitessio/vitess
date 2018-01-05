/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tabletserver

import (
	"testing"
	"time"

	"golang.org/x/net/context"
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
	ql := NewQueryList()
	connID := int64(1)
	qd := NewQueryDetail(context.Background(), &testConn{id: connID})
	ql.Add(qd)

	if qd1, ok := ql.queryDetails[connID]; !ok || qd1.connID != connID {
		t.Errorf("failed to add to QueryList")
	}

	conn2ID := int64(2)
	qd2 := NewQueryDetail(context.Background(), &testConn{id: conn2ID})
	ql.Add(qd2)

	rows := ql.GetQueryzRows()
	if len(rows) != 2 || rows[0].ConnID != 1 || rows[1].ConnID != 2 {
		t.Errorf("wrong rows returned %v", rows)
	}

	ql.Remove(qd)
	if _, ok := ql.queryDetails[connID]; ok {
		t.Errorf("failed to remove from QueryList")
	}
}
