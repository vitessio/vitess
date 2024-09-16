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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmptyPrep(t *testing.T) {
	pp := createAndOpenPreparedPool(0)
	err := pp.Put(nil, "aa")
	require.ErrorContains(t, err, "prepared transactions exceeded limit: 0")
}

func TestPrepPut(t *testing.T) {
	pp := createAndOpenPreparedPool(2)
	err := pp.Put(nil, "aa")
	require.NoError(t, err)
	err = pp.Put(nil, "bb")
	require.NoError(t, err)
	err = pp.Put(nil, "cc")
	require.ErrorContains(t, err, "prepared transactions exceeded limit: 2")
	err = pp.Put(nil, "aa")
	require.ErrorContains(t, err, "duplicate DTID in Prepare: aa")

	_, err = pp.FetchForCommit("aa")
	require.NoError(t, err)
	err = pp.Put(nil, "aa")
	require.ErrorContains(t, err, "duplicate DTID in Prepare: aa")
	pp.Forget("aa")
	err = pp.Put(nil, "aa")
	require.NoError(t, err)
}

func TestPrepFetchForRollback(t *testing.T) {
	pp := createAndOpenPreparedPool(2)
	conn := &StatefulConnection{}
	pp.Put(conn, "aa")
	got := pp.FetchForRollback("bb")
	if got != nil {
		t.Errorf("Get(bb): %v, want nil", got)
	}
	got = pp.FetchForRollback("aa")
	if got != conn {
		t.Errorf("pp.Get(aa): %p, want %p", got, conn)
	}
	got = pp.FetchForRollback("aa")
	if got != nil {
		t.Errorf("Get(aa): %v, want nil", got)
	}
}

func TestPrepFetchForCommit(t *testing.T) {
	pp := createAndOpenPreparedPool(2)
	conn := &StatefulConnection{}
	got, err := pp.FetchForCommit("aa")
	require.NoError(t, err)
	assert.Nil(t, got)

	pp.Put(conn, "aa")
	got, err = pp.FetchForCommit("aa")
	require.NoError(t, err)
	assert.Equal(t, conn, got)

	_, err = pp.FetchForCommit("aa")
	assert.ErrorContains(t, err, "locked for committing")

	pp.SetFailed("aa")
	_, err = pp.FetchForCommit("aa")
	assert.ErrorContains(t, err, "failed to commit")

	pp.SetFailed("bb")
	_, err = pp.FetchForCommit("bb")
	assert.ErrorContains(t, err, "failed to commit")

	pp.Forget("aa")
	got, err = pp.FetchForCommit("aa")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestPrepFetchAll(t *testing.T) {
	pp := createAndOpenPreparedPool(2)
	conn1 := &StatefulConnection{}
	conn2 := &StatefulConnection{}
	pp.Put(conn1, "aa")
	pp.Put(conn2, "bb")
	got := pp.FetchAllForRollback()
	require.Len(t, got, 2)
	require.Len(t, pp.conns, 0)
	_, err := pp.FetchForCommit("aa")
	require.ErrorContains(t, err, "pool is shutdown")
}

// createAndOpenPreparedPool creates a new transaction prepared pool and opens it.
// Used as a helper function for testing.
func createAndOpenPreparedPool(capacity int) *TxPreparedPool {
	pp := NewTxPreparedPool(capacity, true)
	pp.Open()
	return pp
}
