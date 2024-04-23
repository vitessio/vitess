/*
Copyright 2023 The Vitess Authors.

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

package grpc_api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/callerid"
)

// TestEffectiveCallerIDWithAccess verifies that an authenticated gRPC static user with an effectiveCallerID that has ACL access can execute queries
func TestEffectiveCallerIDWithAccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vtgateConn, err := cluster.DialVTGate(ctx, t.Name(), vtgateGrpcAddress, "some_other_user", "test_password")
	require.NoError(t, err)
	defer vtgateConn.Close()

	session := vtgateConn.Session(keyspaceName+"@primary", nil)
	query := "SELECT id FROM test_table"
	ctx = callerid.NewContext(ctx, callerid.NewEffectiveCallerID("user_with_access", "", ""), nil)
	_, err = session.Execute(ctx, query, nil)
	assert.NoError(t, err)
}

// TestEffectiveCallerIDWithNoAccess verifies that an authenticated gRPC static user without an effectiveCallerID that has ACL access cannot execute queries
func TestEffectiveCallerIDWithNoAccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vtgateConn, err := cluster.DialVTGate(ctx, t.Name(), vtgateGrpcAddress, "another_unrelated_user", "test_password")
	require.NoError(t, err)
	defer vtgateConn.Close()

	session := vtgateConn.Session(keyspaceName+"@primary", nil)
	query := "SELECT id FROM test_table"
	ctx = callerid.NewContext(ctx, callerid.NewEffectiveCallerID("user_no_access", "", ""), nil)
	_, err = session.Execute(ctx, query, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Select command denied to user")
	assert.Contains(t, err.Error(), "for table 'test_table' (ACL check error)")
}

// TestAuthenticatedUserWithAccess verifies that an authenticated gRPC static user with ACL access can execute queries
func TestAuthenticatedUserWithAccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vtgateConn, err := cluster.DialVTGate(ctx, t.Name(), vtgateGrpcAddress, "user_with_access", "test_password")
	require.NoError(t, err)
	defer vtgateConn.Close()

	session := vtgateConn.Session(keyspaceName+"@primary", nil)
	query := "SELECT id FROM test_table"
	_, err = session.Execute(ctx, query, nil)
	assert.NoError(t, err)
}

// TestAuthenticatedUserNoAccess verifies that an authenticated gRPC static user with no ACL access cannot execute queries
func TestAuthenticatedUserNoAccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vtgateConn, err := cluster.DialVTGate(ctx, t.Name(), vtgateGrpcAddress, "user_no_access", "test_password")
	require.NoError(t, err)
	defer vtgateConn.Close()

	session := vtgateConn.Session(keyspaceName+"@primary", nil)
	query := "SELECT id FROM test_table"
	_, err = session.Execute(ctx, query, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Select command denied to user")
	assert.Contains(t, err.Error(), "for table 'test_table' (ACL check error)")
}

// TestUnauthenticatedUser verifies that an unauthenticated gRPC user cannot execute queries
func TestUnauthenticatedUser(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vtgateConn, err := cluster.DialVTGate(ctx, t.Name(), vtgateGrpcAddress, "", "")
	require.NoError(t, err)
	defer vtgateConn.Close()

	session := vtgateConn.Session(keyspaceName+"@primary", nil)
	query := "SELECT id FROM test_table"
	_, err = session.Execute(ctx, query, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid credentials")
}
